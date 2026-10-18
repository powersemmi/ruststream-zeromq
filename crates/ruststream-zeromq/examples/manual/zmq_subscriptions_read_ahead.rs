//! The service of `examples/zmq_subscriptions_read_ahead.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_subscriptions_read_ahead --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

struct Render;

impl Handle<Frame> for Render {
    fn handle(
        &self,
        frame: &Frame,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(frame.id, "frame rendered");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    // Each frame takes seconds to render, so a short read-ahead holds the rest at the sender.
    let frames =
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")).read_ahead(nonzero!(4_usize));
    RustStream::new(AppInfo::new("renderer", "0.1.0")).with_broker(frames, |b| {
        b.include(subscriber("frames", Render).build());
    })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Frame {
    id: u64,
}

// `#[outgoing(name = "frames")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Frame {
    type Form = FixedName;
    const DESTINATION: &'static str = "frames";
}

impl MessageHeaders for Frame {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Frame, app};

    #[tokio::test]
    async fn the_renderer_takes_its_frames_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in 1..=3 {
            tb.broker::<ZmqQueue>()
                .message(&Frame { id })
                .publish()
                .await?;
        }

        tb.broker::<ZmqQueue>()
            .subscriber("frames")
            .assert_called(3)
            .settled(HandlerOutcome::ack());
        assert_eq!(
            tb.broker::<ZmqQueue>()
                .subscriber("frames")
                .received::<Frame>(),
            [Frame { id: 1 }, Frame { id: 2 }, Frame { id: 3 }],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
