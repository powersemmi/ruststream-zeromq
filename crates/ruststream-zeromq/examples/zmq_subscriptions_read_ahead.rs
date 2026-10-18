//! `read_ahead` bounds how many deliveries a subscription reads ahead of its handler. Past the
//! bound the subscription stops reading, and the peer that sends waits.
//!
//! ```text
//! cargo test --example zmq_subscriptions_read_ahead --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[subscriber("frames")]
async fn render(frame: &Frame) -> HandlerOutcome {
    tracing::info!(frame.id, "frame rendered");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // Each frame takes seconds to render, so a short read-ahead holds the rest at the sender.
    let frames =
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")).read_ahead(nonzero!(4_usize));
    RustStream::new(AppInfo::new("renderer", "0.1.0")).with_broker(frames, |b| {
        b.include(render);
    })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "frames")]
struct Frame {
    id: u64,
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
