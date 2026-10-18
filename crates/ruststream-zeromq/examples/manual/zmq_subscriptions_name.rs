//! The service of `examples/zmq_subscriptions_name.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_subscriptions_name --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:name]
use ruststream_zeromq::queue::prelude::*;

struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "working on the job");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(subscriber("jobs", Work).build());
        },
    )
}
// --8<-- [end:name]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

// `#[derive(Outgoing)]` with no name by hand: the call site names the destination.
impl OutgoingDestination for Job {
    type Form = CallerName;
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn the_bound_queue_hands_over_every_name() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (name, id) in [("jobs", 1), ("reports", 2)] {
            tb.broker::<ZmqQueue>()
                .message(&Job { id })
                .to(name)
                .publish()
                .await?;
        }

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        assert_eq!(
            tb.broker::<ZmqQueue>().subscriber("jobs").received::<Job>(),
            [Job { id: 1 }, Job { id: 2 }],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
