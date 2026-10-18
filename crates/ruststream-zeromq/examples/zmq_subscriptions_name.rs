//! A subscription is a name: `#[subscriber("jobs")]` is the whole declaration. A queue this
//! service binds hands its subscription every message a peer pushes, whatever frame 0 says.
//!
//! ```text
//! cargo test --example zmq_subscriptions_name --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:name]
use ruststream_zeromq::queue::prelude::*;

#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "working on the job");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(work);
        },
    )
}
// --8<-- [end:name]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
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
