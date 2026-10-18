//! A PUSH/PULL worker that settles a whole batch of jobs at a time.
//!
//! Nothing about the wire changes: the producer still pushes one message at a time, in the same
//! frame layout, and the batches are assembled on this side.
//!
//! ```text
//! cargo test --example zmq_batches --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_zeromq::queue::prelude::*;

/// Stores the jobs of one batch in a single round-trip.
#[subscriber("jobs")]
async fn drain(jobs: &[Job]) -> HandlerOutcome {
    tracing::info!(count = jobs.len(), "stored a batch of jobs");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            // At most 32 jobs per call.
            b.include(drain.batch(nonzero!(32)));
        },
    )
}
// --8<-- [end:handler]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use tokio::time::Instant;

    use super::{Job, app};

    #[tokio::test]
    async fn jobs_waiting_together_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let jobs: Vec<Job> = (1..=3).map(|id| Job { id }).collect();
        // Pushed at once, the three jobs wait for the handler together.
        let queue = tb.broker::<ZmqQueue>();
        for pushed in join_all(jobs.iter().map(|job| queue.message(job).publish())).await {
            pushed?;
        }

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_lone_job_is_handed_over_20_ms_after_it_arrived() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let pushed = Instant::now();
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_batch_sizes(&[1])
            .with(&Job { id: 7 });
        assert_eq!(pushed.elapsed(), Duration::from_millis(20));
        tb.shutdown().await?;
        Ok(())
    }
}
