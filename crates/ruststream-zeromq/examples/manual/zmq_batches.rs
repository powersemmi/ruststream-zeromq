//! The service of `examples/zmq_batches.rs` without the macros: the handler is a named type and
//! `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_batches --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_zeromq::queue::prelude::*;

/// Stores the jobs of one batch in a single round-trip; `Ok` settles the whole batch.
struct Drain;

impl Handle<[Job]> for Drain {
    fn handle(
        &self,
        jobs: &[Job],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = jobs.len(), "stored a batch of jobs");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            // At most 32 jobs per call.
            b.include(subscriber("jobs", Drain).batch(nonzero!(32)).build());
        },
    )
}
// --8<-- [end:handler]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

// `#[outgoing(name = "jobs")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Job {
    type Form = FixedName;
    const DESTINATION: &'static str = "jobs";
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
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
