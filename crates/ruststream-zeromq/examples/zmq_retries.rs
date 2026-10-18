//! A worker that asks for a retry, on a transport that settles nothing.
//!
//! `ZeroMQ` has no delivery counter and no dead-letter topology, so the copies are published by
//! this service and counted by the framework. The mount site declares how many attempts a job
//! gets, and the copies come back through the queue the worker binds.
//!
//! ```text
//! cargo test --example zmq_retries --features testing
//! ```

use std::io;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

/// Stands in for the work: whatever the job actually does, it can fail and be worth another try.
fn run(job: &Job) -> io::Result<()> {
    if job.id == 0 {
        return Err(io::Error::other("a job carries no id"));
    }
    tracing::info!(job.id, "job done");
    Ok(())
}

// --8<-- [start:retries]
#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    match run(job) {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(job.id, %err, "the job failed, retrying");
            HandlerOutcome::retry_after(Duration::from_secs(30))
        }
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            // Five deliveries at most, the first one included; the copies return to this queue.
            b.include(handle)
                .max_attempts(nonzero!(5u32))
                .out_retry(Publish);
        },
    )
}
// --8<-- [end:retries]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test(start_paused = true)]
    async fn a_failing_job_gets_five_deliveries() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 0 })
            .publish()
            .await?;
        for _ in 0..5 {
            tb.advance(Duration::from_secs(30)).await?;
        }

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called(5)
            .settled(HandlerOutcome::retry_after(Duration::from_secs(30)));
        tb.broker::<ZmqQueue>()
            .published::<Job>("jobs")
            .with_header(RETRY_COUNT_HEADER, "4");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_job_that_succeeds_is_handled_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;
        tb.advance(Duration::from_secs(30)).await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
