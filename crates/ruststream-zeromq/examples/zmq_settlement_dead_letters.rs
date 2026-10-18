//! A dead letter is a publish like any other, so it needs an endpoint where the name leaves the
//! worker's own queue: here a second queue this service binds, read by a subscription of its own.
//!
//! ```text
//! cargo test --example zmq_settlement_dead_letters --features testing
//! ```

use std::io;

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

/// Stands in for the work: a job without an id can never succeed.
fn run(job: &Job) -> io::Result<()> {
    if job.id == 0 {
        return Err(io::Error::other("a job carries no id"));
    }
    tracing::info!(job.id, "job done");
    Ok(())
}

#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    match run(job) {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(job.id, %err, "the job failed");
            HandlerOutcome::retry()
        }
    }
}

/// Keeps the jobs nobody could work, for a person to look at.
#[subscriber("jobs.dead")]
async fn park(job: &Job) -> HandlerOutcome {
    tracing::error!(job.id, "job parked");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let dead = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5561")).bindable();
    let parked = dead.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled(
            "worker",
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                // Every failed job leaves for the second queue, under the name it is read by.
                b.include(work).dead_letter("jobs.dead").out_retry(parked);
            },
        )
        .with_broker_labeled("dead", dead, |b| {
            b.include(park);
        })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn a_failed_job_is_parked_on_the_second_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("worker")
            .message(&Job { id: 0 })
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker_named("worker")
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker_named("dead")
            .subscriber("jobs.dead")
            .assert_called_once()
            .with(&Job { id: 0 });
        tb.broker_named("dead")
            .published::<Job>("jobs.dead")
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }
}
