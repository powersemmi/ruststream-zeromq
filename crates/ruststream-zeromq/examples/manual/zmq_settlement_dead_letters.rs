//! The service of `examples/zmq_settlement_dead_letters.rs` without the macros: the handlers are
//! named types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_settlement_dead_letters --features testing
//! ```

use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
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

struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(run(job).map_err(|err| {
            tracing::warn!(job.id, %err, "the job failed");
            HandlerOutcome::retry()
        }))
    }
}

/// Keeps the jobs nobody could work, for a person to look at.
struct Park;

impl Handle<Job> for Park {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(job.id, "job parked");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    let dead = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5561")).bindable();
    let parked = dead.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled(
            "worker",
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                // Every failed job leaves for the second queue, under the name it is read by.
                b.include(subscriber("jobs", Work).build())
                    .dead_letter("jobs.dead")
                    .out_retry(parked);
            },
        )
        .with_broker_labeled("dead", dead, |b| {
            b.include(subscriber("jobs.dead", Park).build());
        })
}
// --8<-- [end:app]

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
