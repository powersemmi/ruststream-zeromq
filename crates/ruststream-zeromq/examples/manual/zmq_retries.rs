//! The service of `examples/zmq_retries.rs` without the macros: the handler is a named type and
//! `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_retries --features testing
//! ```

use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

/// Stands in for the work: a job without an id fails, and is worth another try.
fn run(job: &Job) -> io::Result<()> {
    if job.id == 0 {
        return Err(io::Error::other("a job carries no id"));
    }
    tracing::info!(job.id, "job done");
    Ok(())
}

// --8<-- [start:retries]
/// Works a job, and asks for another try in 30 seconds when it fails.
struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(run(job).map_err(|err| {
            tracing::warn!(job.id, %err, "the job failed, retrying");
            HandlerOutcome::retry_after(Duration::from_secs(30))
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            // Five deliveries at most, the first one included; the copies return to this queue.
            b.include(subscriber("jobs", Work).build())
                .max_attempts(nonzero!(5u32))
                .out_retry(Publish);
        },
    )
}
// --8<-- [end:retries]

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
