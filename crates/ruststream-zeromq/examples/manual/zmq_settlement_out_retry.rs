//! The service of `examples/zmq_settlement_out_retry.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_settlement_out_retry --features testing
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

// --8<-- [start:app]
fn app() -> impl App {
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let copies = retries.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                // The ventilator takes nothing back, so the copies go to the queue bound below.
                b.include(subscriber("jobs", Work).build())
                    .max_attempts(nonzero!(3u32))
                    .out_retry(copies)
                    .to("jobs");
            },
        )
        .with_broker(retries, |b| {
            // A bound queue takes its own copies back, so this mount names no destination.
            b.include(subscriber("jobs", Work).build())
                .max_attempts(nonzero!(3u32));
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
    use std::time::Duration;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test(start_paused = true)]
    async fn the_copies_reach_the_queue_the_worker_binds() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue<Connect>>()
            .message(&Job { id: 0 })
            .publish()
            .await?;
        for _ in 0..3 {
            tb.advance(Duration::from_secs(30)).await?;
        }

        // The first delivery came from the ventilator, the two copies from the bound queue.
        tb.broker::<ZmqQueue<Connect>>()
            .subscriber("jobs")
            .assert_called_once();
        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called(2)
            .with(&Job { id: 0 });
        tb.broker::<ZmqQueue>()
            .published::<Job>("jobs")
            .with_header(RETRY_COUNT_HEADER, "2");
        tb.shutdown().await?;
        Ok(())
    }
}
