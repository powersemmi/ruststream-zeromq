//! The service of `examples/zmq_testing_live.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_testing_live --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if job.id == 0 {
            tracing::warn!("a job without an id, dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(job.id, "job done");
        ready(Ok(()))
    }
}

/// The worker on the address `jobs` names.
fn service(jobs: &str) -> RustStream {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind(jobs)),
        |b| {
            b.include(subscriber("jobs", Work).build());
        },
    )
}

fn app() -> impl App<State = ()> {
    service("tcp://0.0.0.0:5555")
}

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

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app, service};

    // --8<-- [start:both]
    /// The body both modes run; only the start call differs.
    async fn works_a_job_and_drops_an_empty_one(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        for id in [7, 0] {
            tb.broker::<ZmqQueue>()
                .message(&Job { id })
                .publish()
                .await?;
        }

        let queue = tb.broker::<ZmqQueue>();
        let jobs = queue.subscriber("jobs");
        assert_eq!(jobs.received::<Job>(), [Job { id: 7 }, Job { id: 0 }]);
        assert_eq!(jobs.outcomes(), [Outcome::Ack, Outcome::Drop]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        works_a_job_and_drops_an_empty_one(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn over_loopback_sockets() -> Result<(), Box<dyn Error>> {
        // Port zero: the system picks a free port, so parallel tests never collide.
        let tb = TestApp::start_live(service("tcp://127.0.0.1:0")).await?;
        works_a_job_and_drops_an_empty_one(tb).await
    }
    // --8<-- [end:both]
}
