//! The service of `examples/zmq_broker_queue.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_queue --features testing
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
        tracing::info!(job.id, "working on the job");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    // The ventilator only pushes, so the retry copies go to a queue this service binds.
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let first = retries.bind(Publish);
    let second = retries.bind(Publish);
    RustStream::new(AppInfo::new("workers", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                // Two workers dial the ventilator, and each job it pushes reaches one of them.
                b.include(subscriber("jobs", Work).build())
                    .out_retry(first)
                    .to("jobs");
                b.include(subscriber("jobs", Work).build())
                    .out_retry(second)
                    .to("jobs");
            },
        )
        .with_broker(retries, |b| {
            b.include(subscriber("jobs", Work).build());
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

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn each_pushed_job_reaches_one_worker() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [1, 2] {
            tb.broker::<ZmqQueue<Connect>>()
                .message(&Job { id })
                .publish()
                .await?;
        }

        let workers = tb.broker::<ZmqQueue<Connect>>();
        assert_eq!(
            workers.subscriber("jobs").received::<Job>(),
            [Job { id: 1 }, Job { id: 2 }],
        );
        workers
            .subscriber("jobs")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
