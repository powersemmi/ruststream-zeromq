//! The service of `examples/zmq_broker_connect.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_connect --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_zeromq::queue::prelude::*;

struct Work;

impl Handle<Job, Done> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Done, HandlerOutcome>> {
        tracing::info!(job.id, "working on the job");
        ready(Ok(Done { id: job.id }))
    }
}

fn app() -> impl App {
    // The sink listens; this worker dials it and only publishes there.
    let sink = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let results = sink.bind(Publish);
    let failed = sink.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled(
            "ventilator",
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                // The ventilator only pushes: results and retry copies leave for the sink.
                b.include(subscriber("jobs", Work).reply().build())
                    .out_reply(results)
                    .out_retry(failed)
                    .to("jobs.failed");
            },
        )
        .with_broker_labeled("sink", sink, |_b| {})
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
}

// `#[outgoing(name = "results")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Done {
    type Form = FixedName;
    const DESTINATION: &'static str = "results";
}

impl MessageHeaders for Done {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::{Done, Job, app};

    #[tokio::test]
    async fn a_job_the_ventilator_pushes_ends_at_the_sink() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("ventilator")
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker_named("ventilator")
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7 });
        tb.broker_named("sink")
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
