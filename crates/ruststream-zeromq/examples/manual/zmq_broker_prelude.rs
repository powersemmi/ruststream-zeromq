//! The service of `examples/zmq_broker_prelude.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_zeromq::prelude::*;

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
    // Every listener that dials the fan-out hears about each finished job.
    let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("jobs", Work).reply().build())
                    .out_reply(announce);
            },
        )
        .register_broker(events)
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

// `#[outgoing(name = "jobs.done")]` by hand: the type names the channel it is published to.
impl OutgoingDestination for Done {
    type Form = FixedName;
    const DESTINATION: &'static str = "jobs.done";
}

impl MessageHeaders for Done {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::prelude::*;

    use super::{Done, Job, app};

    #[tokio::test]
    async fn a_finished_job_is_announced_on_the_fan_out() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqFanout>()
            .published::<Done>("jobs.done")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
