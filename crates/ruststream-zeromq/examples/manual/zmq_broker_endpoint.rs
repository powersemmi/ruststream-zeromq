//! The service of `examples/zmq_broker_endpoint.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_endpoint --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

// --8<-- [start:app]
fn app() -> impl App {
    // This process dials the collector, which listens on another host.
    let collector = ZmqQueue::new(ZmqEndpoint::connect("tcp://collector:5560")).bindable();
    let results = collector.bind(Publish);
    // This process listens, on a socket file the producers of this host dial.
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("ipc:///tmp/jobs")), |b| {
            b.include(subscriber("jobs", Work).reply().build())
                .out_reply(results);
        })
        .register_broker(collector)
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
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Done, Job, Work, app};

    #[tokio::test]
    async fn a_job_from_the_socket_file_reaches_the_collector() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn another_scheme_is_refused_naming_the_address() {
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("udp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("jobs", Work).reply().build());
            },
        );

        let Err(refused) = TestApp::start(app).await else {
            panic!("ZeroMQ runs on tcp:// and ipc:// only");
        };
        let refused = refused.to_string();
        assert!(
            refused.contains("udp://0.0.0.0:5555"),
            "the refusal names the address, got: {refused}",
        );
    }
}
