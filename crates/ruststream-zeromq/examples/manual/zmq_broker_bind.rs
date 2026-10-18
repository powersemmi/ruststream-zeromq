//! The service of `examples/zmq_broker_bind.rs` without the macros: the handlers are named types
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_bind --features testing
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

struct FileReport;

impl Handle<Report> for FileReport {
    fn handle(
        &self,
        report: &Report,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(report.id, "report filed");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
/// The service on the two addresses it listens on, one per subscription.
fn service(jobs: &str, reports: &str) -> RustStream {
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled("jobs", ZmqQueue::new(ZmqEndpoint::bind(jobs)), |b| {
            b.include(subscriber("jobs", Work).build());
        })
        .with_broker_labeled("reports", ZmqQueue::new(ZmqEndpoint::bind(reports)), |b| {
            b.include(subscriber("reports", FileReport).build());
        })
}

fn app() -> impl App<State = ()> {
    service("tcp://0.0.0.0:5555", "tcp://0.0.0.0:5556")
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
struct Report {
    id: u64,
}

// `#[outgoing(name = "reports")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Report {
    type Form = FixedName;
    const DESTINATION: &'static str = "reports";
}

impl MessageHeaders for Report {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;

    use super::{FileReport, Job, Report, Work, app, service};

    /// The body both modes run: each kind of work reaches the subscription of its endpoint.
    async fn each_endpoint_feeds_its_own_subscription(
        tb: TestApp<()>,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker_named("jobs")
            .message(&Job { id: 7 })
            .publish()
            .await?;
        tb.broker_named("reports")
            .message(&Report { id: 8 })
            .publish()
            .await?;

        tb.broker_named("jobs")
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7 });
        tb.broker_named("reports")
            .subscriber("reports")
            .assert_called_once()
            .with(&Report { id: 8 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        each_endpoint_feeds_its_own_subscription(TestApp::start(app()).await?).await
    }

    /// Port zero lets the system pick each port; the harness's publisher dials what it picked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn over_loopback_on_ports_the_system_picks() -> Result<(), Box<dyn Error>> {
        let app = service("tcp://127.0.0.1:0", "tcp://127.0.0.1:0");
        each_endpoint_feeds_its_own_subscription(TestApp::start_live(app).await?).await
    }

    #[tokio::test]
    async fn a_second_subscription_on_one_bound_endpoint_is_refused() {
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("jobs", Work).build());
                b.include(subscriber("reports", FileReport).build());
            },
        );

        let Err(refused) = TestApp::start(app).await else {
            panic!("a bound endpoint is read by one subscription");
        };
        let refused = refused.to_string();
        assert!(
            refused.contains("'jobs'") && refused.contains("'reports'"),
            "the refusal names both subscriptions, got: {refused}",
        );
    }
}
