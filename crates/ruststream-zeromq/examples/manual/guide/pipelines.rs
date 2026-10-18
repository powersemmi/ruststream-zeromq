//! The worker of the Pipelines guide, written without the macros: the handler is a type with an
//! `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_guide_pipelines --features testing -- run
//! cargo test --example manual_guide_pipelines --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:work]
use ruststream_zeromq::queue::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
    items: u32,
}

/// What the sink collects: one result per job.
#[derive(Debug, PartialEq, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
    items: u32,
}

// `#[outgoing(name = "results")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Done {
    type Form = FixedName;
    const DESTINATION: &'static str = "results";
}

impl MessageHeaders for Done {
    type Contract = NoHeaders;
}

struct Work;

impl Handle<Job, Done> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Done, HandlerOutcome>> {
        tracing::info!(job.id, job.items, "job worked");
        ready(Ok(Done {
            id: job.id,
            items: job.items,
        }))
    }
}
// --8<-- [end:work]

// --8<-- [start:app]
fn app() -> impl App {
    // The sink listens; every worker dials it and only pushes there.
    let sink = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5558")).bindable();
    // The retry copies come back through a queue this worker binds for itself.
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5559")).bindable();
    let (results, retried_results) = (sink.bind(Publish), sink.bind(Publish));
    let copies = retries.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled(
            "ventilator",
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5557")),
            |b| {
                b.include(subscriber("jobs", Work).reply().build())
                    .out_reply(results)
                    .out_retry(copies)
                    .to("jobs");
            },
        )
        .with_broker_labeled("retries", retries, |b| {
            b.include(subscriber("jobs", Work).reply().build())
                .out_reply(retried_results);
        })
        .with_broker_labeled("sink", sink, |_b| {})
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Job, app};

    /// A job as the ventilator pushes it.
    #[derive(Serialize)]
    struct Pushed {
        id: u64,
        items: u32,
    }

    impl OutgoingDestination for Pushed {
        type Form = FixedName;
        const DESTINATION: &'static str = "jobs";
    }

    impl MessageHeaders for Pushed {
        type Contract = NoHeaders;
    }

    /// A result as the sink reads it.
    #[derive(Debug, PartialEq, Deserialize)]
    struct Collected {
        id: u64,
        items: u32,
    }

    #[tokio::test]
    async fn a_job_from_the_ventilator_reaches_the_sink_as_a_result() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;

        tb.broker_named("ventilator")
            .message(&Pushed { id: 7, items: 3 })
            .publish()
            .await?;

        tb.broker_named("ventilator")
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7, items: 3 });
        tb.broker_named("sink")
            .published::<Collected>("results")
            .assert_called_once()
            .with(&Collected { id: 7, items: 3 });
        tb.shutdown().await?;
        Ok(())
    }
}
