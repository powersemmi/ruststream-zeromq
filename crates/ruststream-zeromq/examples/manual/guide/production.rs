//! The service of the Production guide, written without the macros: the handler is a type with an
//! `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! JOBS_BIND=tcp://0.0.0.0:5555 SINK_ADDR=tcp://sink:5558 \
//!     cargo run --example manual_guide_production --features testing -- run
//! cargo test --example manual_guide_production --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:settings]
use std::env;
use std::num::NonZeroUsize;

use ruststream_zeromq::queue::prelude::*;

/// What the deployment decides: where the worker listens, where the sink is, and how far a
/// subscription reads ahead of its handler.
struct Settings {
    jobs: String,
    sink: String,
    read_ahead: NonZeroUsize,
}

impl Settings {
    fn from_env() -> Self {
        Self::from_vars(|name| env::var(name).ok())
    }

    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Self {
        Self {
            jobs: var("JOBS_BIND").unwrap_or_else(|| "tcp://0.0.0.0:5555".to_owned()),
            sink: var("SINK_ADDR").unwrap_or_else(|| "tcp://sink:5558".to_owned()),
            read_ahead: var("ZMQ_READ_AHEAD")
                .and_then(|value| value.parse().ok())
                .unwrap_or(nonzero!(1000_usize)),
        }
    }
}
// --8<-- [end:settings]

// --8<-- [start:service]
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
    items: u32,
}

#[derive(Debug, PartialEq, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
    items: u32,
}

// `#[outgoing(name = "results")]` by hand.
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

fn service(settings: Settings) -> RustStream {
    let sink = ZmqQueue::new(ZmqEndpoint::connect(settings.sink)).bindable();
    let results = sink.bind(Publish);
    let jobs = ZmqQueue::new(ZmqEndpoint::bind(settings.jobs)).read_ahead(settings.read_ahead);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(jobs, |b| {
            b.include(subscriber("jobs", Work).reply().build())
                .out_reply(results);
        })
        .with_broker(sink, |_b| {})
}

fn app() -> RustStream {
    service(Settings::from_env())
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Settings, service};

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

    #[test]
    fn an_unset_environment_takes_the_defaults() {
        let settings = Settings::from_vars(|_| None);

        assert_eq!(settings.jobs, "tcp://0.0.0.0:5555");
        assert_eq!(settings.sink, "tcp://sink:5558");
        assert_eq!(settings.read_ahead.get(), 1000);
    }

    #[tokio::test]
    async fn the_configured_service_reaches_its_sink() -> Result<(), Box<dyn Error>> {
        let settings = Settings::from_vars(|name| match name {
            "SINK_ADDR" => Some("tcp://sink.internal:6000".to_owned()),
            "ZMQ_READ_AHEAD" => Some("64".to_owned()),
            _ => None,
        });
        let tb = TestApp::start(service(settings)).await?;

        tb.broker::<ZmqQueue>()
            .message(&Pushed { id: 7, items: 3 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue<Connect>>()
            .published::<Collected>("results")
            .assert_called_once()
            .with(&Collected { id: 7, items: 3 });
        tb.shutdown().await?;
        Ok(())
    }
}
