//! The first service of the Getting started guide, written without the macros: the handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! cargo run --example manual_guide_getting_started --features testing -- run
//! cargo test --example manual_guide_getting_started --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:service]
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
    items: u32,
}

struct TakeJob;

impl Handle<Job> for TakeJob {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if job.items == 0 {
            tracing::warn!(job.id, "job without items dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(job.id, job.items, "job taken");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:5555")),
        |b| {
            b.include(subscriber("jobs", TakeJob).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use serde::Serialize;

    use super::{Job, app};

    /// A job as the peer that pushes it frames it.
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

    #[tokio::test]
    async fn a_job_without_items_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqQueue>()
            .message(&Pushed { id: 7, items: 0 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7, items: 0 })
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_job_with_items_is_taken() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqQueue>()
            .message(&Pushed { id: 8, items: 3 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 8, items: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
