//! The announcer of the Broadcast guide, written without the macros: the handler is a type with an
//! `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_guide_broadcast --features testing -- run
//! cargo test --example manual_guide_broadcast --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:announcer]
// One file, two patterns: the crate prelude names each policy after its pattern.
use ruststream_zeromq::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
    items: u32,
}

/// The announcement, under a name every `jobs.` watcher matches.
#[derive(Debug, PartialEq, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct JobDone {
    id: u64,
}

// `#[outgoing(name = "jobs.done")]` by hand.
impl OutgoingDestination for JobDone {
    type Form = FixedName;
    const DESTINATION: &'static str = "jobs.done";
}

impl MessageHeaders for JobDone {
    type Contract = NoHeaders;
}

struct Work;

impl Handle<Job, JobDone> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<JobDone, HandlerOutcome>> {
        tracing::info!(job.id, job.items, "job worked");
        ready(Ok(JobDone { id: job.id }))
    }
}

fn app() -> impl App {
    // The watchers dial this endpoint; the announcer is the stable side, so it binds.
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
        .with_broker(events, |_b| {})
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:announcer]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::app;

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

    /// The announcement as a watcher reads it.
    #[derive(Debug, PartialEq, Deserialize)]
    struct Announced {
        id: u64,
    }

    #[tokio::test]
    async fn each_worked_job_is_announced() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqQueue>()
            .message(&Pushed { id: 7, items: 3 })
            .publish()
            .await?;

        tb.broker::<ZmqFanout>()
            .published::<Announced>("jobs.done")
            .assert_called_once()
            .with(&Announced { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
