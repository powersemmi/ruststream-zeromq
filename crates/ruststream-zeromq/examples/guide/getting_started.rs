//! The first service of the Getting started guide: a worker that binds a PULL socket on
//! `tcp://127.0.0.1:5555` and takes the jobs a peer pushes to it.
//!
//! ```text
//! cargo run --example guide_getting_started --features testing -- run
//! cargo test --example guide_getting_started --features testing
//! ```

// --8<-- [start:service]
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
struct Job {
    id: u64,
    items: u32,
}

#[subscriber("jobs")]
async fn take_job(job: &Job) -> HandlerOutcome {
    if job.items == 0 {
        tracing::warn!(job.id, "job without items dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(job.id, job.items, "job taken");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:5555")),
        |b| {
            b.include(take_job);
        },
    )
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
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "jobs")]
    struct Pushed {
        id: u64,
        items: u32,
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
