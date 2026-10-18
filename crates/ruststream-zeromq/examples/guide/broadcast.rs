//! The announcer of the Broadcast guide: a worker that takes jobs on the queue it binds and
//! announces each result on a fan-out it binds, for every watcher that subscribed.
//!
//! ```text
//! cargo run --example guide_broadcast --features testing -- run
//! cargo test --example guide_broadcast --features testing
//! ```

// --8<-- [start:announcer]
// One file, two patterns: the crate prelude names each policy after its pattern.
use ruststream_zeromq::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize)]
struct Job {
    id: u64,
    items: u32,
}

/// The announcement, under a name every `jobs.` watcher matches.
#[derive(Debug, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "jobs.done")]
struct JobDone {
    id: u64,
}

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> JobDone {
    tracing::info!(job.id, job.items, "job worked");
    JobDone { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    // The watchers dial this endpoint; the announcer is the stable side, so it binds.
    let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out_reply(announce);
            },
        )
        .with_broker(events, |_b| {})
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
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "jobs")]
    struct Pushed {
        id: u64,
        items: u32,
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
