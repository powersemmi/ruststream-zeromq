//! The crate's prelude: a routes file that mounts two patterns imports `ruststream_zeromq::prelude`
//! and names each policy by its prefixed name.
//!
//! ```text
//! cargo test --example zmq_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_zeromq::prelude::*;

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    tracing::info!(job.id, "working on the job");
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    // Every listener that dials the fan-out hears about each finished job.
    let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out_reply(announce);
            },
        )
        .register_broker(events)
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs.done")]
struct Done {
    id: u64,
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
