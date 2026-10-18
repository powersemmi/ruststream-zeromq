//! Dialing: a pipeline worker dials the ventilator it pulls jobs from and the sink it pushes
//! results to, the shape of a worker that can run in as many copies as the load needs.
//!
//! ```text
//! cargo test --example zmq_broker_connect --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_zeromq::queue::prelude::*;

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    tracing::info!(job.id, "working on the job");
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    // The sink listens; this worker dials it and only publishes there.
    let sink = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let results = sink.bind(Publish);
    let failed = sink.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled(
            "ventilator",
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                // The ventilator only pushes: results and retry copies leave for the sink.
                b.include(work)
                    .out_reply(results)
                    .out_retry(failed)
                    .to("jobs.failed");
            },
        )
        .with_broker_labeled("sink", sink, |_b| {})
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::{Done, Job, app};

    #[tokio::test]
    async fn a_job_the_ventilator_pushes_ends_at_the_sink() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("ventilator")
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker_named("ventilator")
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7 });
        tb.broker_named("sink")
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
