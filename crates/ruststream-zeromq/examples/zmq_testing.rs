//! A test of a `ZeroMQ` service: the harness starts the app `main` runs, in process, and the test
//! reads what the handler received and what it published.
//!
//! ```text
//! cargo test --example zmq_testing --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    tracing::info!(job.id, "job done");
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    // The results leave for a sink this service dials, so they never come back to `jobs`.
    let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let to_results = results.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out_reply(to_results);
            },
        )
        .with_broker(results, |_b| {})
}

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
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Done, Job, app};

    // --8<-- [start:test]
    #[tokio::test]
    async fn a_job_is_worked_and_its_result_published() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // The job arrives the way a ventilator pushes it.
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7 });
        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:test]
}
