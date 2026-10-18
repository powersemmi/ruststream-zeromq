//! Replies on PUSH/PULL: a worker answers each job through a queue on an endpoint of its own,
//! because the queue its subscription binds would hand the answer back to it.
//!
//! ```text
//! cargo test --example zmq_publishing_replies --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

// --8<-- [start:reply]
/// The result names its own destination, so the subscriber clause stays bare.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    tracing::info!(job.id, "working");
    Done { id: job.id }
}
// --8<-- [end:reply]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // The sink binds its own endpoint; this worker dials it to deliver the results.
    let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let to_results = results.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out_reply(to_results);
            },
        )
        .register_broker(results)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Done, Job, app, work};

    #[tokio::test]
    async fn a_result_leaves_through_the_sinks_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_result_on_the_jobs_queue_is_refused() -> Result<(), Box<dyn Error>> {
        // The reply goes back through the queue the jobs arrive on, where it would come back to
        // `work` as its next job: the publisher refuses it.
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out_reply(Publish);
            },
        );

        let tb = TestApp::start(app).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 8 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 8 });
        tb.broker::<ZmqQueue>()
            .published::<Done>("results")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
