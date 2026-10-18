//! Competing consumers on PUSH/PULL: two workers dial one ventilator, and each job it pushes
//! reaches one of them.
//!
//! ```text
//! cargo test --example zmq_broker_queue --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "working on the job");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // The ventilator only pushes, so the retry copies go to a queue this service binds.
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let first = retries.bind(Publish);
    let second = retries.bind(Publish);
    RustStream::new(AppInfo::new("workers", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                // Two workers dial the ventilator, and each job it pushes reaches one of them.
                b.include(work).out_retry(first).to("jobs");
                b.include(work).out_retry(second).to("jobs");
            },
        )
        .with_broker(retries, |b| {
            b.include(work);
        })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn each_pushed_job_reaches_one_worker() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [1, 2] {
            tb.broker::<ZmqQueue<Connect>>()
                .message(&Job { id })
                .publish()
                .await?;
        }

        let workers = tb.broker::<ZmqQueue<Connect>>();
        assert_eq!(
            workers.subscriber("jobs").received::<Job>(),
            [Job { id: 1 }, Job { id: 2 }],
        );
        workers
            .subscriber("jobs")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
