//! The side as a type: a worker's routes on a queue it dials name `ZmqQueue<Connect>`, and one
//! helper builds the queue on either side.
//!
//! ```text
//! cargo test --example zmq_broker_role --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream::runtime::Bound;
use ruststream_zeromq::queue::prelude::*;
use ruststream_zeromq::{Connect, EndpointRole};

/// Works a job, and asks for another try in a second when it carries no id.
#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    if job.id == 0 {
        return HandlerOutcome::retry_after(Duration::from_secs(1));
    }
    tracing::info!(job.id, "working on the job");
    HandlerOutcome::ack()
}

// --8<-- [start:routes]
/// The worker's routes on a queue it dials: the copies go to the queue `copies` publishes to.
fn routes(copies: Bound<ZmqQueue, Publish>) -> impl RouterDef<ZmqQueue<Connect>> {
    Router::<ZmqQueue<Connect>>::new()
        .include(work)
        .max_attempts(nonzero!(3u32))
        .out_retry(copies)
        .to("jobs")
        .build()
}

/// One queue setting for both sides of an endpoint.
fn queue<Role: EndpointRole>(endpoint: ZmqEndpoint<Role>) -> ZmqQueue<Role> {
    ZmqQueue::new(endpoint).read_ahead(nonzero!(64_usize))
}

#[ruststream::app]
fn app() -> impl App {
    let retries = queue(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let copies = retries.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(queue(ZmqEndpoint::connect("tcp://ventilator:5555")), |b| {
            b.include_router(routes(copies));
        })
        .with_broker(retries, |b| {
            b.include(work).max_attempts(nonzero!(3u32));
        })
}
// --8<-- [end:routes]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test(start_paused = true)]
    async fn the_retry_copy_is_worked_on_the_bound_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue<Connect>>()
            .message(&Job { id: 0 })
            .publish()
            .await?;
        tb.broker::<ZmqQueue<Connect>>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(1)));

        tb.advance(Duration::from_secs(1)).await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 0 });
        tb.shutdown().await?;
        Ok(())
    }
}
