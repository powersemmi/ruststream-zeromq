//! Where the retry copies go follows the side. A queue this service binds takes its own copies
//! back; a worker that dials a ventilator names a queue for them, here one it binds itself.
//!
//! ```text
//! cargo test --example zmq_settlement_out_retry --features testing
//! ```

use std::io;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

/// Stands in for the work: a job without an id fails, and is worth another try.
fn run(job: &Job) -> io::Result<()> {
    if job.id == 0 {
        return Err(io::Error::other("a job carries no id"));
    }
    tracing::info!(job.id, "job done");
    Ok(())
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    match run(job) {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(job.id, %err, "the job failed, retrying");
            HandlerOutcome::retry_after(Duration::from_secs(30))
        }
    }
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let copies = retries.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                // The ventilator takes nothing back, so the copies go to the queue bound below.
                b.include(handle)
                    .max_attempts(nonzero!(3u32))
                    .out_retry(copies)
                    .to("jobs");
            },
        )
        .with_broker(retries, |b| {
            // A bound queue takes its own copies back, so this mount names no destination.
            b.include(handle).max_attempts(nonzero!(3u32));
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
    use std::time::Duration;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app};

    #[tokio::test(start_paused = true)]
    async fn the_copies_reach_the_queue_the_worker_binds() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue<Connect>>()
            .message(&Job { id: 0 })
            .publish()
            .await?;
        for _ in 0..3 {
            tb.advance(Duration::from_secs(30)).await?;
        }

        // The first delivery came from the ventilator, the two copies from the bound queue.
        tb.broker::<ZmqQueue<Connect>>()
            .subscriber("jobs")
            .assert_called_once();
        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called(2)
            .with(&Job { id: 0 });
        tb.broker::<ZmqQueue>()
            .published::<Job>("jobs")
            .with_header(RETRY_COUNT_HEADER, "2");
        tb.shutdown().await?;
        Ok(())
    }
}
