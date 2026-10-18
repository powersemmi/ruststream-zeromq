//! `ZmqError` where a service meets it: a publish the socket refuses, and a start that fails.
//!
//! ```text
//! cargo test --example zmq_broker_errors --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[derive(OutSlot)]
#[publishes(Done)]
struct Results;

// --8<-- [start:handler]
#[subscriber("jobs")]
async fn work(job: &Job, Out(results): Out<impl Publisher, Results>) -> HandlerOutcome {
    if let Err(err) = results.message(&Done { id: job.id }).publish().await {
        // The error names the destination and the socket's reason.
        tracing::warn!(subscription = "jobs", job.id, error = %err, "result not published");
        return HandlerOutcome::retry_after(Duration::from_secs(1));
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let sink = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let results = sink.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out(Results, results).build();
            },
        )
        .register_broker(sink)
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
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use ruststream_zeromq::{Connect, ZmqError};

    use super::{Done, Job, Results, app, work};

    #[tokio::test(start_paused = true)]
    async fn a_result_the_sink_takes_acks_the_job() -> Result<(), Box<dyn Error>> {
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

    #[tokio::test(start_paused = true)]
    async fn a_result_the_socket_refuses_retries_the_job() -> Result<(), Box<dyn Error>> {
        // The results leave through the queue the jobs arrive on, which would hand each one back
        // to `work` as its next job: the publisher refuses them.
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(work).out(Results, Publish).build();
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
            .settled(HandlerOutcome::retry_after(Duration::from_secs(1)));
        tb.broker::<ZmqQueue>()
            .published::<Done>("results")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_start_carries_the_zmq_error() {
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("udp://0.0.0.0:5555")),
            |b| {
                b.include(work).out(Results, Publish).build();
            },
        );

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(zmq) = err.downcast_ref::<ZmqError>() {
                assert!(matches!(zmq, ZmqError::Invalid(_)), "{zmq}");
                return;
            }
            cause = err.source();
        }
        panic!("the start did not fail with a ZmqError: {failed:?}");
    }
}
