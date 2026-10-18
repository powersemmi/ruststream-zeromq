//! Startup: `connect` checks the endpoint without I/O, and the subscription binds its socket as
//! it opens. A scheme `ZeroMQ` does not carry and a port another process holds each fail the start,
//! naming the address.
//!
//! ```text
//! cargo test --example zmq_broker_startup --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "working on the job");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// The worker on the address `jobs` names; nothing is opened until the runtime starts it.
fn service(jobs: &str) -> RustStream {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind(jobs)),
        |b| {
            b.include(work);
        },
    )
}

#[ruststream::app]
fn app() -> impl App {
    service("tcp://0.0.0.0:5555")
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
    use std::net::TcpListener;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::ZmqError;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Job, app, service};

    /// The `ZmqError` somewhere in the chain of `err`'s sources.
    fn zmq_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a ZmqError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(zmq) = err.downcast_ref::<ZmqError>() {
                return Some(zmq);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn the_service_starts_and_works_a_job() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_scheme_zeromq_does_not_carry_fails_the_start() {
        let Err(refused) = TestApp::start(service("udp://0.0.0.0:5555")).await else {
            panic!("the endpoint check refuses udp://");
        };

        let zmq = zmq_error(&refused).expect("the start fails with a ZmqError");
        assert!(matches!(zmq, ZmqError::Invalid(_)), "{zmq}");
        assert!(zmq.to_string().contains("udp://0.0.0.0:5555"), "{zmq}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_port_another_process_holds_fails_the_start() -> Result<(), Box<dyn Error>> {
        let held = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("tcp://{}", held.local_addr()?);

        let Err(refused) = TestApp::start_live(service(&address)).await else {
            panic!("the bind finds the port taken");
        };

        let zmq = zmq_error(&refused).expect("the start fails with a ZmqError");
        assert!(matches!(zmq, ZmqError::Endpoint { .. }), "{zmq}");
        assert!(zmq.to_string().contains(&address), "{zmq}");
        Ok(())
    }
}
