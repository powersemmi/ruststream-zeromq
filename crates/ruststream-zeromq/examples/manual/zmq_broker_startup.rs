//! The service of `examples/zmq_broker_startup.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_startup --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "working on the job");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
/// The worker on the address `jobs` names; nothing is opened until the runtime starts it.
fn service(jobs: &str) -> RustStream {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind(jobs)),
        |b| {
            b.include(subscriber("jobs", Work).build());
        },
    )
}

fn app() -> impl App {
    service("tcp://0.0.0.0:5555")
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

// `#[outgoing(name = "jobs")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Job {
    type Form = FixedName;
    const DESTINATION: &'static str = "jobs";
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
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
