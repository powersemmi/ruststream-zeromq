//! The service of `examples/zmq_broker_errors.rs` without the macros: the handler is a named type,
//! the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_errors --features testing
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

/// The slot results leave through.
struct Results;

impl OutSlot for Results {
    const NAME: &'static str = "Results";
    // `Done` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Results> for Done {}

// --8<-- [start:handler]
struct Work;

impl<Entry> Handle<Job, (), Outs<(Entry,)>> for Work
where
    Entry: OutEntry<Results, Wire: Publisher>,
{
    async fn handle(
        &self,
        job: &Job,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let done = Done { id: job.id };
        if let Err(err) = outs.get(Results).message(&done).publish().await {
            // The error names the destination and the socket's reason.
            tracing::warn!(subscription = "jobs", job.id, error = %err, "result not published");
            return Err(HandlerOutcome::retry_after(Duration::from_secs(1)));
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    let sink = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let results = sink.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("jobs", Work).build())
                    .out(Results, results)
                    .build();
            },
        )
        .register_broker(sink)
}

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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
}

// `#[outgoing(name = "results")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Done {
    type Form = FixedName;
    const DESTINATION: &'static str = "results";
}

impl MessageHeaders for Done {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use ruststream_zeromq::{Connect, ZmqError};

    use super::{Done, Job, Results, Work, app};

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
        // to `Work` as its next job: the publisher refuses them.
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("jobs", Work).build())
                    .out(Results, Publish)
                    .build();
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
                b.include(subscriber("jobs", Work).build())
                    .out(Results, Publish)
                    .build();
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
