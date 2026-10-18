//! The service of `examples/zmq_broker_role.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_role --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::Bound;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;
use ruststream_zeromq::{Connect, EndpointRole};

/// Works a job, and asks for another try in a second when it carries no id.
struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if job.id == 0 {
            return ready(Err(HandlerOutcome::retry_after(Duration::from_secs(1))));
        }
        tracing::info!(job.id, "working on the job");
        ready(Ok(()))
    }
}

// --8<-- [start:routes]
/// The worker's routes on a queue it dials: the copies go to the queue `copies` publishes to.
fn routes(copies: Bound<ZmqQueue, Publish>) -> impl RouterDef<ZmqQueue<Connect>> {
    Router::<ZmqQueue<Connect>>::new()
        .include(subscriber("jobs", Work).build())
        .max_attempts(nonzero!(3u32))
        .out_retry(copies)
        .to("jobs")
        .build()
}

/// One queue setting for both sides of an endpoint.
fn queue<Role: EndpointRole>(endpoint: ZmqEndpoint<Role>) -> ZmqQueue<Role> {
    ZmqQueue::new(endpoint).read_ahead(nonzero!(64_usize))
}

fn app() -> impl App {
    let retries = queue(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let copies = retries.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(queue(ZmqEndpoint::connect("tcp://ventilator:5555")), |b| {
            b.include_router(routes(copies));
        })
        .with_broker(retries, |b| {
            b.include(subscriber("jobs", Work).build())
                .max_attempts(nonzero!(3u32));
        })
}
// --8<-- [end:routes]

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
