//! The service of `examples/zmq_wire_headers.rs` without the macros: the handler is a named type,
//! the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_wire_headers --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

// --8<-- [start:handler]
/// The header line `x-tenant: acme`, as a contract both directions check.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Tenant {
    #[serde(rename = "x-tenant")]
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
}

// `#[outgoing(name = "results", headers = Tenant)]` by hand: a result cannot be published
// without its tenant.
impl OutgoingDestination for Done {
    type Form = FixedName;
    const DESTINATION: &'static str = "results";
}

impl MessageHeaders for Done {
    type Contract = WithHeaders<Tenant>;
}

/// The slot results leave through.
struct Results;

impl OutSlot for Results {
    const NAME: &'static str = "Results";
    // `Done` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Results> for Done {}

struct Work;

impl<Entry> Handle<Job, (), Outs<(Entry,)>> for Work
where
    Entry: OutEntry<Results, Wire: Publisher>,
{
    async fn handle(
        &self,
        job: &Job,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Headers(tenant) = Headers::<Tenant>::from_context(ctx).await?;
        tracing::info!(job.id, tenant = %tenant.tenant, "working");
        let done = Done { id: job.id };
        if let Err(err) = outs
            .get(Results)
            .message(&done)
            .with_headers(&tenant)
            .publish()
            .await
        {
            tracing::warn!(subscription = "jobs", job.id, error = %err, "result not published");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Done, Job, Tenant, app};

    #[tokio::test]
    async fn the_tenant_travels_from_the_job_to_its_result() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let tenant = Tenant {
            tenant: "acme".to_owned(),
        };
        tb.broker::<ZmqQueue>()
            .publish_with_headers("jobs", &Job { id: 7 }, &tenant)
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 })
            .with_header("x-tenant", "acme");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_header_frame_that_is_not_text_reads_as_no_headers() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // A peer that writes raw bytes into the header frame: the delivery reads as one without
        // headers instead of guessing what the bytes meant, so the tenant the handler requires
        // is missing and no result leaves.
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", [0xff, 0xfe].as_slice());
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 8 })
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
