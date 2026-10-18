//! The header frame: a worker reads the tenant a peer names in a `x-tenant` header line, and its
//! result carries the same header to the sink.
//!
//! ```text
//! cargo test --example zmq_wire_headers --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

// --8<-- [start:handler]
/// The header line `x-tenant: acme`, as a contract both directions check.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Tenant {
    #[serde(rename = "x-tenant")]
    tenant: String,
}

/// A result cannot be published without its tenant.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results", headers = Tenant)]
struct Done {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Done)]
struct Results;

#[subscriber("jobs")]
async fn work(
    job: &Job,
    Headers(tenant): Headers<Tenant>,
    Out(results): Out<impl Publisher, Results>,
) -> HandlerOutcome {
    tracing::info!(job.id, tenant = %tenant.tenant, "working");
    match results
        .message(&Done { id: job.id })
        .with_headers(&tenant)
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(subscription = "jobs", job.id, error = %err, "result not published");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:handler]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

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
    async fn a_header_frame_that_is_not_text_never_reaches_the_handler()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // A peer that writes raw bytes into the header frame: the subscription refuses the
        // message as a wire error instead of guessing what the bytes meant.
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", [0xff, 0xfe].as_slice());
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 8 })
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_not_called();
        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
