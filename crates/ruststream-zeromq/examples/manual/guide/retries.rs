//! The service of the Retries guide, written without the macros: the handlers are types with an
//! `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_guide_retries --features testing -- run
//! cargo test --example manual_guide_retries --features testing
//! ```

use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:jobs]
use std::time::Duration;

use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

/// How long a failed job waits before its next delivery.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(run(job).map_err(|err| {
            tracing::warn!(job.id, %err, "the job failed, retrying later");
            HandlerOutcome::retry_after(BACKOFF)
        }))
    }
}
// --8<-- [end:jobs]

// --8<-- [start:invoices]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    id: u64,
    amount_cents: u64,
}

struct SendInvoice;

impl Handle<Invoice> for SendInvoice {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if invoice.amount_cents == 0 {
            tracing::warn!(invoice.id, "an invoice without an amount, parking it");
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(invoice.id, invoice.amount_cents, "invoice sent");
        ready(Ok(()))
    }
}

/// Keeps the invoices nobody could send, for a person to look at.
struct Park;

impl Handle<Invoice> for Park {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(invoice.id, "invoice parked");
        ready(Ok(()))
    }
}
// --8<-- [end:invoices]

// --8<-- [start:app]
fn app() -> impl App {
    let dead = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5561")).bindable();
    let parked = dead.bind(Publish);
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker_labeled(
            "jobs",
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                // Five deliveries at most, the first one included; the copies return to this queue.
                b.include(subscriber("jobs", Work).build())
                    .max_attempts(nonzero!(5u32))
                    .out_retry(Publish);
            },
        )
        .with_broker_labeled(
            "invoices",
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
            |b| {
                // A failed invoice leaves for the second queue, under the name it is read by.
                b.include(subscriber("invoices", SendInvoice).build())
                    .dead_letter("invoices.dead")
                    .out_retry(parked);
            },
        )
        .with_broker_labeled("dead", dead, |b| {
            b.include(subscriber("invoices.dead", Park).build());
        })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

/// Stands in for the work: a job without an id fails, and is worth another try.
fn run(job: &Job) -> io::Result<()> {
    if job.id == 0 {
        return Err(io::Error::other("the job carries no id"));
    }
    tracing::info!(job.id, "job done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use serde::Serialize;

    use super::{BACKOFF, Invoice, Job, app};

    /// A job as the peer that pushes it frames it.
    #[derive(Serialize)]
    struct PushedJob {
        id: u64,
    }

    impl OutgoingDestination for PushedJob {
        type Form = FixedName;
        const DESTINATION: &'static str = "jobs";
    }

    impl MessageHeaders for PushedJob {
        type Contract = NoHeaders;
    }

    /// An invoice as the peer that pushes it frames it.
    #[derive(Serialize)]
    struct PushedInvoice {
        id: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for PushedInvoice {
        type Form = FixedName;
        const DESTINATION: &'static str = "invoices";
    }

    impl MessageHeaders for PushedInvoice {
        type Contract = NoHeaders;
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_job_gets_five_deliveries() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("jobs")
            .message(&PushedJob { id: 0 })
            .publish()
            .await?;
        for _ in 0..5 {
            tb.advance(BACKOFF).await?;
        }

        tb.broker_named("jobs")
            .subscriber("jobs")
            .assert_called(5)
            .with(&Job { id: 0 })
            .settled(HandlerOutcome::retry_after(BACKOFF));
        tb.broker_named("jobs")
            .published::<Job>("jobs")
            .with_header(RETRY_COUNT_HEADER, "4");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_invoice_that_fails_is_parked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("invoices")
            .message(&PushedInvoice {
                id: 9,
                amount_cents: 0,
            })
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker_named("invoices")
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker_named("dead")
            .subscriber("invoices.dead")
            .assert_called_once()
            .with(&Invoice {
                id: 9,
                amount_cents: 0,
            });
        tb.shutdown().await?;
        Ok(())
    }
}
