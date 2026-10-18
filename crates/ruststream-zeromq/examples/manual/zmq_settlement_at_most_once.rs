//! The service of `examples/zmq_settlement_at_most_once.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_settlement_at_most_once --features testing
//! ```

use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

/// Stands in for the payment gateway, which times out on large amounts at busy hours.
fn gateway(payment: &Payment) -> io::Result<()> {
    if payment.amount > 1_000 {
        return Err(io::Error::from(io::ErrorKind::TimedOut));
    }
    Ok(())
}

// --8<-- [start:settle]
struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            // Nothing to charge. The delivery is gone either way, so it is dropped on purpose.
            tracing::warn!(payment.id, "a payment of nothing, dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(gateway(payment).map_err(|err| {
            // The socket keeps no copy, so the runtime publishes one after the delay.
            tracing::warn!(payment.id, %err, "the gateway failed, trying again later");
            HandlerOutcome::retry_after(Duration::from_secs(10))
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(subscriber("payments", Charge).build());
        },
    )
}
// --8<-- [end:settle]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    amount: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Payment, app};

    #[tokio::test(start_paused = true)]
    async fn a_dropped_payment_never_comes_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Payment { id: 1, amount: 0 })
            .publish()
            .await?;
        tb.advance(Duration::from_secs(60)).await?;

        tb.broker::<ZmqQueue>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_payment_comes_back_as_a_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 2,
            amount: 5_000,
        };
        tb.broker::<ZmqQueue>().message(&payment).publish().await?;
        tb.broker::<ZmqQueue>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(10)));

        tb.advance(Duration::from_secs(10)).await?;

        tb.broker::<ZmqQueue>()
            .subscriber("payments")
            .assert_called(2)
            .with(&payment);
        tb.broker::<ZmqQueue>()
            .published::<Payment>("payments")
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }
}
