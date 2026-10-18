//! `ZeroMQ` settles nothing. A delivery is the socket's only copy: `ack` and `nack` report
//! `Unsupported`, and a delivery that should come back is a copy this service publishes.
//!
//! ```text
//! cargo test --example zmq_settlement_at_most_once --features testing
//! ```

use std::io;
use std::time::Duration;

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
#[subscriber("payments")]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        // Nothing to charge. The delivery is gone either way, so it is dropped on purpose.
        tracing::warn!(payment.id, "a payment of nothing, dropped");
        return HandlerOutcome::drop();
    }
    match gateway(payment) {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            // The socket keeps no copy, so the runtime publishes one after the delay.
            tracing::warn!(payment.id, %err, "the gateway failed, trying again later");
            HandlerOutcome::retry_after(Duration::from_secs(10))
        }
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(charge);
        },
    )
}
// --8<-- [end:settle]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount: u64,
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
