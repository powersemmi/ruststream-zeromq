//! Giving up on a request through its timeout: a handler asks a pricing service over
//! DEALER/ROUTER and bounds the wait with the request's own timeout, never with a `select!` arm.
//!
//! ```text
//! cargo test --example zmq_publishing_cancellation --features testing
//! ```

use std::time::Duration;

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::prelude::*;

/// How long a quote may take before the order is tried again later.
const QUOTE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Ask {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Quote {
    price: u64,
}

// --8<-- [start:handler]
#[subscriber("orders")]
async fn price(order: &Order, Out(pricing): Out<impl RequestReply>) -> HandlerOutcome {
    let Ok(ask) = serde_json::to_vec(&Ask { id: order.id }) else {
        return HandlerOutcome::drop();
    };
    let request = OutgoingMessage::new("quotes", ask.as_slice());
    // The timeout is how the handler gives up: dropping a pending request instead would close
    // its DEALER and lose an answer already on its way.
    match pricing.request(request, QUOTE_TIMEOUT).await {
        Ok(answer) => {
            let Ok(quote) = serde_json::from_slice::<Quote>(answer.payload()) else {
                return HandlerOutcome::drop();
            };
            tracing::info!(order.id, quote.price, "priced");
            HandlerOutcome::ack()
        }
        Err(err) => {
            tracing::warn!(subscription = "orders", order.id, error = %err, "no quote");
            HandlerOutcome::retry_after(Duration::from_secs(30))
        }
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let pricing = ZmqRpc::new(ZmqEndpoint::connect("tcp://pricing:5557")).bindable();
    let quotes = pricing.bind(ZmqRpcPublish);
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(price).out(DefaultSlot, quotes).build();
            },
        )
        .register_broker(pricing)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::prelude::*;

    use super::{Order, app};

    #[tokio::test(start_paused = true)]
    async fn an_order_with_no_quote_in_time_is_tried_again_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // The pricing service lives in another process, so nothing answers here and the request
        // runs out its timeout on the paused clock.
        tb.broker::<ZmqQueue>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(30)));
        tb.shutdown().await?;
        Ok(())
    }
}
