//! The service of `examples/zmq_publishing_cancellation.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_publishing_cancellation --features testing
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::prelude::*;

/// How long a quote may take before the order is tried again later.
const QUOTE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
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
struct Price;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Price
where
    Entry: OutEntry<DefaultSlot, Wire: RequestReply>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let ask = serde_json::to_vec(&Ask { id: order.id }).map_err(|_| HandlerOutcome::drop())?;
        let request = OutgoingMessage::new("quotes", ask.as_slice());
        // The timeout is how the handler gives up: dropping a pending request instead would close
        // its DEALER and lose an answer already on its way.
        let answer = outs
            .get(DefaultSlot)
            .request(request, QUOTE_TIMEOUT)
            .await
            .map_err(|err| {
                tracing::warn!(subscription = "orders", order.id, error = %err, "no quote");
                HandlerOutcome::retry_after(Duration::from_secs(30))
            })?;
        let quote = serde_json::from_slice::<Quote>(answer.payload())
            .map_err(|_| HandlerOutcome::drop())?;
        tracing::info!(order.id, quote.price, "priced");
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    let pricing = ZmqRpc::new(ZmqEndpoint::connect("tcp://pricing:5557")).bindable();
    let quotes = pricing.bind(ZmqRpcPublish);
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("orders", Price).build())
                    .out(DefaultSlot, quotes)
                    .build();
            },
        )
        .register_broker(pricing)
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
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
