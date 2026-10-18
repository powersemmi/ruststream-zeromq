//! The service of `examples/zmq_publishing_publish.rs` without the macros: the handler is a named
//! type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_publishing_publish --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::prelude::*;

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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipped {
    id: u64,
}

impl OutgoingDestination for Shipped {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders.shipped";
}

impl MessageHeaders for Shipped {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
/// The slot shipments leave through.
struct Events;

impl OutSlot for Events {
    const NAME: &'static str = "Events";
    // `Shipped` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Events> for Shipped {}

struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Events, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(order.id, "shipping");
        // A send waits up to five seconds for a subscriber to attach, then fails.
        let shipped = Shipped { id: order.id };
        if let Err(err) = outs.get(Events).message(&shipped).publish().await {
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not announced");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    // No subscription of this service reads the fan-out, so its publisher binds the endpoint.
    let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("shipping", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("orders", Ship).build())
                    .out(Events, announce)
                    .build();
            },
        )
        .register_broker(events)
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::prelude::*;

    use super::{Order, Shipped, app};

    #[tokio::test]
    async fn an_order_is_announced_on_the_fan_out() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<ZmqFanout>()
            .published::<Shipped>("orders.shipped")
            .assert_called_once()
            .with(&Shipped { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
