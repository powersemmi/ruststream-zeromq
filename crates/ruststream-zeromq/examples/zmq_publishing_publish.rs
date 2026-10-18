//! Publishing from a handler: a worker reads orders from its PUSH/PULL queue and announces each
//! shipment on a PUB/SUB fan-out of its own, through an injected publisher.
//!
//! ```text
//! cargo test --example zmq_publishing_publish --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.shipped")]
struct Shipped {
    id: u64,
}

// --8<-- [start:handler]
/// The slot shipments leave through, and what it may publish.
#[derive(OutSlot)]
#[publishes(Shipped)]
struct Events;

#[subscriber("orders")]
async fn ship(order: &Order, Out(events): Out<impl Publisher, Events>) -> HandlerOutcome {
    tracing::info!(order.id, "shipping");
    // A send waits up to five seconds for a subscriber to attach, then fails.
    if let Err(err) = events.message(&Shipped { id: order.id }).publish().await {
        tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not announced");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // No subscription of this service reads the fan-out, so its publisher binds the endpoint.
    let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("shipping", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(ship).out(Events, announce).build();
            },
        )
        .register_broker(events)
}
// --8<-- [end:app]

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
