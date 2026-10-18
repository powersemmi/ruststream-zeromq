//! Broadcast on PUB/SUB: an auditor binds a fan-out, and its subscription on `orders` receives
//! every message whose name starts with `orders`.
//!
//! ```text
//! cargo test --example zmq_broker_fanout --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_zeromq::fanout::prelude::*;

#[subscriber("orders")]
async fn audit(event: &OrderEvent) -> HandlerOutcome {
    tracing::info!(event.order_id, "order event audited");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // The order services dial this endpoint and publish `orders.created`, `orders.shipped`, ...
    RustStream::new(AppInfo::new("auditor", "0.1.0")).with_broker(
        ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
        |b| {
            b.include(audit);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct OrderEvent {
    order_id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::fanout::prelude::*;

    use super::{OrderEvent, app};

    #[tokio::test]
    async fn the_subscription_receives_every_name_under_its_prefix() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (name, order_id) in [
            ("orders.created", 1),
            ("orders.shipped", 1),
            ("payments.settled", 1),
        ] {
            tb.broker::<ZmqFanout>()
                .message(&OrderEvent { order_id })
                .to(name)
                .publish()
                .await?;
        }

        tb.broker::<ZmqFanout>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
