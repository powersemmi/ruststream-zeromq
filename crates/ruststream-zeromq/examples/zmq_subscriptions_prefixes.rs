//! On PUB/SUB a subscription's name is a prefix filter on frame 0, compared byte by byte. A name
//! that ends with the separator takes one family of names and nothing that merely starts alike.
//!
//! ```text
//! cargo test --example zmq_subscriptions_prefixes --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:prefixes]
use ruststream_zeromq::fanout::prelude::*;

/// Every order event, `orders.created`, `orders.shipped`, and no `orders-archive` record.
#[subscriber("orders.")]
async fn audit(event: &OrderEvent) -> HandlerOutcome {
    tracing::info!(event.order_id, "order event audited");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("auditor", "0.1.0")).with_broker(
        ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
        |b| {
            b.include(audit);
        },
    )
}
// --8<-- [end:prefixes]

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
    async fn the_separator_keeps_a_lookalike_name_out() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (name, order_id) in [
            ("orders.created", 1),
            ("orders.shipped", 2),
            ("orders-archive", 3),
        ] {
            tb.broker::<ZmqFanout>()
                .message(&OrderEvent { order_id })
                .to(name)
                .publish()
                .await?;
        }

        tb.broker::<ZmqFanout>()
            .subscriber("orders.")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        assert_eq!(
            tb.broker::<ZmqFanout>()
                .subscriber("orders.")
                .received::<OrderEvent>(),
            [OrderEvent { order_id: 1 }, OrderEvent { order_id: 2 }],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
