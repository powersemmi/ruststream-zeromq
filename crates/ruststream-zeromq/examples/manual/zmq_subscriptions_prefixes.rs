//! The service of `examples/zmq_subscriptions_prefixes.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_subscriptions_prefixes --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:prefixes]
use ruststream_zeromq::fanout::prelude::*;

/// Every order event, `orders.created`, `orders.shipped`, and no `orders-archive` record.
struct Audit;

impl Handle<OrderEvent> for Audit {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.order_id, "order event audited");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("auditor", "0.1.0")).with_broker(
        ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
        |b| {
            b.include(subscriber("orders.", Audit).build());
        },
    )
}
// --8<-- [end:prefixes]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderEvent {
    order_id: u64,
}

// `#[derive(Outgoing)]` with no name by hand: the call site names the destination.
impl OutgoingDestination for OrderEvent {
    type Form = CallerName;
}

impl MessageHeaders for OrderEvent {
    type Contract = NoHeaders;
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
