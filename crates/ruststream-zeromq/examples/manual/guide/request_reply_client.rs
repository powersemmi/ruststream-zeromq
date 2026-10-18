//! The requester of the Request and reply guide, written without the macros: the handler is a
//! type with an `impl Handle` over its out slots, mounted with the `subscriber` constructor, and
//! `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_guide_request_reply_client --features testing -- run
//! cargo test --example manual_guide_request_reply_client --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:requester]
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_zeromq::prelude::*;
use serde::{Deserialize, Serialize};

/// How long one request waits for its answer.
const ASK_TIMEOUT: Duration = Duration::from_secs(2);
/// When the order is asked about again after the scoring service did not answer.
const ASK_AGAIN: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    customer: u64,
    amount_cents: u64,
}

#[derive(Serialize)]
struct ScoreRequest {
    customer: u64,
    amount_cents: u64,
}

#[derive(Deserialize)]
struct Score {
    approved: bool,
}

struct Check;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Check
where
    Entry: OutEntry<DefaultSlot, Wire: RequestReply>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let request = serde_json::to_vec(&ScoreRequest {
            customer: order.customer,
            amount_cents: order.amount_cents,
        })
        .map_err(|_| HandlerOutcome::drop())?;
        let request = OutgoingMessage::new("score", request.as_slice());
        let answer = match outs.get(DefaultSlot).request(request, ASK_TIMEOUT).await {
            Ok(answer) => answer,
            Err(err) => {
                tracing::warn!(order.id, %err, "no score, asking again later");
                return Err(HandlerOutcome::retry_after(ASK_AGAIN));
            }
        };
        let score = serde_json::from_slice::<Score>(answer.payload())
            .map_err(|_| HandlerOutcome::drop())?;
        tracing::info!(order.id, score.approved, "order checked");
        Ok(())
    }
}

fn app() -> impl App {
    // The scoring service binds; every requester dials it.
    let scoring = ZmqRpc::new(ZmqEndpoint::connect("tcp://scoring:5557")).bindable();
    let ask = scoring.bind(ZmqRpcPublish);
    RustStream::new(AppInfo::new("checkout", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("orders", Check).build())
                    .max_attempts(nonzero!(3u32))
                    .out(DefaultSlot, ask)
                    .build();
            },
        )
        .with_broker(scoring, |_b| {})
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:requester]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::prelude::*;
    use serde::Serialize;

    use super::{ASK_AGAIN, app};

    /// An order as the shop pushes it.
    #[derive(Serialize)]
    struct Placed {
        id: u64,
        customer: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Placed {
        type Form = FixedName;
        const DESTINATION: &'static str = "orders";
    }

    impl MessageHeaders for Placed {
        type Contract = NoHeaders;
    }

    #[tokio::test(start_paused = true)]
    async fn an_order_nobody_scores_is_asked_about_again_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // No scoring service answers in process, so the request times out.
        tb.broker::<ZmqQueue>()
            .message(&Placed {
                id: 7,
                customer: 3,
                amount_cents: 1250,
            })
            .publish()
            .await?;
        tb.broker::<ZmqQueue>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(ASK_AGAIN));

        tb.advance(ASK_AGAIN).await?;
        tb.broker::<ZmqQueue>()
            .subscriber("orders")
            .assert_called(2);
        tb.broker::<ZmqQueue>()
            .published::<()>("orders")
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }
}
