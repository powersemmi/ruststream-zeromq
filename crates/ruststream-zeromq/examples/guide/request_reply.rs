//! The responder of the Request and reply guide: a scoring service that binds a ROUTER and answers
//! each request at the peer that sent it.
//!
//! ```text
//! cargo run --example guide_request_reply --features testing -- run
//! cargo test --example guide_request_reply --features testing
//! ```

// The prelude's `Outgoing` is the derive; the publish pipeline's message of the same name is the
// type a transform rewrites.
use ruststream::runtime::{Outgoing, PublishContext};

// --8<-- [start:responder]
use ruststream_zeromq::rpc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize)]
struct ScoreRequest {
    customer: u64,
    amount_cents: u64,
}

// No destination of its own: the ROUTER addresses each answer per request.
#[derive(Debug, PartialEq, Serialize, Outgoing)]
struct Score {
    customer: u64,
    approved: bool,
}

// The answer goes to the peer that asked, so the clause's name is only the fallback.
#[subscriber("score", reply("reply"))]
async fn score(request: &ScoreRequest) -> Result<Score, HandlerOutcome> {
    if request.amount_cents == 0 {
        tracing::warn!(request.customer, "a request without an amount, dropped");
        return Err(HandlerOutcome::drop());
    }
    tracing::info!(request.customer, request.amount_cents, "scoring");
    Ok(Score {
        customer: request.customer,
        approved: request.amount_cents <= 50_000,
    })
}

#[ruststream::app]
fn app() -> impl App {
    // The responder is the stable side: it binds, and every requester dials it.
    RustStream::new(AppInfo::new("scoring", "0.1.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
        |b| {
            // A responder addresses no retry copies, so the mount names where they go.
            b.include(score)
                .out_reply(Publish)
                .transform(ReplyToRequester)
                .out_retry(Publish)
                .to("score.retry");
        },
    )
}
// --8<-- [end:responder]

// --8<-- [start:transform]
/// Answers at the `reply-to` address the ROUTER stamped on the request, under the request's
/// `correlation-id`.
struct ReplyToRequester;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(reply_to) = cx.headers().get_shared("reply-to")
            && let Ok(reply_to) = Str::try_from(reply_to)
        {
            out.set_name(reply_to);
        }
        if let Some(correlation) = cx.headers().get_shared("correlation-id") {
            out.headers_mut()
                .insert(Str::from_static("correlation-id"), correlation);
        }
    }
}
// --8<-- [end:transform]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::rpc::prelude::*;
    use serde::Serialize;

    use super::{ScoreRequest, app};

    /// A request as the DEALER of a requester sends it.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "score")]
    struct Asked {
        customer: u64,
        amount_cents: u64,
    }

    #[tokio::test]
    async fn a_request_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqRpc>()
            .message(&Asked {
                customer: 7,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<ZmqRpc>()
            .subscriber("score")
            .assert_called_once()
            .with(&ScoreRequest {
                customer: 7,
                amount_cents: 0,
            })
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_request_with_an_amount_is_answered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqRpc>()
            .message(&Asked {
                customer: 8,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<ZmqRpc>()
            .subscriber("score")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
