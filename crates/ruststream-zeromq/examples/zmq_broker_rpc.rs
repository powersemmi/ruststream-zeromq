//! Request and reply on DEALER/ROUTER: a responder binds the endpoint and answers each request at
//! the peer that sent it.
//!
//! ```text
//! cargo test --example zmq_broker_rpc --features testing
//! ```

use serde::{Deserialize, Serialize};

// The prelude's `Outgoing` is the derive; the publish pipeline's message of the same name is
// the type a transform rewrites.
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_zeromq::rpc::prelude::*;

// --8<-- [start:app]
// The answer goes to the peer that asked, so the clause's name is only the fallback.
#[subscriber("greeter", reply("reply"))]
async fn greet(request: &Greeting) -> Answer {
    tracing::info!(who = %request.who, "greeting");
    Answer {
        text: format!("hello {}", request.who),
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
        |b| {
            // A responder addresses no retry copies, so the mount names where they go.
            b.include(greet)
                .out_reply(Publish)
                .transform(ReplyToRequester)
                .out_retry(Publish)
                .to("greeter.retry");
        },
    )
}
// --8<-- [end:app]

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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "greeter")]
struct Greeting {
    who: String,
}

// No destination of its own: the ROUTER addresses each answer per request.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Answer {
    text: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::rpc::prelude::*;

    use super::{Greeting, ReplyToRequester, app, greet};

    #[tokio::test]
    async fn a_request_is_answered_at_the_peer_that_sent_it() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqRpc>()
            .message(&Greeting {
                who: "world".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<ZmqRpc>()
            .subscriber("greeter")
            .assert_called_once()
            .with(&Greeting {
                who: "world".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_responder_that_names_no_retry_destination_does_not_start() {
        let app = RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
            ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
            |b| {
                b.include(greet)
                    .out_reply(Publish)
                    .transform(ReplyToRequester);
            },
        );

        let Err(refused) = TestApp::start(app).await else {
            panic!("a responder addresses no retry copies, so its mount must name where they go");
        };
        let refused = refused.to_string();
        assert!(
            refused.contains("greeter") && refused.contains("NamedCopies"),
            "the refusal names the subscription and the fix, got: {refused}",
        );
    }
}
