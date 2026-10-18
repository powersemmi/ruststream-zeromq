//! The service of `examples/zmq_broker_rpc.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_rpc --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::rpc::prelude::*;

// --8<-- [start:app]
struct Greet;

impl Handle<Greeting, Answer> for Greet {
    fn handle(
        &self,
        request: &Greeting,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Answer, HandlerOutcome>> {
        tracing::info!(who = %request.who, "greeting");
        ready(Ok(Answer {
            text: format!("hello {}", request.who),
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
        |b| {
            // The answer goes to the peer that asked, so the `to` name is only the fallback.
            // A responder addresses no retry copies, so the mount names where they go.
            b.include(subscriber("greeter", Greet).reply().to("reply").build())
                .out_reply(Publish)
                .transform(ReplyToRequester)
                .out_retry(Publish)
                .to("greeter.retry");
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Greeting {
    who: String,
}

// `#[outgoing(name = "greeter")]` by hand: the type names the responder it is sent to.
impl OutgoingDestination for Greeting {
    type Form = FixedName;
    const DESTINATION: &'static str = "greeter";
}

impl MessageHeaders for Greeting {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Answer {
    text: String,
}

// `#[derive(Outgoing)]` with no name by hand: the ROUTER addresses each answer per request.
impl OutgoingDestination for Answer {
    type Form = CallerName;
}

impl MessageHeaders for Answer {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::rpc::prelude::*;

    use super::{Greet, Greeting, ReplyToRequester, app};

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
                b.include(subscriber("greeter", Greet).reply().to("reply").build())
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
