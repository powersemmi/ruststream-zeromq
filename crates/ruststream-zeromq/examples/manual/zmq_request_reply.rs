//! The service of `examples/zmq_request_reply.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_zmq_request_reply --features testing -- run
//! cargo test --example manual_zmq_request_reply --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Name {
    who: String,
}

// `#[outgoing(name = "names")]` by hand: the type names the subscription it is pushed to.
impl OutgoingDestination for Name {
    type Form = FixedName;
    const DESTINATION: &'static str = "names";
}

impl MessageHeaders for Name {
    type Contract = NoHeaders;
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

// --8<-- [start:request]
struct Ask;

impl<Entry> Handle<Name, (), Outs<(Entry,)>> for Ask
where
    Entry: OutEntry<DefaultSlot, Wire: RequestReply>,
{
    async fn handle(
        &self,
        name: &Name,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let greeting = serde_json::to_vec(&Greeting {
            who: name.who.clone(),
        })
        .map_err(|_| HandlerOutcome::drop())?;
        let request = OutgoingMessage::new("greeter", greeting.as_slice());
        let answer = outs
            .get(DefaultSlot)
            .request(request, Duration::from_secs(5))
            .await
            .map_err(|_| HandlerOutcome::retry_after(Duration::from_secs(1)))?;
        let answer = serde_json::from_slice::<Answer>(answer.payload())
            .map_err(|_| HandlerOutcome::drop())?;
        tracing::info!(answer = %answer.text, "greeted");
        Ok(())
    }
}
// --8<-- [end:request]

// --8<-- [start:responder]
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

/// Answers at the `reply-to` address the ROUTER stamped on the request, under the request's
/// `correlation-id`.
struct ReplyToRequester;

// The transform reads the delivery being answered, so it names `ForReply`, and it names the
// destination per delivery, so it declares `Names`. Both headers are taken with `get_shared`,
// which hands them over as the buffer the request arrived in.
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
// --8<-- [end:responder]

// --8<-- [start:app]
fn app() -> impl App {
    let rpc = ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")).bindable();
    let to_greeter = rpc.bind(ZmqRpcPublish);
    RustStream::new(AppInfo::new("greeter", "0.1.0"))
        .with_broker(rpc, |b| {
            // The answer goes to the peer that asked, so the `to` name is only the fallback.
            // A responder addresses no retry copies, so the mount names where they go.
            b.include(subscriber("greeter", Greet).reply().to("reply").build())
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        })
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("names", Ask).build())
                    .out(DefaultSlot, to_greeter)
                    .build();
            },
        )
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

    use super::{Greeting, Name, app};

    #[tokio::test]
    async fn a_name_is_greeted_by_the_responder() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Name {
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
        // The asker acknowledges only once the answer came back under its correlation id.
        tb.broker::<ZmqQueue>()
            .subscriber("names")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
