//! Request and reply over DEALER/ROUTER, as one self-contained service.
//!
//! A handler on a queue asks the greeter and waits for its answer. The greeter is an ordinary
//! reply handler on a ROUTER this same service binds; a publish transform sends each answer to
//! the `reply-to` address the ROUTER stamped on the request, so it goes back to the peer that
//! asked.
//!
//! ```text
//! cargo run --example zmq_request_reply --features testing -- run
//! cargo test --example zmq_request_reply --features testing
//! ```

use std::time::Duration;

use ruststream::OutgoingMessage;
// The prelude's `Outgoing` is the derive; the publish pipeline's message of the same name is the
// type a transform rewrites.
use ruststream::runtime::{Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

use ruststream_zeromq::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "names")]
struct Name {
    who: String,
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

// --8<-- [start:request]
#[subscriber("names")]
async fn ask(name: &Name, Out(greeter): Out<impl RequestReply>) -> HandlerOutcome {
    let Ok(greeting) = serde_json::to_vec(&Greeting {
        who: name.who.clone(),
    }) else {
        return HandlerOutcome::drop();
    };
    let request = OutgoingMessage::new("greeter", greeting.as_slice());
    let Ok(answer) = greeter.request(request, Duration::from_secs(5)).await else {
        return HandlerOutcome::retry_after(Duration::from_secs(1));
    };
    let Ok(answer) = serde_json::from_slice::<Answer>(answer.payload()) else {
        return HandlerOutcome::drop();
    };
    tracing::info!(answer = %answer.text, "greeted");
    HandlerOutcome::ack()
}
// --8<-- [end:request]

// --8<-- [start:responder]
// The answer goes to the peer that asked, so the clause's name is only the fallback.
#[subscriber("greeter", reply("reply"))]
async fn greet(request: &Greeting) -> Answer {
    tracing::info!(who = %request.who, "greeting");
    Answer {
        text: format!("hello {}", request.who),
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
#[ruststream::app]
fn app() -> impl App {
    let rpc = ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")).bindable();
    let to_greeter = rpc.bind(ZmqRpcPublish);
    RustStream::new(AppInfo::new("greeter", "0.1.0"))
        .with_broker(rpc, |b| {
            // A responder addresses no retry copies, so the mount names where they go.
            b.include(greet)
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        })
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(ask).out(DefaultSlot, to_greeter).build();
            },
        )
}
// --8<-- [end:app]

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
