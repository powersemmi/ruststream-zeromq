//! Request and reply over DEALER/ROUTER, as one self-contained service.
//!
//! The responder is an ordinary reply handler; a publish transform rewrites the reply
//! destination to the `reply-to` address the ROUTER stamped on the request, so the answer
//! goes back to the peer that asked. The requester runs from the scope's `after_startup`
//! hook, against the ephemeral endpoint this same process bound.
//!
//! ```text
//! cargo run --example zmq_request_reply -- run
//! ```

use std::io;
use std::time::Duration;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{
    App, AppInfo, Outgoing, PublishContext, PublishTransform, RustStream, TypedPublisher,
};
use ruststream::{IncomingMessage, OutgoingMessage, RequestReply, subscriber};
use ruststream_zeromq::{ZmqEndpoint, ZmqRpc, ZmqRpcPublish};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Greeting {
    who: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Reply {
    text: String,
}

/// Routes a reply back to the peer that asked: the responder's ROUTER addresses each request
/// with a `reply-to` header, and the requester matches answers by `correlation-id`, so both
/// travel from the delivery the handler is answering.
struct ReplyToRequester;

// --8<-- [start:transform]
impl<C> PublishTransform<C> for ReplyToRequester {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        if let Some(reply_to) = cx.headers().reply_to() {
            out.set_name(reply_to.to_owned());
        }
        if let Some(correlation) = cx.headers().correlation_id() {
            out.headers_mut()
                .insert("correlation-id", correlation.to_owned());
        }
    }
}

// The literal destination is a placeholder: `ReplyToRequester` replaces it per delivery.
#[subscriber("greeter", publish("reply"))]
async fn greet(request: &Greeting) -> Reply {
    Reply {
        text: format!("hello {}", request.who),
    }
}
// --8<-- [end:transform]

#[ruststream::app]
fn app() -> impl App {
    // Port 0 binds an ephemeral port; the requester below dials whatever it resolved to.
    RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            // --8<-- [start:responder]
            b.include(greet)
                .publisher(TypedPublisher::new(ZmqRpcPublish).transform(ReplyToRequester));
            // --8<-- [end:responder]

            // --8<-- [start:request]
            b.after_startup(ZmqRpcPublish, async move |publisher| -> io::Result<()> {
                let request = JsonCodec
                    .encode(&Greeting {
                        who: "world".to_owned(),
                    })
                    .map_err(io::Error::other)?;
                let answer = publisher
                    .request(
                        OutgoingMessage::new("greeter", request.as_ref()),
                        Duration::from_secs(5),
                    )
                    .await
                    .map_err(io::Error::other)?;
                let answer: Reply = JsonCodec
                    .decode(answer.payload())
                    .map_err(io::Error::other)?;
                println!("reply: {}", answer.text);
                Ok(())
            });
            // --8<-- [end:request]
        },
    )
}
