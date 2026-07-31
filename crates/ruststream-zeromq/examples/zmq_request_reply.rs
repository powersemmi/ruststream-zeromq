//! Request and reply over DEALER/ROUTER: the responder subscribes, replies route back
//! through the `reply-to` header; the requester uses the `RequestReply` capability.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, RequestReply, Subscribe,
    Subscriber,
};
use ruststream_zeromq::{ZmqEndpoint, ZmqRpc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connected = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await?;

    // The responder half: answer one request.
    let mut subscriber = connected.subscribe("greeter").await?;
    let responder = connected.publisher();
    tokio::spawn(async move {
        let mut stream = pin!(subscriber.stream());
        if let Some(Ok(request)) = stream.next().await {
            let reply_to = request.headers().reply_to().unwrap_or_default().to_owned();
            let mut headers = ruststream::Headers::new();
            if let Some(correlation) = request.headers().correlation_id() {
                headers.insert("correlation-id", correlation.to_owned());
            }
            let _ = responder
                .publish(
                    OutgoingMessage::new(&reply_to, b"hello back".as_slice()).with_headers(headers),
                )
                .await;
        }
    });

    // The requester half.
    let requester = connected.publisher();
    let reply = requester
        .request(
            OutgoingMessage::new("greeter", b"hello".as_slice()),
            Duration::from_secs(5),
        )
        .await?;
    println!("reply: {}", String::from_utf8_lossy(reply.payload()));

    connected.shutdown().await?;
    Ok(())
}
