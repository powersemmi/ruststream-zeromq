//! End-to-end checks over real sockets on the loopback, including the wire-layout contract a
//! non-Rust peer relies on.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage,
    Publisher, Serialized, Subscribe, Subscriber,
};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue};
use zeromq::prelude::*;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_roundtrip_preserves_payload_and_headers() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("queue connects");
    let mut subscriber = connected
        .subscribe("orders")
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new("orders", b"{\"id\":1}".as_slice()).with_headers(headers))
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    // At most once, no durability: acknowledgement is honestly unsupported.
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Payloads a foreign peer already framed are the crate's common case, and they travel through
/// the framework's typed publish builder as a `Serialized` newtype. This proves the documented
/// path end to end over a socket: no codec runs, and the bytes reach the wire untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serialized_bytes_reach_the_wire_untouched() {
    #[derive(Outgoing, Serialized)]
    #[outgoing(name = "orders")]
    struct Framed(Vec<u8>);

    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("queue connects");
    let mut subscriber = connected
        .subscribe("orders")
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .message(&Framed(b"\x00\x01not-json".to_vec()))
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.name(), "orders");
    assert_eq!(message.payload(), b"\x00\x01not-json");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fanout_filters_by_name_prefix() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("fanout connects");
    let mut subscriber = connected
        .subscribe("orders.eu")
        .await
        .expect("subscription opens");

    // The slow joiner is real and honest scope: the publisher-side filter table fills only
    // after the handshake, so publish until the first delivery lands, then assert filtering.
    let publisher = connected.publisher();
    let mut stream = pin!(subscriber.stream());
    let mut delivered = None;
    for _ in 0..50 {
        publisher
            .publish(OutgoingMessage::new("orders.us.1", b"skipped".as_slice()))
            .await
            .expect("publish succeeds");
        publisher
            .publish(OutgoingMessage::new("orders.eu.1", b"kept".as_slice()))
            .await
            .expect("publish succeeds");
        if let Ok(Some(next)) =
            tokio::time::timeout(Duration::from_millis(200), stream.next()).await
        {
            delivered = Some(next.expect("delivery is ok"));
            break;
        }
    }
    let message = delivered.expect("a matching delivery arrives");
    assert_eq!(message.payload(), b"kept");
    assert_eq!(message.name(), "orders.eu.1");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The documented three-frame layout is the contract a Python or C++ peer composes by hand;
/// this test plays that peer with a raw socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_layout_is_stable_for_foreign_peers() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("queue connects");
    let mut subscriber = connected
        .subscribe("orders")
        .await
        .expect("subscription opens");
    let address = connected.bound_address().expect("subscription bound");

    // The foreign peer: a raw PUSH socket composing the three documented frames by hand,
    // exactly as the docs tell a Python peer to.
    let mut raw = zeromq::PushSocket::new();
    raw.connect(&address).await.expect("raw peer connects");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut frames = zeromq::ZmqMessage::from("orders");
    frames.push_back(bytes::Bytes::from_static(b"content-type: application/json"));
    frames.push_back(bytes::Bytes::from_static(b"{\"id\":2}"));
    raw.send(frames).await.expect("raw send succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.name(), "orders");
    assert_eq!(message.payload(), b"{\"id\":2}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
