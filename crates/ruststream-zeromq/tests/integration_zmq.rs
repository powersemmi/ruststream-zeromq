//! End-to-end checks over real sockets on the loopback, including the wire-layout contract a
//! non-Rust peer relies on.

use std::io;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream, SubscriberSettings};
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage,
    Publisher, RequestReply, Serialized, Subscribe, Subscriber, nonzero, subscriber,
};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqQueuePublish, ZmqRpc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
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
        .publish(
            OutgoingMessage::new("orders", b"{\"id\":1}".as_slice()).with_headers(headers),
            None,
        )
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
            .publish(
                OutgoingMessage::new("orders.us.1", b"skipped".as_slice()),
                None,
            )
            .await
            .expect("publish succeeds");
        publisher
            .publish(
                OutgoingMessage::new("orders.eu.1", b"kept".as_slice()),
                None,
            )
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

#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "batches")]
struct Job {
    id: usize,
}

/// Reports the length of every batch it is handed, so the test reads the shape back without
/// waiting on a clock.
static BATCHES: OnceLock<mpsc::UnboundedSender<usize>> = OnceLock::new();

#[subscriber("batches")]
async fn record_batches(jobs: &[Job]) -> HandlerOutcome {
    BATCHES
        .get()
        .expect("the test installs the sender before the app starts")
        .send(jobs.len())
        .expect("the test holds the receiver");
    HandlerOutcome::ack()
}

/// A socket hands over one multipart message per receive, so the batches a `&[T]` body sees are
/// assembled on the client - and the size the mount site named is what caps them. This runs the
/// whole path a service writes: a batch mount, real sockets, and a publisher dialing the port the
/// subscription bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_mount_caps_the_batches_a_body_sees() {
    const BATCH: usize = 3;
    const COUNT: usize = 7;

    let (tx, mut rx) = mpsc::unbounded_channel();
    BATCHES.set(tx).expect("one batch mount per test binary");

    let app = RustStream::new(AppInfo::new("batcher", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(record_batches.batch(nonzero!(3)));
            // `start()` resolves only after subscriptions are open, and this hook runs there, so
            // the publisher has a bound address to dial and nothing is published into the void.
            b.after_startup(ZmqQueuePublish, async move |publisher| -> io::Result<()> {
                for id in 0..COUNT {
                    publisher
                        .message(&Job { id })
                        .publish()
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            });
        },
    );
    let running = app.start().await.expect("startup succeeds");

    let mut seen = 0;
    while seen < COUNT {
        let batch = tokio::time::timeout(RECV_TIMEOUT, rx.recv())
            .await
            .expect("a batch arrives")
            .expect("the app holds the sender");
        assert!(
            batch <= BATCH,
            "a batch must never carry more than the size the mount named: got {batch}",
        );
        seen += batch;
    }

    running.shutdown().await.expect("shutdown succeeds");
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

/// A unique `ipc://` endpoint for one test, so a run leaves nothing behind and two runs do not
/// collide. `ipc` is the second transport the crate supports and the one that needs no port.
fn ipc_endpoint(label: &str) -> (String, PathBuf) {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "ruststream-zmq-{label}-{}-{}.sock",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    ));
    (format!("ipc://{}", path.display()), path)
}

/// Competing consumers is the queue's headline, and it is a property of the sockets rather than
/// of this crate: one producer binds, every consumer dials it, and the stream is split rather
/// than copied. The run also covers `ipc://`, the transport a same-host deployment uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_consumers_split_one_stream_and_never_duplicate_it() {
    const JOBS: usize = 6;

    let (address, path) = ipc_endpoint("competing");
    let producer = ZmqQueue::new(ZmqEndpoint::bind(address.clone()))
        .connect()
        .await
        .expect("the producer connects");

    // Sockets attach lazily, so the producer takes the endpoint on its first publish. That job
    // waits inside the crate's own retry window until a consumer attaches, which is what lets
    // the consumers dial an endpoint that exists.
    let publisher = producer.publisher();
    let first = tokio::spawn({
        let publisher = publisher.clone();
        async move {
            publisher
                .publish(OutgoingMessage::new("jobs", b"0".as_slice()), None)
                .await
        }
    });
    // An ipc endpoint is a filesystem entry, so the bind is observable rather than guessed.
    tokio::time::timeout(RECV_TIMEOUT, async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the producer takes the endpoint");

    let consumers = ZmqQueue::new(ZmqEndpoint::connect(address));
    let left = consumers
        .clone()
        .connect()
        .await
        .expect("a consumer connects");
    let right = consumers.connect().await.expect("a consumer connects");
    let mut left = left.subscribe("jobs").await.expect("a subscription opens");
    let mut right = right.subscribe("jobs").await.expect("a subscription opens");

    first
        .await
        .expect("the publishing task runs")
        .expect("the first job reaches the consumer that attached");
    for id in 1..JOBS {
        publisher
            .publish(
                OutgoingMessage::new("jobs", id.to_string().as_bytes()),
                None,
            )
            .await
            .expect("the job is published");
    }

    let mut seen = Vec::new();
    let mut from_left = 0usize;
    while seen.len() < JOBS {
        let (delivery, side) = tokio::time::timeout(RECV_TIMEOUT, async {
            let mut left_stream = pin!(left.stream());
            let mut right_stream = pin!(right.stream());
            tokio::select! {
                next = left_stream.next() => (next, 1usize),
                next = right_stream.next() => (next, 0usize),
            }
        })
        .await
        .expect("a job arrives before the deadline");
        let delivery = delivery
            .expect("the stream is open")
            .expect("the delivery is well formed");
        from_left += side;
        seen.push(String::from_utf8(delivery.payload().to_vec()).expect("the payload is text"));
    }

    seen.sort_unstable();
    let expected: Vec<String> = (0..JOBS).map(|id| id.to_string()).collect();
    assert_eq!(seen, expected, "every job must arrive exactly once");
    assert!(
        from_left > 0 && from_left < JOBS,
        "the stream must be split rather than handed to one consumer: {from_left} of {JOBS} \
         went left",
    );

    producer.shutdown().await.expect("the producer shuts down");
    std::fs::remove_file(&path).expect("the endpoint file is this test's to remove");
}

/// The fan-out filters on the publisher side, so a name that does not match is never put on the
/// wire at all. The positive half above shows the match arriving; this is the half that matters
/// to a service picking prefixes, and only a socket can show it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_never_delivers_a_name_outside_the_prefix() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("fanout connects");
    let mut subscriber = connected
        .subscribe("orders.eu")
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    let mut matched = 0usize;
    for _ in 0..50 {
        for name in ["orders.us.1", "orders.eu.1", "ordersX"] {
            publisher
                .publish(OutgoingMessage::new(name, name.as_bytes()), None)
                .await
                .expect("publish succeeds");
        }
        let mut stream = pin!(subscriber.stream());
        while let Ok(Some(next)) =
            tokio::time::timeout(Duration::from_millis(100), stream.next()).await
        {
            let message = next.expect("delivery is ok");
            assert_eq!(
                message.name(),
                "orders.eu.1",
                "only a name the subscription is a prefix of may reach it",
            );
            matched += 1;
        }
        if matched > 0 {
            break;
        }
    }
    assert!(matched > 0, "the matching name must reach the subscription");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A broadcast nobody subscribed to is dropped by the protocol, not reported. A service that
/// treated a successful publish as proof of delivery would be wrong on this pattern, and this is
/// where that is written down against a real PUB socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_with_no_subscriber_is_dropped_without_an_error() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("fanout connects");

    connected
        .publisher()
        .publish(OutgoingMessage::new("events", b"nobody".as_slice()), None)
        .await
        .expect("a publish with no subscriber must succeed, because the protocol drops it");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The transports the crate serves are checked before any socket is opened, and `connect` is
/// where a service meets that check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsupported_transport_is_refused_at_connect() {
    let err = ZmqQueue::new(ZmqEndpoint::bind("inproc://orders"))
        .connect()
        .await
        .expect_err("inproc is not one of the transports this crate serves")
        .to_string();
    assert!(
        err.contains("inproc://orders") && err.contains("tcp://") && err.contains("ipc://"),
        "the refusal must name the address and the transports on offer, got: {err}",
    );
}

/// Two services asked to listen on one address is an operator's mistake, and the second one has
/// to say so rather than run without a subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binding_an_occupied_endpoint_is_refused() {
    let held = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the first queue connects");
    let _subscriber = held.subscribe("jobs").await.expect("the first bind wins");
    let address = held.bound_address().expect("the subscription bound a port");

    let clash = ZmqQueue::new(ZmqEndpoint::bind(address.clone()))
        .connect()
        .await
        .expect("recording an endpoint is not I/O, so this still succeeds");
    let err = clash
        .subscribe("jobs")
        .await
        .expect_err("the address is already listening")
        .to_string();
    assert!(
        err.contains(&address),
        "the refusal must name the endpoint it could not take, got: {err}",
    );

    clash.shutdown().await.expect("the second queue shuts down");
    held.shutdown().await.expect("the first queue shuts down");
}

/// A PUSH socket with nothing attached hands every message back, so the crate retries for five
/// seconds and then reports the destination it could not reach. Without that window a publish
/// issued while a peer is still shaking hands would fail for no reason; with it, a publish into
/// an empty topology still ends rather than hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_with_no_peer_fails_once_the_retry_window_is_spent() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the queue connects");

    let err = connected
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"{}".as_slice()), None)
        .await
        .expect_err("no consumer ever attaches, so the window runs out")
        .to_string();
    assert!(
        err.contains("jobs") && err.contains("no connected peer"),
        "the failure must name the destination and the reason, got: {err}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A peer composing the frames by hand gets the malformed ones back as an error on the
/// subscription, on the first message rather than as headers that quietly went missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_header_frame_from_a_foreign_peer_surfaces_on_the_subscription() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("queue connects");
    let mut subscriber = connected
        .subscribe("orders")
        .await
        .expect("subscription opens");
    let address = connected.bound_address().expect("subscription bound");

    let mut raw = zeromq::PushSocket::new();
    raw.connect(&address).await.expect("raw peer connects");

    let mut frames = zeromq::ZmqMessage::from("orders");
    frames.push_back(bytes::Bytes::from_static(b"content-type application/json"));
    frames.push_back(bytes::Bytes::from_static(b"{}"));
    // The handshake may still be settling, so hand the frames over the same way a publisher does.
    for _ in 0..50 {
        if raw.send(frames.clone()).await.is_ok() {
            break;
        }
    }

    let mut stream = pin!(subscriber.stream());
    let err = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the subscription answers")
        .expect("the stream is open")
        .expect_err("a header line with no separator is not a message")
        .to_string();
    assert!(
        err.contains("content-type application/json"),
        "the error must quote the line it could not read, got: {err}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The header frame is text in both directions. A value that has no text form is refused before
/// the socket is touched, and the error names the header and the destination it was meant for,
/// which is what an operator needs to find the publish site.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_value_that_is_not_text_is_refused_against_its_destination() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("queue connects");

    let mut headers = HeaderMap::new();
    headers.insert("x-signature", [0xff, 0xfe].as_slice());
    let err = connected
        .publisher()
        .publish(
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect_err("a binary value has no place in a text frame")
        .to_string();
    assert!(
        err.contains("x-signature") && err.contains("orders"),
        "the refusal must name the header and the destination, got: {err}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A publisher outliving its connection is the one dynamic part of the lifecycle ladder: the
/// consuming transitions make subscribing after shutdown a compile error, but a handle handed out
/// earlier is still a value someone can call. It has to say the connection is gone rather than
/// report a send nobody will ever receive.
///
/// The conformance suite holds the queue to this. The other two patterns are held to it here,
/// because their publishers reach the shared state by different routes - the fan-out through its
/// own cell, the responder through the ROUTER a subscription installs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_pattern_refuses_a_publisher_before_connect_and_after_shutdown() {
    let queue = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let fanout = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let rpc = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let early = [
        publish_error(&queue.publisher()).await,
        publish_error(&fanout.publisher()).await,
        publish_error(&rpc.publisher()).await,
    ];
    for err in early {
        assert!(
            err.contains("not connected"),
            "a publisher built before connect must say the transport is not connected, got: {err}",
        );
    }

    let queue = queue.connect().await.expect("the queue connects");
    let fanout = fanout.connect().await.expect("the fan-out connects");
    let rpc = rpc.connect().await.expect("the responder connects");
    let outliving = [queue.publisher(), queue.publisher()];
    let fanout_publisher = fanout.publisher();
    let rpc_publisher = rpc.publisher();
    queue.shutdown().await.expect("the queue shuts down");
    fanout.shutdown().await.expect("the fan-out shuts down");
    rpc.shutdown().await.expect("the responder shuts down");

    for publisher in &outliving {
        let err = publish_error(publisher).await;
        assert!(
            err.contains("not connected"),
            "a publisher outliving the connection must refuse, got: {err}",
        );
    }
    assert!(
        publish_error(&fanout_publisher)
            .await
            .contains("not connected")
    );
    assert!(
        publish_error(&rpc_publisher)
            .await
            .contains("not connected")
    );
    let asking = rpc_publisher
        .request(
            OutgoingMessage::new("greeter", b"{}".as_slice()),
            Duration::from_millis(100),
        )
        .await
        .expect_err("a request after shutdown has no connection to issue on")
        .to_string();
    assert!(
        asking.contains("not connected"),
        "a request after shutdown must refuse, got: {asking}",
    );
}

/// The failure a publish reports, as text, so the three patterns can be read the same way.
async fn publish_error<P: Publisher>(publisher: &P) -> String {
    publisher
        .publish(OutgoingMessage::new("jobs", b"{}".as_slice()), None)
        .await
        .expect_err("the transport is not connected")
        .to_string()
}
