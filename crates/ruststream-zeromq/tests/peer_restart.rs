//! A publisher that dials its peer, after that peer restarts on the same address.
//!
//! The client reconnects only a SUB socket on its own, and a publisher keeps the socket it
//! attached. A publisher that lost its peer therefore drops that socket and dials again on its
//! next publish, so it reaches the restarted peer. The subject is the transport, so these drive the
//! connected forms over the loopback against a raw peer that is stopped and started again.

use std::net::TcpListener;
use std::time::Duration;

use bytes::Bytes;
use ruststream::{Broker, BytesMut, OutgoingMessage, Publisher};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue};
use zeromq::prelude::*;

/// Long enough that a slow machine does not fail a message that is already on its way.
const WAIT: Duration = Duration::from_secs(10);

/// How many publishes a test makes before it gives up on the restarted peer.
const ATTEMPTS: u32 = 20;

/// A loopback address with a port the operating system just handed out and took back, so the
/// peer can be started again on the same address.
fn fixed_port() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free loopback port");
    let port = listener.local_addr().expect("the port").port();
    format!("tcp://127.0.0.1:{port}")
}

fn message(name: &'static str, payload: &'static [u8]) -> OutgoingMessage<'static, BytesMut> {
    OutgoingMessage::produced(name, BytesMut::from(payload))
}

fn payload(frames: &zeromq::ZmqMessage) -> Option<Bytes> {
    frames.get(2).cloned()
}

/// A queue publisher that dials a sink keeps working after the sink restarts. The first send
/// into the dead connection is lost inside TCP and the next one fails, as on any socket; the
/// publish after that dials the new sink and reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialing_queue_publisher_reaches_a_restarted_sink() {
    let address = fixed_port();
    let mut sink = zeromq::PullSocket::new();
    sink.bind(&address).await.expect("the sink binds");
    let connected = ZmqQueue::new(ZmqEndpoint::connect(address.clone()))
        .connect()
        .await
        .expect("the queue connects");
    let publisher = connected.publisher();
    publisher
        .publish(message("jobs", b"before"), None)
        .await
        .expect("the sink takes the first job");
    let first = tokio::time::timeout(WAIT, sink.recv())
        .await
        .expect("the first job arrives")
        .expect("the sink reads it");
    assert_eq!(payload(&first).as_deref(), Some(&b"before"[..]));

    // Closed rather than dropped: a drop stops the listener in the background, and binding the
    // address again before that is refused.
    let errors = sink.close().await;
    assert!(errors.is_empty(), "the sink closes: {errors:?}");
    let mut sink = zeromq::PullSocket::new();
    sink.bind(&address).await.expect("the sink binds again");

    // Publishes into the connection the old sink left behind, until the socket reports it dead.
    let mut failed = false;
    for _ in 0..ATTEMPTS {
        if publisher
            .publish(message("jobs", b"lost"), None)
            .await
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(
        failed,
        "a send into a dead connection fails within {ATTEMPTS} attempts"
    );

    publisher
        .publish(message("jobs", b"after"), None)
        .await
        .expect("the publisher dials the restarted sink");
    let after = tokio::time::timeout(WAIT, sink.recv())
        .await
        .expect("the job reaches the restarted sink")
        .expect("the sink reads it");
    assert_eq!(payload(&after).as_deref(), Some(&b"after"[..]));
}

/// Publishes `name` until `subscriber` receives one: a PUB socket drops a message until the
/// filter of a peer that has just connected arrives, as the pattern drops it, so the first
/// publishes to a fresh connection may reach nobody.
async fn publish_until_received(
    publisher: &impl Publisher<Payload = ruststream::Take, Options = ()>,
    subscriber: &mut zeromq::SubSocket,
    payload_sent: &'static [u8],
) -> Option<Bytes> {
    for _ in 0..ATTEMPTS {
        let _ = publisher
            .publish(message("orders", payload_sent), None)
            .await;
        if let Ok(received) = tokio::time::timeout(WAIT / ATTEMPTS, subscriber.recv()).await {
            return payload(&received.expect("the subscriber reads it"));
        }
    }
    None
}

/// A fan-out publisher that dials a subscriber keeps reaching it after the subscriber restarts:
/// the socket reports the peer gone, the publisher drops it, and the next publish dials again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialing_fanout_publisher_reaches_a_restarted_subscriber() {
    let address = fixed_port();
    let mut subscriber = zeromq::SubSocket::new();
    subscriber
        .bind(&address)
        .await
        .expect("the subscriber binds");
    subscriber
        .subscribe("orders")
        .await
        .expect("the subscriber subscribes");
    let connected = ZmqFanout::new(ZmqEndpoint::connect(address.clone()))
        .connect()
        .await
        .expect("the fan-out connects");
    let publisher = connected.publisher();
    assert_eq!(
        publish_until_received(&publisher, &mut subscriber, b"before")
            .await
            .as_deref(),
        Some(&b"before"[..]),
    );

    // Closed rather than dropped: a drop stops the listener in the background, and binding the
    // address again before that is refused.
    let errors = subscriber.close().await;
    assert!(errors.is_empty(), "the subscriber closes: {errors:?}");
    let mut subscriber = zeromq::SubSocket::new();
    subscriber
        .bind(&address)
        .await
        .expect("the subscriber binds again");
    subscriber
        .subscribe("orders")
        .await
        .expect("the subscriber subscribes again");

    assert_eq!(
        publish_until_received(&publisher, &mut subscriber, b"after")
            .await
            .as_deref(),
        Some(&b"after"[..]),
        "the publisher dials the restarted subscriber",
    );
}
