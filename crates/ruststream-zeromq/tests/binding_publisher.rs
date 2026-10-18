//! A publisher whose side binds the endpoint, with no subscription of this service holding it.
//!
//! The runtime pairs a publisher when the service starts, and a publisher on the bind side
//! listens from that moment: a peer that dials the endpoint connects before anything is published,
//! and the first publish reaches it. The subject is the transport, so these pair the publish
//! policies against the connected forms over the loopback and dial them with a raw peer.

use std::net::TcpListener;
use std::time::Duration;

use bytes::Bytes;
use ruststream::{Broker, BytesMut, OutgoingMessage, PublishPolicy, Publisher, Take};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish};
use zeromq::prelude::*;

/// Long enough that a slow machine does not fail a message that is already on its way.
const WAIT: Duration = Duration::from_secs(10);

/// How long a peer is given to connect: the publisher listens already, so the handshake is all
/// it waits for.
const CONNECT: Duration = Duration::from_secs(5);

/// How many publishes the fan-out test makes before it gives up on the subscriber.
const ATTEMPTS: u32 = 20;

/// A loopback address with a port the operating system just handed out and took back.
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

/// A PULL peer dials a binding queue publisher that has published nothing yet, connects, and
/// receives the first job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_peer_connects_to_a_binding_queue_publisher_before_its_first_publish() {
    let address = fixed_port();
    let connected = ZmqQueue::new(ZmqEndpoint::bind(address.clone()))
        .connect()
        .await
        .expect("the queue connects");
    let publisher = ZmqQueuePublish
        .pair(&connected)
        .await
        .expect("the publisher pairs");

    let mut worker = zeromq::PullSocket::new();
    tokio::time::timeout(CONNECT, worker.connect(&address))
        .await
        .expect("the publisher listens before its first publish")
        .expect("the worker connects");

    publisher
        .publish(message("jobs", b"first"), None)
        .await
        .expect("the worker takes the first job");
    let first = tokio::time::timeout(WAIT, worker.recv())
        .await
        .expect("the first job arrives")
        .expect("the worker reads it");
    assert_eq!(payload(&first).as_deref(), Some(&b"first"[..]));
}

/// Publishes until `subscriber` receives one: a PUB socket drops a message until the filter of a
/// peer that has just connected arrives, as the pattern drops it.
async fn publish_until_received(
    publisher: &impl Publisher<Payload = Take, Options = ()>,
    subscriber: &mut zeromq::SubSocket,
) -> Option<Bytes> {
    for _ in 0..ATTEMPTS {
        publisher
            .publish(message("orders", b"announced"), None)
            .await
            .expect("a fan-out publish leaves");
        if let Ok(received) = tokio::time::timeout(WAIT / ATTEMPTS, subscriber.recv()).await {
            return payload(&received.expect("the subscriber reads it"));
        }
    }
    None
}

/// A SUB peer dials a binding fan-out publisher that has published nothing yet, connects, and
/// receives what is announced once its filter has arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sub_peer_connects_to_a_binding_fanout_publisher_before_its_first_publish() {
    let address = fixed_port();
    let connected = ZmqFanout::new(ZmqEndpoint::bind(address.clone()))
        .connect()
        .await
        .expect("the fan-out connects");
    let publisher = ZmqFanoutPublish
        .pair(&connected)
        .await
        .expect("the publisher pairs");

    let mut subscriber = zeromq::SubSocket::new();
    tokio::time::timeout(CONNECT, subscriber.connect(&address))
        .await
        .expect("the publisher listens before its first publish")
        .expect("the subscriber connects");
    subscriber
        .subscribe("orders")
        .await
        .expect("the subscriber subscribes");

    assert_eq!(
        publish_until_received(&publisher, &mut subscriber)
            .await
            .as_deref(),
        Some(&b"announced"[..]),
    );
}
