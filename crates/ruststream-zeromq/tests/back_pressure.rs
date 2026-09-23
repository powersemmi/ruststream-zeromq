//! A subscription that is not read slows its sender down instead of buffering without bound.
//!
//! The subject is the transport itself, so these run over real sockets on the loopback: a
//! subscription is opened and left unpolled, and a publisher sends until a send stops completing.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, OutgoingMessage, Publisher, Subscribe, Subscriber, nonzero,
};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue};

/// Bodies large enough that the kernel's socket buffers hold only a handful of them, so what
/// stops the publisher is the subscription's bound and not megabytes of socket memory.
const BODY: usize = 64 * 1024;

/// Far more than the read-ahead plus what the socket buffers hold: a publisher that gets this
/// far unread is being buffered for without bound.
const CEILING: usize = 2_000;

/// How long one send may take before the publisher counts as held back.
const STALL: Duration = Duration::from_secs(2);

/// How long the drained subscription has to hand over its first delivery.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Publishes until a send does not complete within [`STALL`], and returns how many completed.
async fn publish_until_held<P>(publisher: &P, name: &str) -> usize
where
    P: Publisher<Options = ()>,
    P::Error: std::fmt::Debug,
{
    let body = vec![0_u8; BODY];
    for sent in 0..CEILING {
        let send = publisher.publish(OutgoingMessage::new(name, body.as_slice()), None);
        match tokio::time::timeout(STALL, send).await {
            Ok(outcome) => outcome.expect("publish succeeds"),
            Err(_) => return sent,
        }
    }
    CEILING
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unread_queue_subscription_holds_its_publisher_back() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .read_ahead(nonzero!(8_usize))
        .connect()
        .await
        .expect("queue connects");
    let mut subscriber = connected
        .subscribe("jobs")
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();

    let accepted = publish_until_held(&publisher, "jobs").await;
    assert!(
        accepted < CEILING,
        "the publisher ran {accepted} messages ahead of a subscription nobody read"
    );

    // The publisher was held back, not failed: reading the subscription lets deliveries through.
    let mut stream = pin!(subscriber.stream());
    tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("a held delivery arrives once the subscription is read")
        .expect("stream is open")
        .expect("delivery is ok");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The fan-out's sender waits for a subscriber that is not reading, the same as the queue's: the
/// `zeromq` implementation writes a PUB message to each matching peer in turn and waits for the
/// write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unread_fanout_subscription_holds_its_publisher_back() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .read_ahead(nonzero!(8_usize))
        .connect()
        .await
        .expect("fan-out connects");
    let mut subscriber = connected
        .subscribe("events")
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();

    // Sends before the subscription reached the publisher's filter table are dropped (the slow
    // joiner); they complete at once and only raise the count.
    let accepted = publish_until_held(&publisher, "events").await;
    assert!(
        accepted < CEILING,
        "the publisher ran {accepted} messages ahead of a subscription nobody read"
    );

    let mut stream = pin!(subscriber.stream());
    tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("a held delivery arrives once the subscription is read")
        .expect("stream is open")
        .expect("delivery is ok");

    connected.shutdown().await.expect("shutdown succeeds");
}
