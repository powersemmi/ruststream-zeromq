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
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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
/// Reads `subscriber` on a task of its own, answering when the first delivery arrived.
fn read_on<S>(mut subscriber: S) -> (JoinHandle<()>, oneshot::Receiver<()>)
where
    S: Subscriber + Send + 'static,
{
    let (first_tx, first) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut stream = pin!(subscriber.stream());
        let mut first_tx = Some(first_tx);
        while let Some(delivery) = stream.next().await {
            delivery.expect("delivery is ok");
            if let Some(first_tx) = first_tx.take() {
                let _ = first_tx.send(());
            }
        }
    });
    (reader, first)
}

/// Publishes until a send does not complete within [`STALL`], then starts reading `subscriber` and
/// asserts that very send completes once the reader makes room. Answers how many sends completed
/// before the stall.
///
/// The stalled send is kept, not dropped: the claim is that the publisher is held back and then
/// let through, and a send given up on midway proves nothing about that.
async fn stalls_then_resumes<P, S>(publisher: &P, name: &str, subscriber: S) -> usize
where
    P: Publisher<Options = ()>,
    P::Error: std::fmt::Debug,
    S: Subscriber + Send + 'static,
{
    let body = vec![0_u8; BODY];
    for sent in 0..CEILING {
        let mut send = pin!(publisher.publish(OutgoingMessage::new(name, body.as_slice()), None));
        if let Ok(outcome) = tokio::time::timeout(STALL, &mut send).await {
            outcome.expect("publish succeeds");
            continue;
        }
        let (reader, first) = read_on(subscriber);
        tokio::time::timeout(RECV_TIMEOUT, first)
            .await
            .expect("a held delivery arrives once the subscription is read")
            .expect("the reader is running");
        tokio::time::timeout(RECV_TIMEOUT, send)
            .await
            .expect("the held send completes once the subscription makes room")
            .expect("publish succeeds");
        reader.abort();
        return sent;
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
    let subscriber = connected
        .subscribe("jobs")
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();

    let accepted = stalls_then_resumes(&publisher, "jobs", subscriber).await;
    assert!(
        accepted < CEILING,
        "the publisher ran {accepted} messages ahead of a subscription nobody read"
    );

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
    let subscriber = connected
        .subscribe("events")
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();

    // Sends before the subscription reached the publisher's filter table are dropped (the slow
    // joiner); they complete at once and only raise the count.
    let accepted = stalls_then_resumes(&publisher, "events", subscriber).await;
    assert!(
        accepted < CEILING,
        "the publisher ran {accepted} messages ahead of a subscription nobody read"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
