//! What a publish that is dropped mid-send leaves behind, over real sockets on the loopback.
//!
//! A send waits while the peer applies back-pressure, and a caller may give up on it (a
//! `select!` arm that loses, a timeout). The publisher has to stay usable afterwards: the next
//! publish reaches the peer once the peer reads again, after the message whose publish was
//! dropped.

use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::poll;
use ruststream::{Broker, BytesMut, OutgoingMessage, Publisher};
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue};
use tokio::sync::mpsc;
use zeromq::PullSocket;
use zeromq::prelude::*;

/// A guard against a hang, never a synchronization point: every wait below ends on an event.
const GUARD: Duration = Duration::from_secs(30);

/// Large enough that a few sends fill the loopback's buffers while the peer does not read.
const BULK: usize = 1 << 20;

/// How many bulk sends may complete before the peer's back-pressure must have stopped one.
const FILL_LIMIT: usize = 4096;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_dropped_under_back_pressure_leaves_the_publisher_usable() {
    // The peer is a PULL socket of its own, so the test decides when it reads.
    let mut peer = PullSocket::new();
    let address = peer
        .bind("tcp://127.0.0.1:0")
        .await
        .expect("the peer binds")
        .to_string();

    let connected = ZmqQueue::new(ZmqEndpoint::connect(address))
        .connect()
        .await
        .expect("the queue connects");
    let publisher = connected.publisher();

    // One message through proves the handshake is done, so a pending send below is the peer's
    // back-pressure and nothing else.
    publisher
        .publish(
            OutgoingMessage::produced("jobs", BytesMut::from(&b"first"[..])),
            None,
        )
        .await
        .expect("the first publish leaves");
    let first = tokio::time::timeout(GUARD, peer.recv())
        .await
        .expect("the first message arrives")
        .expect("the peer receives");
    assert_eq!(first.get(2).map(Bytes::as_ref), Some(&b"first"[..]));

    // The peer stops reading. Publish until a send cannot complete, then drop it mid-send.
    let bulk = vec![0_u8; BULK];
    let mut stalled = false;
    let mut attempted = 0;
    for _ in 0..FILL_LIMIT {
        attempted += 1;
        let publish = publisher.publish(
            OutgoingMessage::produced("jobs", BytesMut::from(bulk.as_slice())),
            None,
        );
        match poll!(pin!(publish)) {
            Poll::Ready(result) => result.expect("a bulk publish leaves"),
            Poll::Pending => {
                stalled = true;
                break;
            }
        }
    }
    assert!(stalled, "the peer never applied back-pressure");

    // The peer reads again, and the publisher is asked for one more message.
    let (tx, mut received) = mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        while let Ok(message) = peer.recv().await {
            if tx.send(message.get(2).cloned()).is_err() {
                break;
            }
        }
    });
    tokio::time::timeout(
        GUARD,
        publisher.publish(
            OutgoingMessage::produced("jobs", BytesMut::from(&b"last"[..])),
            None,
        ),
    )
    .await
    .expect("the publish after a dropped one does not hang")
    .expect("the publish after a dropped one leaves");

    // Every bulk message arrives, the dropped one included, and then the last one.
    let bulk_before_last = tokio::time::timeout(GUARD, async {
        let mut bulk = 0;
        while let Some(payload) = received.recv().await {
            if payload.as_deref() == Some(&b"last"[..]) {
                return Some(bulk);
            }
            bulk += 1;
        }
        None
    })
    .await
    .expect("the peer drains what was sent");
    assert_eq!(
        bulk_before_last,
        Some(attempted),
        "the message published after the dropped one reaches the peer, after every bulk message",
    );
    reader.abort();
}
