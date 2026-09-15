//! The stand-in settles the way the transport settles: not at all.
//!
//! `ZeroMQ` has no protocol frame to settle a delivery with, so a handler that asks for a retry
//! gets none. The test that matters is the one that used to pass for the wrong reason: a retrying
//! handler must be called once here, exactly as it is called once over a socket.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::{TestApp, TestableBroker};
use ruststream::{
    AckError, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscribe, Subscriber,
};
use ruststream_zeromq::testing::{Queue, ZmqTestBroker};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqRpc};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn always_retry(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::retry()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retrying_handler_is_not_called_again() {
    let app = RustStream::new(AppInfo::new("zmq-settlement", "0.0.0")).with_broker(
        ZmqTestBroker::queue(),
        |b| {
            b.include(always_retry);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Job { id: 1 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    // The transport cannot redeliver, so the retry the handler asked for never happens. A
    // stand-in that requeued would report two calls and promise a guarantee production lacks.
    tb.broker::<ZmqTestBroker<Queue>>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 1 });
}

/// The settlement answer itself is the subject here, so this one reads it off the delivery
/// rather than through the harness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_refuses_to_settle() {
    let connected = ZmqTestBroker::queue().connect().await.expect("connects");
    let mut subscriber = connected.subscribe("jobs").await.expect("subscribes");
    connected.inject(OutgoingMessage::new("jobs", b"{\"id\":1}".as_slice()));

    let mut stream = pin!(subscriber.stream());
    let message = stream
        .next()
        .await
        .expect("a delivery arrives")
        .expect("the delivery is ok");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    let mut subscriber = connected.subscribe("jobs").await.expect("subscribes");
    connected.inject(OutgoingMessage::new("jobs", b"{\"id\":2}".as_slice()));
    let mut stream = pin!(subscriber.stream());
    let message = stream
        .next()
        .await
        .expect("a delivery arrives")
        .expect("the delivery is ok");
    assert!(matches!(
        message.nack(true).await,
        Err(AckError::Unsupported)
    ));
}

// -- The same answer from the sockets ----------------------------------------------------------
//
// The stand above mirrors the handler's behaviour. What it cannot mirror is the transport: a
// delivery over ZMTP has no frame to settle with, and the proof of that is a socket that neither
// accepts an acknowledgement nor sends the delivery again.

/// Long enough for a handshake on a loaded machine.
const LIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a subscription has to stay silent before "it never comes back" is established.
const SILENCE: Duration = Duration::from_millis(500);

async fn next_delivery<S: Subscriber>(subscriber: &mut S, why: &str) -> S::Message {
    let mut stream = pin!(subscriber.stream());
    timeout(LIVE_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{why}: nothing arrived before the deadline"))
        .expect("the stream is open")
        .expect("the delivery is well formed")
}

async fn expect_silence<S: Subscriber>(subscriber: &mut S, why: &str) {
    let mut stream = pin!(subscriber.stream());
    assert!(
        timeout(SILENCE, stream.next()).await.is_err(),
        "{why}: something still arrived",
    );
}

/// A rejection with a requeue is the one settlement that would bring a delivery back, and this
/// transport has neither. The refusal is the whole guarantee: a service reading it knows the
/// message is gone, and a service that ignored it would wait forever for a redelivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_delivery_refuses_every_settlement_and_never_returns() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the queue connects");
    let mut subscriber = connected
        .subscribe("jobs")
        .await
        .expect("the subscription opens");
    let publisher = connected.publisher();

    for (index, settle) in ["ack", "nack-requeue", "nack-drop"].iter().enumerate() {
        publisher
            .publish(
                OutgoingMessage::new("jobs", index.to_string().as_bytes()),
                None,
            )
            .await
            .expect("the job is published");
        let message = next_delivery(&mut subscriber, settle).await;
        // ZMTP carries no delivery counter, so a handler reading one gets nothing rather than a
        // number the transport made up; the framework's own retry header is what counts copies.
        assert_eq!(message.redelivery_count(), None);
        let outcome = match *settle {
            "ack" => message.ack().await,
            "nack-requeue" => message.nack(true).await,
            _ => message.nack(false).await,
        };
        assert!(
            matches!(outcome, Err(AckError::Unsupported)),
            "{settle} must report itself unsupported, got: {outcome:?}",
        );
    }
    expect_silence(&mut subscriber, "a requeue the transport cannot perform").await;

    connected.shutdown().await.expect("the queue shuts down");
}

/// The fan-out settles no more than the queue does, and a rejected broadcast is not re-sent to
/// anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_delivery_refuses_every_settlement() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the fan-out connects");
    let mut subscriber = connected
        .subscribe("events")
        .await
        .expect("the subscription opens");
    let publisher = connected.publisher();

    // The publisher-side filter table fills only after the handshake, so publish until the first
    // delivery lands - the slow joiner is the pattern's contract, not a fault.
    let mut delivered = None;
    for _ in 0..50 {
        publisher
            .publish(OutgoingMessage::new("events", b"one".as_slice()), None)
            .await
            .expect("the event is published");
        let mut stream = pin!(subscriber.stream());
        if let Ok(Some(next)) = timeout(Duration::from_millis(200), stream.next()).await {
            delivered = Some(next.expect("the delivery is well formed"));
            break;
        }
    }
    let message = delivered.expect("a matching delivery arrives");
    assert!(matches!(
        message.nack(true).await,
        Err(AckError::Unsupported)
    ));

    connected.shutdown().await.expect("the fan-out shuts down");
}

/// A request is a delivery like any other: answering it is a publish, and there is no settlement
/// frame behind it either. A responder that treated `nack` as a way to decline would decline into
/// the void and leave the requester waiting out its timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_delivery_refuses_every_settlement() {
    let connected = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the responder connects");
    let mut subscriber = connected
        .subscribe("greeter")
        .await
        .expect("the responder subscription opens");
    let requester = connected.publisher();

    // Nothing answers, so the request ends in its own timeout; the delivery it produced is the
    // subject here.
    let asking = async {
        let _ = requester
            .request(
                OutgoingMessage::new("greeter", b"{}".as_slice()),
                Duration::from_millis(300),
            )
            .await;
    };
    let receiving = next_delivery(&mut subscriber, "a request");
    let (message, ()) = futures::join!(receiving, asking);

    assert!(
        message.headers().reply_to().is_some(),
        "the ROUTER stamps the requesting peer on every delivery",
    );
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected
        .shutdown()
        .await
        .expect("the responder shuts down");
}
