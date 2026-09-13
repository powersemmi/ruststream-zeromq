//! The stand-in settles the way the transport settles: not at all.
//!
//! `ZeroMQ` has no protocol frame to settle a delivery with, so a handler that asks for a retry
//! gets none. The test that matters is the one that used to pass for the wrong reason: a retrying
//! handler must be called once here, exactly as it is called once over a socket.

#![cfg(feature = "testing")]

use std::pin::pin;

use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::{TestApp, TestableBroker};
use ruststream::{AckError, IncomingMessage, OutgoingMessage, Subscribe, Subscriber};
use ruststream_zeromq::testing::{Queue, ZmqTestBroker};
use serde::{Deserialize, Serialize};

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
