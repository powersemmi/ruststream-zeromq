//! Where a deferred retry lands on this transport.
//!
//! `ZeroMQ` settles nothing, so a handler asking for a delayed retry is served only by the copy
//! the runtime publishes after the delay, and every pattern has to say whether a publish reaches
//! its subscription again. The one-way patterns do and say so; the responder does not, and the
//! service that wires a retry over one is stopped at startup rather than losing the message.

#![cfg(feature = "testing")]

use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{Broker, RedeliveryAddress, Subscribe};
use ruststream_zeromq::testing::ZmqTestBroker;
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqRpc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn work(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("greeter")]
async fn answer(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Each pattern's answer, read straight off the connected form: the one-way patterns name the
/// subscription itself, and the responder says it cannot.
///
/// On PUSH/PULL the name frame is not a filter - a subscription receives whatever its socket
/// receives - so a publish under the wrong name would still arrive there and the end-to-end suite
/// could not tell the answers apart. This is where the value is pinned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_pattern_reports_where_a_retry_reaches_it() {
    let queue = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the queue connects");
    let fanout = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the fan-out connects");
    let rpc = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the responder connects");

    assert_eq!(
        queue.redelivery_address("jobs"),
        Some(RedeliveryAddress::new("jobs")),
    );
    assert_eq!(
        fanout.redelivery_address("events"),
        Some(RedeliveryAddress::new("events")),
    );
    assert_eq!(rpc.redelivery_address("greeter"), None);
}

/// A queue subscription answers with its own name, so a scope that wires the fallback starts and
/// the runtime has somewhere to publish a delayed copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_scope_wires_the_deferred_retry() {
    let broker = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let retry = broker.publisher();
    let app = RustStream::new(AppInfo::new("zmq-retry", "0.0.0")).with_broker(broker, |b| {
        b.retry_via(retry);
        b.include(work);
    });

    let running = app
        .start()
        .await
        .expect("a queue scope starts with a retry publisher");
    running.shutdown().await.expect("the app shuts down");
}

/// A responder's name is not a publish destination: the reply publisher routes to the peer
/// identity a request carried and refuses a plain name. The scope is refused at startup, and the
/// error names the subscription so the operator knows which mount to change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_scope_refuses_the_deferred_retry() {
    let broker = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let retry = broker.publisher();
    let app = RustStream::new(AppInfo::new("zmq-retry-rpc", "0.0.0")).with_broker(broker, |b| {
        b.retry_via(retry);
        b.include(answer);
    });

    let error = app
        .start()
        .await
        .expect_err("a responder reports no redelivery address, so the scope must not start");
    let message = error.to_string();
    assert!(
        message.contains("greeter") && message.contains("redelivery address"),
        "the refusal must name the subscription and what it could not report, got: {message}",
    );
}

/// The stand-in answers the one-way patterns' way for every subscription, so a routes file that
/// composes with the fallback in production composes with it under the harness too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stand_in_wires_the_deferred_retry() {
    let broker = ZmqTestBroker::new();
    let retry = broker.queue_publisher();
    let app =
        RustStream::new(AppInfo::new("zmq-retry-harness", "0.0.0")).with_broker(broker, |b| {
            b.retry_via(retry);
            b.include(work);
        });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker>()
        .message(&Job { id: 1 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker::<ZmqTestBroker>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 1 });
}
