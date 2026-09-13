//! Where a deferred retry lands on this transport.
//!
//! `ZeroMQ` settles nothing, so a handler asking for a delayed retry is served only by the copy
//! the runtime publishes after the delay, and every pattern has to say whether a publish reaches
//! its subscription again. The one-way patterns do and say so; the responder does not, and the
//! registration that binds a retry over one is stopped at startup rather than losing the message.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a reply
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, RETRY_COUNT_HEADER, SlotContext};
use ruststream::testing::TestApp;
use ruststream::{Broker, RedeliveryAddress, Subscribe};
use ruststream_zeromq::testing::ZmqTestBroker;
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish};
use serde::{Deserialize, Serialize};

/// Long enough that the copy is visibly deferred on a paused clock.
const RETRY_DELAY: Duration = Duration::from_secs(5);

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

/// A queue subscription answers with its own name, so a registration that binds the fallback
/// starts and the runtime has somewhere to publish a delayed copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_scope_wires_the_deferred_retry() {
    let broker = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let app = RustStream::new(AppInfo::new("zmq-retry", "0.0.0")).with_broker(broker, |b| {
        b.include(work).out_retry(ZmqQueuePublish);
    });

    let running = app
        .start()
        .await
        .expect("a queue scope starts with a retry publisher");
    running.shutdown().await.expect("the app shuts down");
}

/// A responder's name is not a publish destination: the reply publisher routes to the peer
/// identity a request carried and refuses a plain name. The registration is refused at startup,
/// and the error names the subscription so the operator knows which mount to change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_scope_refuses_the_deferred_retry() {
    let broker = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let app = RustStream::new(AppInfo::new("zmq-retry-rpc", "0.0.0")).with_broker(broker, |b| {
        b.include(answer).out_retry(ZmqRpcPublish);
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
    let app =
        RustStream::new(AppInfo::new("zmq-retry-harness", "0.0.0")).with_broker(broker, |b| {
            b.include(work).out_retry(ZmqQueuePublish);
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

/// Stamps every message leaving the slot it is mounted on with that slot's name.
///
/// It sets no per-message setting, so it is generic over the options type and mounts on any
/// publisher. On this transport that is the only shape available: every publisher here declares
/// `Options = ()`.
#[derive(Debug, Clone, Copy)]
struct StampSlot;

impl<Options> PublishTransform<ForSlot, Options> for StampSlot {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, cx: &SlotContext<'_>) {
        out.headers_mut()
            .insert("x-left-through", cx.slot().to_owned());
    }
}

/// Defers the first delivery and acks the copy that comes back.
#[subscriber("deferred")]
async fn defer_once(_job: &Job, ctx: &mut Context) -> HandlerOutcome {
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(RETRY_DELAY)
    }
}

/// The retry position is an ordinary slot, so a transform mounted on it runs on the deferred copy
/// and nothing else sees that copy. This is where a service marks a redelivery on a transport
/// that settles nothing.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_slot_stamps_the_deferred_copy() {
    let app = RustStream::new(AppInfo::new("zmq-retry-stamp", "0.0.0")).with_broker(
        ZmqTestBroker::new(),
        |b| {
            b.include(defer_once)
                .out_retry(ZmqQueuePublish)
                .transform(StampSlot);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    tb.broker::<ZmqTestBroker>()
        .message(&Job { id: 2 })
        .to("deferred")
        .publish()
        .await
        .expect("the job is published");
    tb.advance(RETRY_DELAY).await.expect("the delay elapses");

    tb.broker::<ZmqTestBroker>()
        .published::<Job>("deferred")
        .with_header("x-left-through", "Retry");
    tb.broker::<ZmqTestBroker>()
        .subscriber("deferred")
        .assert_called(2)
        .with(&Job { id: 2 });
}
