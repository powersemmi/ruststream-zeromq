//! Where a retry copy lands on this transport, and what a registration declares about it.
//!
//! `ZeroMQ` settles nothing, so every retry is a copy this process publishes, and each pattern has
//! to say whether it knows where that copy goes. The one-way patterns do: a publish under the
//! subscribe name reaches the subscription again. The responder does not, so its mount site names
//! the destination or is refused before the subscription opens.

#![cfg(feature = "testing")]

use std::any::type_name;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a reply
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::TestApp;
use ruststream::{
    AddressedCopies, Broker, IncomingMessage, NamedCopies, OutgoingMessage, Publisher,
    RedeliveryAddress, RedeliveryAddressed, Subscribe, Subscriber,
};
use ruststream_zeromq::testing::ZmqTestBroker;
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

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

/// Which copy path each pattern declares, read off the type rather than off a running app.
///
/// The declaration is what decides at compile time whether a mount site owes a destination, so it
/// is worth pinning on its own: a pattern that silently changed its answer would only show up as a
/// mount somewhere else refusing to build.
#[test]
fn every_pattern_declares_its_copy_path() {
    fn declared<C: Subscribe>() -> &'static str {
        type_name::<C::Copies>()
    }

    for addressed in [
        declared::<<ZmqQueue as Broker>::Connected>(),
        declared::<<ZmqFanout as Broker>::Connected>(),
        declared::<<ZmqTestBroker as Broker>::Connected>(),
    ] {
        assert_eq!(addressed, type_name::<AddressedCopies>());
    }

    assert_eq!(
        declared::<<ZmqRpc as Broker>::Connected>(),
        type_name::<NamedCopies>(),
    );
}

/// The address the one-way patterns report: the subscription's own name.
///
/// On PUSH/PULL the name frame is not a filter - a subscription receives whatever its socket
/// receives - so a publish under the wrong name would still arrive there and an end-to-end test
/// could not tell the answers apart. This is where the value itself is pinned; the conformance
/// suite checks that a publish there really does come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_one_way_patterns_address_their_own_subscription() {
    let queue = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the queue connects");
    let fanout = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the fan-out connects");

    assert_eq!(
        Name::new("jobs")
            .redelivery_address(&queue)
            .await
            .expect("a queue reports an address"),
        RedeliveryAddress::new("jobs"),
    );
    assert_eq!(
        Name::new("events")
            .redelivery_address(&fanout)
            .await
            .expect("a fan-out reports an address"),
        RedeliveryAddress::new("events"),
    );
}

/// A publish to the address a fan-out reports really does come back on the subscription that
/// reported it.
///
/// The conformance scenario checks this for the queue, where a PUSH send waits for its peer. It
/// cannot check the fan-out: a PUB socket drops what it sends before the subscriber's filter has
/// propagated, which is the pattern's contract and not a fault, so a single-shot publish would be
/// a flaky test rather than a contract check. Publishing until the first delivery lands proves the
/// same promise for the warm connection a running service publishes a retry copy over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_to_the_fan_outs_address_reaches_its_subscription() {
    let fanout = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the fan-out connects");
    let mut subscriber = fanout
        .subscribe("events")
        .await
        .expect("the subscription opens");
    let address = Name::new("events")
        .redelivery_address(&fanout)
        .await
        .expect("a fan-out reports an address");

    let publisher = fanout.publisher();
    let mut stream = pin!(subscriber.stream());
    let mut delivered = None;
    for _ in 0..50 {
        publisher
            .publish(
                OutgoingMessage::new(address.as_str(), b"redelivered".as_slice()),
                None,
            )
            .await
            .expect("the copy is published");
        if let Ok(Some(next)) = timeout(Duration::from_millis(200), stream.next()).await {
            delivered = Some(next.expect("the delivery is ok"));
            break;
        }
    }

    let message = delivered.expect("a copy published to the reported address arrives");
    assert_eq!(message.payload(), b"redelivered");
}

/// A queue subscription addresses itself, so a registration binds the retry publisher and names
/// nothing: the runtime already knows where a copy goes.
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
/// identity a request carried and refuses a plain name. The descriptor therefore addresses
/// nothing, and a registration that names no destination for its copies is refused at startup,
/// with the subscription and the copy path in the message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_scope_refuses_an_unnamed_retry_destination() {
    let broker = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let app = RustStream::new(AppInfo::new("zmq-retry-rpc", "0.0.0")).with_broker(broker, |b| {
        b.include(answer).out_retry(ZmqRpcPublish);
    });

    let error = app
        .start()
        .await
        .expect_err("a responder addresses no copies, so the scope must not start");
    let message = error.to_string();
    assert!(
        message.contains("greeter") && message.contains("NamedCopies"),
        "the refusal must name the subscription and the copy path, got: {message}",
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

/// Stamps a retry copy with the subscription the delivery came from.
///
/// The retry position reads the delivery being retried, so the transform is written for
/// [`ForReply`] and takes a [`PublishContext`]. It sets no per-message setting, so it stays
/// generic over the options type - on this transport that is the only shape available, since
/// every publisher here declares `Options = ()`.
#[derive(Debug, Clone, Copy)]
struct StampRetry;

impl<C, Options> PublishTransform<ForReply<C>, Options> for StampRetry {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
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

/// A transform on the retry position runs on the deferred copy and nothing else sees that copy.
/// This is where a service marks a redelivery on a transport that settles nothing, and it reads
/// the delivery it is retrying rather than the slot it leaves through.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    let app = RustStream::new(AppInfo::new("zmq-retry-stamp", "0.0.0")).with_broker(
        ZmqTestBroker::new(),
        |b| {
            b.include(defer_once)
                .out_retry(ZmqQueuePublish)
                .transform(StampRetry);
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
        .with_header("x-retried-from", "deferred");
    tb.broker::<ZmqTestBroker>()
        .subscriber("deferred")
        .assert_called(2)
        .with(&Job { id: 2 });
}

/// Asks for a retry on every delivery, so the cap is what stops it.
#[subscriber("capped")]
async fn never_succeeds(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::retry()
}

/// The cap and the dead-letter destination are declared at the mount site, and nothing on this
/// transport applies them natively: `ZeroMQ` has no delivery counter and no dead-letter topology,
/// so the runtime counts the copies through its own header and republishes the spent delivery
/// where the registration said.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declared_cap_sends_a_spent_delivery_to_the_dead_letter_destination() {
    let app = RustStream::new(AppInfo::new("zmq-retry-cap", "0.0.0")).with_broker(
        ZmqTestBroker::new(),
        |b| {
            b.include(never_succeeds)
                .max_attempts(nonzero!(3u32))
                .dead_letter("capped.dead")
                .out_retry(ZmqQueuePublish);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    tb.broker::<ZmqTestBroker>()
        .message(&Job { id: 3 })
        .to("capped")
        .publish()
        .await
        .expect("the job is published");
    tb.settle().await.expect("the copies settle");

    tb.broker::<ZmqTestBroker>()
        .subscriber("capped")
        .assert_called(3)
        .with(&Job { id: 3 });
    tb.broker::<ZmqTestBroker>()
        .published::<Job>("capped.dead")
        .with(&Job { id: 3 });
}
