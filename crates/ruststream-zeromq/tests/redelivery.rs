//! Where a retry copy lands on this transport, and what a registration declares about it.
//!
//! `ZeroMQ` settles nothing, so every retry is a copy this process publishes, and each pattern has
//! to say whether it knows where that copy goes. The one-way patterns do: a publish under the
//! subscribe name reaches the subscription again. The responder does not, so its mount site names
//! the destination or is refused before the subscription opens.

#![cfg(feature = "testing")]

use std::any::type_name;
use std::io;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a reply
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Bindable, Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::TestApp;
use ruststream::{
    AddressedCopies, BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, NamedCopies,
    OutgoingMessage, Publisher, RedeliveryAddress, RedeliveryAddressed, Subscribe, Subscriber,
};
use ruststream_zeromq::testing::{
    Fanout, Queue, Rpc, ZmqTestBroker, ZmqTestRpcSubscriber, ZmqTestSubscriber,
};
use ruststream_zeromq::{
    ConnectedZmqQueue, ZmqEndpoint, ZmqFanout, ZmqMessage, ZmqQueue, ZmqQueuePublish, ZmqRpc,
    ZmqRpcPublish, ZmqSubscriber,
};
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
        declared::<<ZmqTestBroker<Queue> as Broker>::Connected>(),
        declared::<<ZmqTestBroker<Fanout> as Broker>::Connected>(),
    ] {
        assert_eq!(addressed, type_name::<AddressedCopies>());
    }

    for named in [
        declared::<<ZmqRpc as Broker>::Connected>(),
        declared::<<ZmqTestBroker<Rpc> as Broker>::Connected>(),
    ] {
        assert_eq!(named, type_name::<NamedCopies>());
    }
}

/// The responder stand hands out the subscriber that withholds batching, the way `ZmqRpc` does.
///
/// `.batch(..)` on a responder mount is a compile error against both, and a compile error cannot
/// be asserted from a test binary, so what is pinned here is the type the stand yields: the one-way
/// stands yield the batchable subscriber, the responder yields the one that is not.
#[test]
fn the_responder_stand_withholds_batching() {
    fn subscriber_of<C: Subscribe>() -> &'static str {
        type_name::<C::Subscriber>()
    }
    fn batchable<S: BatchSubscriber>() {}

    batchable::<ZmqTestSubscriber>();
    assert_eq!(
        subscriber_of::<<ZmqTestBroker<Rpc> as Broker>::Connected>(),
        type_name::<ZmqTestRpcSubscriber>(),
    );
    for stand in [
        subscriber_of::<<ZmqTestBroker<Queue> as Broker>::Connected>(),
        subscriber_of::<<ZmqTestBroker<Fanout> as Broker>::Connected>(),
    ] {
        assert_eq!(stand, type_name::<ZmqTestSubscriber>());
    }
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
/// nothing, and every registration on it owes a destination for its copies - not only one that
/// binds a retry publisher, because the runtime pairs one from the broker's default either way.
///
/// The stand refuses the same mount with the same words. That is the point of the assertion: a
/// responder mount that starts under the harness and refuses on deployment is the failure mode
/// this parity exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_refuses_a_mount_that_names_no_destination() {
    let socket = RustStream::new(AppInfo::new("zmq-retry-rpc", "0.0.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(answer);
        },
    );
    let stand = RustStream::new(AppInfo::new("zmq-retry-rpc", "0.0.0")).with_broker(
        ZmqTestBroker::rpc(),
        |b| {
            b.include(answer);
        },
    );

    let on_socket = socket
        .start()
        .await
        .expect_err("a responder addresses no copies, so the scope must not start")
        .to_string();
    let on_stand = stand
        .start()
        .await
        .expect_err("the stand must refuse what the socket refuses")
        .to_string();

    assert!(
        on_socket.contains("greeter") && on_socket.contains("NamedCopies"),
        "the refusal must name the subscription and the copy path, got: {on_socket}",
    );
    assert_eq!(
        on_socket, on_stand,
        "the stand must refuse in the words the socket uses, or a test read against it teaches \
         the wrong fix",
    );
}

/// Naming the destination is what makes a responder mount start, and the stand starts on the
/// same spelling.
///
/// A copy published under a plain name reaches nothing on this pattern - the reply publisher
/// routes by peer identity - so the destination that carries a responder's copies belongs to a
/// broker that has destinations. Both forms are shown: the plain name a service writes when it
/// only wants the mount to be explicit, and the queue token a service binds when it means the
/// copies to arrive somewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_mount_that_names_a_destination_runs() {
    let named = RustStream::new(AppInfo::new("zmq-retry-named", "0.0.0")).with_broker(
        ZmqTestBroker::rpc(),
        |b| {
            b.include(answer)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        },
    );
    named
        .start()
        .await
        .expect("a named retry destination starts the responder")
        .shutdown()
        .await
        .expect("the app shuts down");

    let queue = Bindable::new(ZmqTestBroker::queue());
    let token = queue.bind(ZmqQueuePublish);
    let crossed = RustStream::new(AppInfo::new("zmq-retry-crossed", "0.0.0"))
        .with_broker(ZmqTestBroker::rpc(), |b| {
            b.include(answer).out_retry(token).to("greeter.retry");
        })
        .with_broker(queue, |_b| {});
    crossed
        .start()
        .await
        .expect("copies bound to a queue on another broker start the responder")
        .shutdown()
        .await
        .expect("the app shuts down");
}

/// The stand-in answers the one-way patterns' way for every subscription, so a routes file that
/// composes with the fallback in production composes with it under the harness too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stand_in_wires_the_deferred_retry() {
    let broker = ZmqTestBroker::queue();
    let app =
        RustStream::new(AppInfo::new("zmq-retry-harness", "0.0.0")).with_broker(broker, |b| {
            b.include(work).out_retry(ZmqQueuePublish);
        });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Job { id: 1 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker::<ZmqTestBroker<Queue>>()
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
        ZmqTestBroker::queue(),
        |b| {
            b.include(defer_once)
                .out_retry(ZmqQueuePublish)
                .transform(StampRetry);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Job { id: 2 })
        .to("deferred")
        .publish()
        .await
        .expect("the job is published");
    tb.advance(RETRY_DELAY).await.expect("the delay elapses");

    tb.broker::<ZmqTestBroker<Queue>>()
        .published::<Job>("deferred")
        .with_header("x-retried-from", "deferred");
    tb.broker::<ZmqTestBroker<Queue>>()
        .subscriber("deferred")
        .assert_called(2)
        .with(&Job { id: 2 });
}

/// Asks for a retry on every delivery, so the cap is what stops it.
#[subscriber("capped")]
async fn never_succeeds(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::retry()
}

/// The cap is declared at the mount site, and nothing on this transport applies it natively:
/// `ZeroMQ` has no delivery counter, so the runtime counts the copies through its own header and
/// stops where the declaration said. The copies come back through the subscription's own queue.
///
/// A dead-letter destination on that same queue is refused, as the socket refuses it: the
/// subscription that holds the queue would receive the spent delivery as its next one, and a
/// handler that keeps failing would keep making copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declared_cap_stops_the_copies_and_the_own_queue_takes_no_dead_letter() {
    let app = RustStream::new(AppInfo::new("zmq-retry-cap", "0.0.0")).with_broker(
        ZmqTestBroker::queue(),
        |b| {
            b.include(never_succeeds)
                .max_attempts(nonzero!(3u32))
                .dead_letter("capped.dead")
                .out_retry(ZmqQueuePublish);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Job { id: 3 })
        .to("capped")
        .publish()
        .await
        .expect("the job is published");
    tb.settle().await.expect("the copies settle");

    tb.broker::<ZmqTestBroker<Queue>>()
        .subscriber("capped")
        .assert_called(3)
        .with(&Job { id: 3 });
    tb.broker::<ZmqTestBroker<Queue>>()
        .published::<Job>("capped.dead")
        .assert_not_called();
}

/// A dead-letter destination takes the spent delivery once the copies leave through a queue of
/// its own, the arrangement the live test below runs over sockets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_destination_takes_the_spent_delivery_through_a_queue_of_its_own() {
    let dead = ZmqTestBroker::queue().bindable();
    let copies = dead.bind(ZmqQueuePublish);
    let app = RustStream::new(AppInfo::new("zmq-retry-dead-letter", "0.0.0"))
        .with_broker_labeled("worker", ZmqTestBroker::queue(), |b| {
            b.include(never_succeeds)
                .dead_letter("capped.dead")
                .out_retry(copies);
        })
        .with_broker_labeled("dead", dead, |_b| {});
    let tb = TestApp::start(app).await.expect("the harness starts");

    tb.broker_named("worker")
        .message(&Job { id: 4 })
        .to("capped")
        .publish()
        .await
        .expect("the job is published");
    tb.settle().await.expect("the copy settles");

    tb.broker_named("worker")
        .subscriber("capped")
        .assert_called_once()
        .with(&Job { id: 4 });
    tb.broker_named("dead")
        .published::<Job>("capped.dead")
        .assert_called_once()
        .with(&Job { id: 4 });
}

// -- The same promises over real sockets ------------------------------------------------------
//
// Everything above this line reads a declaration or runs on the stand. A copy this service
// publishes is the whole retry story on this transport, so the copy has to be seen leaving one
// socket and arriving on another: a stand that routes in memory cannot tell whether the PUSH
// socket ever accepted it.

/// How long the deferred copy waits. Real sockets rule out a paused clock, so this is short
/// enough to keep the test quick and long enough to be a deferral rather than a race.
const LIVE_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Long enough for a handshake and a deferral on a loaded machine.
const LIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a run has to stay silent before the copies count as stopped.
const SILENCE: Duration = Duration::from_millis(500);

/// What a handler saw on one delivery, published to a second queue.
///
/// The test reads the run off that queue's socket rather than out of the handler, so what it
/// asserts travelled over ZMTP like every other message.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "reports")]
struct Report {
    id: u64,
    attempt: u64,
}

#[derive(OutSlot)]
#[publishes(Report)]
struct Reports;

/// The framework's own counter, as a handler reads it: absent on the first delivery.
fn retry_count<Broker, Shared>(ctx: &Context<'_, Broker, Shared>) -> u64 {
    ctx.headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// Reports every delivery and defers the first one.
#[subscriber("jobs")]
async fn report_then_defer(
    job: &Job,
    ctx: &mut Context,
    Out(reports): Out<impl Publisher, Reports>,
) -> HandlerOutcome {
    let attempt = retry_count(ctx);
    reports
        .message(&Report {
            id: job.id,
            attempt,
        })
        .publish()
        .await
        .expect("the report reaches the sink queue");
    if attempt == 0 {
        HandlerOutcome::retry_after(LIVE_RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Reports every delivery and never succeeds, so the declared cap is what stops it.
#[subscriber("capped")]
async fn report_and_fail(
    job: &Job,
    ctx: &mut Context,
    Out(reports): Out<impl Publisher, Reports>,
) -> HandlerOutcome {
    let attempt = retry_count(ctx);
    reports
        .message(&Report {
            id: job.id,
            attempt,
        })
        .publish()
        .await
        .expect("the report reaches the sink queue");
    HandlerOutcome::retry()
}

/// Opens the queue the copies and reports arrive on, and hands back the address the app dials.
async fn live_sink() -> (ConnectedZmqQueue, ZmqSubscriber, String) {
    let sink = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the sink queue connects");
    let subscriber = sink
        .subscribe("sink")
        .await
        .expect("the sink subscription opens");
    let address = sink
        .bound_address()
        .expect("the sink subscription bound a port");
    (sink, subscriber, address)
}

/// The next delivery off the sink's socket, or a failure naming what was waited for.
async fn next_at_sink(subscriber: &mut ZmqSubscriber) -> ZmqMessage {
    let mut stream = pin!(subscriber.stream());
    timeout(LIVE_TIMEOUT, stream.next())
        .await
        .expect("a delivery arrives at the sink before the deadline")
        .expect("the sink stream is open")
        .expect("the delivery is well formed")
}

async fn next_report(subscriber: &mut ZmqSubscriber) -> Report {
    let message = next_at_sink(subscriber).await;
    serde_json::from_slice(message.payload()).expect("the report decodes")
}

/// Fails when anything else reaches the sink within the silence window.
async fn expect_silence(subscriber: &mut ZmqSubscriber, why: &str) {
    let mut stream = pin!(subscriber.stream());
    assert!(
        timeout(SILENCE, stream.next()).await.is_err(),
        "{why}: something still arrived at the sink",
    );
}

/// Nothing settles here, so a deferred delivery survives only as a copy the service publishes.
/// This is that copy on a real PUSH/PULL pair: it leaves the publisher socket, arrives on the
/// subscription's own socket, and carries the framework's counter forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_copy_travels_the_socket_and_carries_the_count() {
    let (sink, mut reports, address) = live_sink().await;
    let egress = ZmqQueue::new(ZmqEndpoint::connect(address)).bindable();
    let slot = egress.bind(ZmqQueuePublish);

    let app = RustStream::new(AppInfo::new("zmq-live-retry", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")), |b| {
            b.include(report_then_defer)
                .out(Reports, slot)
                .out_retry(ZmqQueuePublish)
                .build();
            b.after_startup(ZmqQueuePublish, async move |publisher| -> io::Result<()> {
                publisher
                    .message(&Job { id: 11 })
                    .to("jobs")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        })
        .with_broker(egress, |_b| {});
    let running = app.start().await.expect("the app starts");

    assert_eq!(
        next_report(&mut reports).await,
        Report { id: 11, attempt: 0 },
        "the first delivery carries no retry count",
    );
    assert_eq!(
        next_report(&mut reports).await,
        Report { id: 11, attempt: 1 },
        "the deferred copy comes back on the subscription with the count incremented",
    );
    expect_silence(&mut reports, "the second delivery was acknowledged").await;

    running.shutdown().await.expect("the app shuts down");
    sink.shutdown().await.expect("the sink shuts down");
}

/// The cap is the framework's, counted through a header the copies carry. Over a socket that
/// shows what the stand cannot: the copies really do come back to the subscription, and the run
/// really does stop where the declaration said rather than looping while a handler keeps failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_cap_stops_the_copies_over_a_socket() {
    let (sink, mut reports, address) = live_sink().await;
    let egress = ZmqQueue::new(ZmqEndpoint::connect(address)).bindable();
    let slot = egress.bind(ZmqQueuePublish);

    let app = RustStream::new(AppInfo::new("zmq-live-cap", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")), |b| {
            b.include(report_and_fail)
                .max_attempts(nonzero!(3u32))
                .out(Reports, slot)
                .out_retry(ZmqQueuePublish)
                .build();
            b.after_startup(ZmqQueuePublish, async move |publisher| -> io::Result<()> {
                publisher
                    .message(&Job { id: 12 })
                    .to("capped")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        })
        .with_broker(egress, |_b| {});
    let running = app.start().await.expect("the app starts");

    for attempt in 0..3 {
        assert_eq!(
            next_report(&mut reports).await,
            Report { id: 12, attempt },
            "delivery {attempt} must be the copy the previous one asked for",
        );
    }
    // Three deliveries is what the declaration allows, and the transport has no redelivery of
    // its own to add a fourth.
    expect_silence(
        &mut reports,
        "the cap is spent and no dead-letter destination is declared",
    )
    .await;

    running.shutdown().await.expect("the app shuts down");
    sink.shutdown().await.expect("the sink shuts down");
}

/// A dead-letter destination on the queue the subscription binds would come back to that
/// subscription: the spent delivery would arrive as a fourth job, fail again, and be dead-lettered
/// again, forever. The publish is refused instead, so the run stops at the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_on_the_bound_queue_is_refused_rather_than_looped_over_a_socket() {
    let (sink, mut reports, address) = live_sink().await;
    let egress = ZmqQueue::new(ZmqEndpoint::connect(address)).bindable();
    let slot = egress.bind(ZmqQueuePublish);

    let app = RustStream::new(AppInfo::new("zmq-live-own-dead-letter", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")), |b| {
            b.include(report_and_fail)
                .max_attempts(nonzero!(3u32))
                .dead_letter("capped.dead")
                .out(Reports, slot)
                .out_retry(ZmqQueuePublish)
                .build();
            b.after_startup(ZmqQueuePublish, async move |publisher| -> io::Result<()> {
                publisher
                    .message(&Job { id: 14 })
                    .to("capped")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        })
        .with_broker(egress, |_b| {});
    let running = app.start().await.expect("the app starts");

    for attempt in 0..3 {
        assert_eq!(
            next_report(&mut reports).await,
            Report { id: 14, attempt },
            "delivery {attempt} must be the copy the previous one asked for",
        );
    }
    expect_silence(
        &mut reports,
        "the dead letter came back to the subscription that gave up on it",
    )
    .await;

    running.shutdown().await.expect("the app shuts down");
    sink.shutdown().await.expect("the sink shuts down");
}

/// A dead-letter destination alone takes every failed delivery, and here it takes it over a
/// socket to a queue of its own: the copy arrives under the declared name, with the payload it
/// was delivered with and the framework's counter on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_destination_takes_the_spent_delivery_over_a_socket() {
    let (sink, mut dead, address) = live_sink().await;
    let egress = ZmqQueue::new(ZmqEndpoint::connect(address)).bindable();
    let copies = egress.bind(ZmqQueuePublish);

    let app = RustStream::new(AppInfo::new("zmq-live-dead-letter", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")), |b| {
            b.include(never_succeeds)
                .dead_letter("capped.dead")
                .out_retry(copies);
            b.after_startup(ZmqQueuePublish, async move |publisher| -> io::Result<()> {
                publisher
                    .message(&Job { id: 13 })
                    .to("capped")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        })
        .with_broker(egress, |_b| {});
    let running = app.start().await.expect("the app starts");

    let message = next_at_sink(&mut dead).await;
    assert_eq!(
        message.name(),
        "capped.dead",
        "frame 0 of the copy is the declared destination, which is what a peer routes on",
    );
    assert_eq!(
        message.headers().get_str(RETRY_COUNT_HEADER),
        Some("1"),
        "the copy carries the framework's counter, because the transport has none",
    );
    assert_eq!(
        serde_json::from_slice::<Job>(message.payload()).expect("the job decodes"),
        Job { id: 13 },
    );
    expect_silence(&mut dead, "one failed delivery makes one dead letter").await;

    running.shutdown().await.expect("the app shuts down");
    sink.shutdown().await.expect("the sink shuts down");
}
