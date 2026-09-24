//! Where a retry copy lands on this transport, and what a registration declares about it.
//!
//! `ZeroMQ` settles nothing, so every retry is a copy this process publishes, and each pattern has
//! to say whether it knows where that copy goes. A one-way subscription that binds does: a publish
//! under the subscribe name reaches it again through its own listener. One that dials does not,
//! because the peer it reads from only sends, and the responder does not either; their mount sites
//! name the destination or are refused before the subscription opens.

#![cfg(feature = "testing")]

use std::any::type_name;
use std::io;
use std::pin::pin;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::runtime::{Bindable, RETRY_COUNT_HEADER};
use ruststream::testing::{InProcess, TestApp};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, IncomingMessage, NamedCopies, OutgoingMessage,
    Publisher, RedeliveryAddress, RedeliveryAddressed, Subscribe, Subscriber,
};
use ruststream_zeromq::{
    Bind, Connect, ConnectedZmqQueue, ZmqEndpoint, ZmqFanout, ZmqMessage, ZmqQueue,
    ZmqQueuePublish, ZmqRpc, ZmqRpcPublish, ZmqSubscriber,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use zeromq::{PubSocket, PushSocket, Socket, SocketSend};

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
        declared::<<ZmqQueue<Bind> as Broker>::Connected>(),
        declared::<<ZmqFanout<Bind> as Broker>::Connected>(),
    ] {
        assert_eq!(addressed, type_name::<AddressedCopies>());
    }

    // A one-way subscription that dials reads from a peer that only sends, so it addresses no
    // copy, whichever pattern it is.
    for named in [
        declared::<<ZmqQueue<Connect> as Broker>::Connected>(),
        declared::<<ZmqFanout<Connect> as Broker>::Connected>(),
        declared::<<ZmqRpc as Broker>::Connected>(),
    ] {
        assert_eq!(named, type_name::<NamedCopies>());
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

/// A responder whose mount names no destination for its copies.
fn responder_without_a_destination() -> RustStream {
    RustStream::new(AppInfo::new("zmq-retry-rpc", "0.0.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(answer);
        },
    )
}

/// A responder's name is not a publish destination: the reply publisher routes to the peer
/// identity a request carried and refuses a plain name. The descriptor therefore addresses
/// nothing, and every registration on it owes a destination for its copies - not only one that
/// binds a retry publisher, because the runtime pairs one from the broker's default either way.
///
/// The in-process mode refuses the same mount with the same words. That is the point of the
/// assertion: a responder mount that starts under the harness and refuses on deployment is the
/// failure mode this parity exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_refuses_a_mount_that_names_no_destination() {
    let Err(live) = TestApp::start_live(responder_without_a_destination()).await else {
        panic!("a responder addresses no copies, so the scope must not start");
    };
    let Err(in_process) = TestApp::start(responder_without_a_destination()).await else {
        panic!("the in-process mode must refuse what the socket refuses");
    };

    let live = live.to_string();
    assert!(
        live.contains("greeter") && live.contains("NamedCopies"),
        "the refusal must name the subscription and the copy path, got: {live}",
    );
    assert_eq!(
        in_process.to_string(),
        live,
        "the in-process mode must refuse in the words the socket uses, or a test read against it \
         teaches the wrong fix",
    );
}

/// Naming the destination is what makes a responder mount start.
///
/// A copy published under a plain name reaches nothing on this pattern - the reply publisher
/// routes by peer identity - so the destination that carries a responder's copies belongs to a
/// broker that has destinations. Both forms are shown: the plain name a service writes when it
/// only wants the mount to be explicit, and the queue token a service binds when it means the
/// copies to arrive somewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_responder_mount_that_names_a_destination_runs() {
    let named = RustStream::new(AppInfo::new("zmq-retry-named", "0.0.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(answer)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        },
    );
    TestApp::start(named)
        .await
        .expect("a named retry destination starts the responder")
        .shutdown()
        .await
        .expect("the app shuts down");

    let queue = Bindable::new(ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")));
    let token = queue.bind(ZmqQueuePublish);
    let crossed = RustStream::new(AppInfo::new("zmq-retry-crossed", "0.0.0"))
        .with_broker(ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")), |b| {
            b.include(answer).out_retry(token).to("greeter.retry");
        })
        .with_broker(queue, |_b| {});
    TestApp::start(crossed)
        .await
        .expect("copies bound to a queue on another broker start the responder")
        .shutdown()
        .await
        .expect("the app shuts down");
}

/// A worker that dials the ventilator at `address` and names no destination for its copies.
fn dialing_worker_without_a_destination(address: &str) -> RustStream {
    RustStream::new(AppInfo::new("zmq-retry-worker", "0.0.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::connect(address)),
        |b| {
            b.include(work).out_retry(ZmqQueuePublish);
        },
    )
}

/// A worker that dials the endpoint pulls from the peer there, and that peer only pushes: a copy
/// published back to it is refused by the handshake. The subscription therefore addresses
/// nothing, and a registration that names no destination for its copies is refused before the
/// subscription opens, rather than dropping every copy with a warning once it runs.
///
/// The in-process mode on the same side refuses the same mount in the same words.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connect_role_worker_refuses_a_mount_that_names_no_destination() {
    let (_ventilator, address) = ventilator().await;
    let Err(live) = TestApp::start_live(dialing_worker_without_a_destination(&address)).await
    else {
        panic!("a worker that dials addresses no copies, so the scope must not start");
    };
    let Err(in_process) = TestApp::start(dialing_worker_without_a_destination(&address)).await
    else {
        panic!("the in-process mode must refuse what the socket refuses");
    };

    let live = live.to_string();
    assert!(
        live.contains("jobs") && live.contains("NamedCopies"),
        "the refusal must name the subscription and the copy path, got: {live}",
    );
    assert_eq!(in_process.to_string(), live);
}

/// A worker that dials the ventilator at `address` and sends its copies to a queue it binds.
fn dialing_worker_with_a_destination(address: &str) -> RustStream {
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")).bindable();
    let copies = retries.bind(ZmqQueuePublish);
    RustStream::new(AppInfo::new("zmq-retry-worker", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::connect(address)), |b| {
            b.include(work).out_retry(copies).to("jobs");
        })
        .with_broker(retries, |_b| {})
}

/// The same mount starts once it names where the copies go, in both modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connect_role_worker_that_names_a_destination_starts() {
    let (_ventilator, address) = ventilator().await;
    TestApp::start_live(dialing_worker_with_a_destination(&address))
        .await
        .expect("a named destination starts the worker over the socket")
        .shutdown()
        .await
        .expect("the app shuts down");
    TestApp::start(dialing_worker_with_a_destination(&address))
        .await
        .expect("a named destination starts the worker in process")
        .shutdown()
        .await
        .expect("the app shuts down");
}

/// A queue scope wires the fallback in process as it does over the socket, so a routes file that
/// composes with it in production composes with it under the harness too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_wires_the_deferred_retry() {
    let app = RustStream::new(AppInfo::new("zmq-retry-harness", "0.0.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(work).out_retry(ZmqQueuePublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqQueue>()
        .message(&Job { id: 1 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker::<ZmqQueue>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 1 });
    tb.shutdown().await.expect("the app shuts down");
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
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(never_succeeds)
                .max_attempts(nonzero!(3u32))
                .dead_letter("capped.dead")
                .out_retry(ZmqQueuePublish);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    tb.broker::<ZmqQueue>()
        .message(&Job { id: 3 })
        .to("capped")
        .publish()
        .await
        .expect("the job is published");
    tb.settle().await.expect("the copies settle");

    tb.broker::<ZmqQueue>()
        .subscriber("capped")
        .assert_called(3)
        .with(&Job { id: 3 });
    tb.broker::<ZmqQueue>()
        .published::<Job>("capped.dead")
        .assert_not_called();
    tb.shutdown().await.expect("the app shuts down");
}

/// A dead-letter destination takes the spent delivery once the copies leave through a queue of
/// its own, the arrangement the live test below runs over sockets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_destination_takes_the_spent_delivery_through_a_queue_of_its_own() {
    let dead = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")).bindable();
    let copies = dead.bind(ZmqQueuePublish);
    let app = RustStream::new(AppInfo::new("zmq-retry-dead-letter", "0.0.0"))
        .with_broker_labeled(
            "worker",
            ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
            |b| {
                b.include(never_succeeds)
                    .dead_letter("capped.dead")
                    .out_retry(copies);
            },
        )
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
    tb.shutdown().await.expect("the app shuts down");
}

// -- The same promises over real sockets ------------------------------------------------------
//
// Everything above this line reads a declaration or runs in process. A copy this service
// publishes is the whole retry story on this transport, so the copy has to be seen leaving one
// socket and arriving on another: a transport that routes in memory cannot tell whether the PUSH
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
/// shows what the in-process mode cannot: the copies really do come back to the subscription, and the run
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

/// A ventilator the way a foreign peer runs one: a raw PUSH socket listening on an ephemeral
/// port, and the address a worker dials.
async fn ventilator() -> (PushSocket, String) {
    let mut socket = PushSocket::new();
    let address = socket
        .bind("tcp://127.0.0.1:0")
        .await
        .expect("the ventilator binds")
        .to_string();
    (socket, address)
}

/// Pushes one job in the documented three frames, waiting out the handshake a worker that has
/// just dialled may still be settling: a PUSH socket with no peer attached hands the frames back.
async fn push_job(ventilator: &mut PushSocket, name: &str, job: &Job) {
    let mut frames = zeromq::ZmqMessage::from(name.to_owned());
    frames.push_back(Bytes::new());
    frames.push_back(Bytes::from(
        serde_json::to_vec(job).expect("the job encodes"),
    ));
    timeout(LIVE_TIMEOUT, async {
        loop {
            match ventilator.send(frames.clone()).await {
                Ok(()) => break,
                Err(zeromq::ZmqError::ReturnToSender { .. }) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("the ventilator could not push: {err}"),
            }
        }
    })
    .await
    .expect("a worker attaches to the ventilator before the deadline");
}

/// A worker that dials a ventilator cannot hand a copy back to it, so its mount names where the
/// copies go: here a queue this service binds, where the same handler takes them. The copy leaves
/// one socket, arrives on another, and carries the framework's counter forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connect_role_workers_copy_reaches_the_queue_its_mount_names() {
    let (sink, mut reports, address) = live_sink().await;
    let egress = ZmqQueue::new(ZmqEndpoint::connect(address)).bindable();
    let from_worker = egress.bind(ZmqQueuePublish);
    let from_retries = egress.bind(ZmqQueuePublish);
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")).bindable();
    let copies = retries.bind(ZmqQueuePublish);
    let (mut ventilator, jobs) = ventilator().await;

    let app = RustStream::new(AppInfo::new("zmq-live-worker-retry", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::connect(jobs)), |b| {
            b.include(report_then_defer)
                .out(Reports, from_worker)
                .out_retry(copies)
                .to("jobs")
                .build();
        })
        .with_broker(retries, |b| {
            b.include(report_then_defer)
                .out(Reports, from_retries)
                .build();
        })
        .with_broker(egress, |_b| {});
    let running = app.start().await.expect("the app starts");
    push_job(&mut ventilator, "jobs", &Job { id: 21 }).await;

    assert_eq!(
        next_report(&mut reports).await,
        Report { id: 21, attempt: 0 },
        "the first delivery carries no retry count",
    );
    assert_eq!(
        next_report(&mut reports).await,
        Report { id: 21, attempt: 1 },
        "the deferred copy reaches the queue the mount named, with the count incremented",
    );
    expect_silence(&mut reports, "the copy was acknowledged").await;

    running.shutdown().await.expect("the app shuts down");
    sink.shutdown().await.expect("the sink shuts down");
}

/// A publish on a broker whose subscription dialed the endpoint reaches the peer that subscription
/// reads from, which only sends. The socket publisher refuses it before the handshake would, and
/// the in-process mode refuses it in the same words, so a mount that names the worker's own queue
/// for its copies fails its test instead of dropping every copy after deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connect_role_publish_is_refused_in_the_sockets_words_in_process() {
    let (_ventilator, address) = ventilator().await;
    let worker = ZmqQueue::new(ZmqEndpoint::connect(address))
        .connect()
        .await
        .expect("the worker connects");
    let _dialed = worker
        .subscribe("jobs")
        .await
        .expect("the subscription dials the ventilator");
    let on_socket = worker
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"copy".as_slice()), None)
        .await
        .expect_err("the ventilator takes nothing, so the publish is refused")
        .to_string();

    let in_process = ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555"))
        .connect_in_process()
        .await
        .expect("the worker connects in process");
    let _dialed = in_process
        .subscribe("jobs")
        .await
        .expect("the subscription opens");
    let refused = in_process
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"copy".as_slice()), None)
        .await
        .expect_err("the in-process mode must refuse what the socket refuses")
        .to_string();

    assert!(
        on_socket.contains("'jobs'") && on_socket.contains("PUSH"),
        "the refusal must name the subscription and the peer it dialed, got: {on_socket}",
    );
    assert_eq!(refused, on_socket);
    worker.shutdown().await.expect("the worker shuts down");
}

/// The same refusal on the fan-out: a watcher that dials reads from a PUB peer, which takes
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connect_role_fan_out_publish_is_refused_in_the_sockets_words_in_process() {
    let mut source = PubSocket::new();
    let address = source
        .bind("tcp://127.0.0.1:0")
        .await
        .expect("the source binds")
        .to_string();
    let watcher = ZmqFanout::new(ZmqEndpoint::connect(address))
        .connect()
        .await
        .expect("the watcher connects");
    let _dialed = watcher
        .subscribe("events")
        .await
        .expect("the subscription dials the source");
    let on_socket = watcher
        .publisher()
        .publish(OutgoingMessage::new("events", b"copy".as_slice()), None)
        .await
        .expect_err("the source takes nothing, so the publish is refused")
        .to_string();

    let in_process = ZmqFanout::new(ZmqEndpoint::connect("tcp://source:5556"))
        .connect_in_process()
        .await
        .expect("the watcher connects in process");
    let _dialed = in_process
        .subscribe("events")
        .await
        .expect("the subscription opens");
    let refused = in_process
        .publisher()
        .publish(OutgoingMessage::new("events", b"copy".as_slice()), None)
        .await
        .expect_err("the in-process mode must refuse what the socket refuses")
        .to_string();

    assert!(
        on_socket.contains("'events'") && on_socket.contains("PUB"),
        "the refusal must name the subscription and the peer it dialed, got: {on_socket}",
    );
    assert_eq!(refused, on_socket);
    watcher.shutdown().await.expect("the watcher shuts down");
}

/// A publisher taken before the connection shares the broker's state, as every handle of one
/// broker does: once a subscription dials, it is refused the way the connected broker's own is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_early_publisher_is_refused_once_a_subscription_dials() {
    let broker = ZmqQueue::new(ZmqEndpoint::connect("tcp://127.0.0.1:5555"));
    let early = broker.publisher();
    let connected = broker
        .connect_in_process()
        .await
        .expect("the queue connects in process");
    let _dialed = connected
        .subscribe("jobs")
        .await
        .expect("the subscription opens");

    let refused = early
        .publish(OutgoingMessage::new("jobs", b"copy".as_slice()), None)
        .await
        .expect_err("the early publisher is refused as the broker's own is")
        .to_string();
    assert!(refused.contains("PUSH"), "got: {refused}");
    connected.shutdown().await.expect("the queue shuts down");
}
