//! What the in-process stand-in does, pattern by pattern.
//!
//! Two halves: the publishers driven directly, where each pattern's delivery rule is visible on
//! its own, and the same rules reached through a service's wiring under the `TestApp` harness -
//! the mount sites a routes file writes, with the production policies attached. Socket behaviour
//! (delivery guarantees, back-pressure, the slow joiner) is the loopback suite's business.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::sync::OnceLock;
use std::time::Duration;

use futures::StreamExt;
use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{
    AppInfo, DefaultSlot, ForReply, HandlerOutcome, Names, Out, Outgoing, PublishContext,
    PublishTransform, Reply, RustStream,
};
use ruststream::testing::{TestApp, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, Outgoing, OutgoingMessage, Publisher, RequestReply,
    Str, Subscribe, Subscriber, subscriber,
};
use ruststream_zeromq::testing::{
    ConnectedZmqTestBroker, Fanout, Queue, Rpc, ZmqTestBroker, ZmqTestSubscriber,
};
use ruststream_zeromq::{
    ZmqEndpoint, ZmqError, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpcPublish,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Long enough that a slow machine does not fail a delivery that is already queued.
const WAIT: Duration = Duration::from_secs(1);

/// Deliveries are enqueued before a publish resolves, so a message that has not arrived by now
/// was never routed here. Short enough to keep the negative cases quick.
const NOTHING: Duration = Duration::from_millis(100);

async fn queue() -> ConnectedZmqTestBroker<Queue> {
    ZmqTestBroker::queue()
        .connect()
        .await
        .expect("the queue stand connects")
}

async fn fanout() -> ConnectedZmqTestBroker<Fanout> {
    ZmqTestBroker::fanout()
        .connect()
        .await
        .expect("the fan-out stand connects")
}

async fn rpc() -> ConnectedZmqTestBroker<Rpc> {
    ZmqTestBroker::rpc()
        .connect()
        .await
        .expect("the responder stand connects")
}

/// Takes the next payload, or fails the test rather than hanging.
async fn next_payload(subscriber: &mut ZmqTestSubscriber) -> Vec<u8> {
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("a delivery arrives")
        .expect("the stream is open")
        .expect("the delivery is ok");
    message.payload().to_vec()
}

/// Fails if anything is waiting for this subscription.
async fn expect_idle(subscriber: &mut ZmqTestSubscriber, why: &str) {
    let mut stream = pin!(subscriber.stream());
    assert!(
        tokio::time::timeout(NOTHING, stream.next()).await.is_err(),
        "{why}",
    );
}

/// A publisher handed out before the shutdown aliases the transport and outlives it, so it must
/// report the closed connection rather than route into a dead broker - the aliasing half of the
/// broker contract, which the ladder cannot make a compile error. Every handle answers the same
/// way the real ones do, so a service can match on the error it would really see.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_after_shutdown_errors() {
    let queue_stand = queue().await;
    let queue_publisher = queue_stand.publisher();
    // Subscribing after the shutdown cannot be reached through the owner - the ladder consumed it -
    // so the aliased clone stands in for the handle a service kept.
    let alias = queue_stand.clone();
    queue_stand.shutdown().await.expect("the stand shuts down");

    let fanout_stand = fanout().await;
    let fanout_publisher = fanout_stand.publisher();
    fanout_stand.shutdown().await.expect("the stand shuts down");

    for (label, error) in [
        (
            "queue publish",
            queue_publisher
                .publish(OutgoingMessage::new("jobs", b"late".as_slice()), None)
                .await
                .expect_err("a queue publish after shutdown must fail"),
        ),
        (
            "fan-out publish",
            fanout_publisher
                .publish(OutgoingMessage::new("events", b"late".as_slice()), None)
                .await
                .expect_err("a fan-out publish after shutdown must fail"),
        ),
    ] {
        assert!(
            matches!(error, ZmqError::NotConnected),
            "{label} through a closed transport must report NotConnected, got: {error}",
        );
    }

    let subscribing = alias
        .subscribe("jobs")
        .await
        .expect_err("subscribing on a closed transport must fail");
    assert!(
        matches!(subscribing, ZmqError::NotConnected),
        "a subscription opened after shutdown must report NotConnected, got: {subscribing}",
    );
}

/// The responder's two directions answer the same way, and a reply address minted while the
/// transport was live keeps the refusal about the shutdown rather than about the destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answering_after_shutdown_errors() {
    let broker = rpc().await;
    let responder_publisher = broker.publisher();
    let reply_to = {
        let mut responder = broker.subscribe("echo").await.expect("the responder");
        let asking = broker.publisher();
        let request = tokio::spawn(async move {
            let _ = asking
                .request(OutgoingMessage::new("echo", b"ping".as_slice()), NOTHING)
                .await;
        });
        let address = {
            let mut stream = pin!(responder.stream());
            let message = tokio::time::timeout(WAIT, stream.next())
                .await
                .expect("the request arrives")
                .expect("the stream is open")
                .expect("the delivery is ok");
            message
                .headers()
                .reply_to()
                .expect("a request carries the address to answer")
                .to_owned()
        };
        // The requester gives up on its own timeout; nothing answers it, which is not this test's
        // subject.
        request.await.expect("the request task joins");
        address
    };

    broker.shutdown().await.expect("the stand shuts down");

    for (label, error) in [
        (
            "reply publish",
            responder_publisher
                .publish(
                    OutgoingMessage::new(reply_to.as_str(), b"late".as_slice()),
                    None,
                )
                .await
                .expect_err("a reply after shutdown must fail"),
        ),
        (
            "request",
            responder_publisher
                .request(OutgoingMessage::new("echo", b"late".as_slice()), NOTHING)
                .await
                .expect_err("a request after shutdown must fail"),
        ),
    ] {
        assert!(
            matches!(error, ZmqError::NotConnected),
            "{label} through a closed transport must report NotConnected, got: {error}",
        );
    }
}

/// PUSH hands each message to one of the peers connected to it, so a second worker dialing the
/// ventilator spreads the load instead of doubling the work. The stand-in picks in subscription
/// order, and an injection is the ventilator's push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_hands_each_message_to_one_consumer_in_turn() {
    let broker = ZmqTestBroker::queue()
        .dialing()
        .connect()
        .await
        .expect("the queue stand connects");
    let mut first = broker.subscribe("jobs").await.expect("the first worker");
    let mut second = broker.subscribe("jobs").await.expect("the second worker");

    for payload in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        broker.inject(OutgoingMessage::new("jobs", payload));
    }

    assert_eq!(next_payload(&mut first).await, b"one");
    assert_eq!(next_payload(&mut second).await, b"two");
    assert_eq!(next_payload(&mut first).await, b"three");
    expect_idle(
        &mut second,
        "a pushed message goes to one consumer, so the second worker must not see `three` too",
    )
    .await;

    broker.shutdown().await.expect("the broker shuts down");
}

/// PUB/SUB filters by name prefix on the publisher side: a message from the PUB peer the
/// watchers dial reaches every subscription whose name prefixes it, and none other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_peer_reaches_every_prefix_match_and_nothing_else() {
    let broker = ZmqTestBroker::fanout()
        .dialing()
        .connect()
        .await
        .expect("the fan-out stand connects");
    let mut eu = broker.subscribe("orders.eu").await.expect("the eu watcher");
    let mut all = broker.subscribe("orders").await.expect("the broad watcher");
    let mut other = broker.subscribe("shipments").await.expect("the outsider");

    broker.inject(OutgoingMessage::new("orders.eu.1", b"kept".as_slice()));

    assert_eq!(next_payload(&mut eu).await, b"kept");
    assert_eq!(next_payload(&mut all).await, b"kept");
    expect_idle(
        &mut other,
        "a fan-out subscription only receives names its own name prefixes",
    )
    .await;

    broker.shutdown().await.expect("the broker shuts down");
}

/// The fan-out's own publisher keeps the same filter, and drops what no subscription matches
/// rather than failing the publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fanout_publisher_keeps_the_prefix_and_drops_the_rest() {
    let broker = fanout().await;
    let mut all = broker.subscribe("orders").await.expect("the watcher");
    let publisher = broker.publisher();

    publisher
        .publish(
            OutgoingMessage::new("orders.eu.1", b"kept".as_slice()),
            None,
        )
        .await
        .expect("the publish succeeds");
    assert_eq!(next_payload(&mut all).await, b"kept");

    publisher
        .publish(
            OutgoingMessage::new("unheard.1", b"dropped".as_slice()),
            None,
        )
        .await
        .expect("an unmatched fan-out publish is not an error");
    expect_idle(&mut all, "an unmatched message reaches no subscription").await;

    broker.shutdown().await.expect("the broker shuts down");
}

/// The reply publisher answers the address a request carried, and refuses anything else - the
/// socket publisher's rule, so a responder mounted without a reply-routing transform fails here
/// as it would fail on deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reply_publisher_refuses_a_destination_that_is_not_a_reply_address() {
    let broker = rpc().await;

    let err = broker
        .publisher()
        .publish(OutgoingMessage::new("reply", b"answer".as_slice()), None)
        .await
        .expect_err("a literal destination is not a reply address");

    assert!(
        matches!(&err, ZmqError::Send { name, .. } if name == "reply"),
        "the error must name the destination it refused, got: {err}",
    );

    broker.shutdown().await.expect("the broker shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_times_out_when_nothing_answers() {
    let broker = rpc().await;

    let err = broker
        .publisher()
        .request(
            OutgoingMessage::new("nobody.home", b"ping".as_slice()),
            NOTHING,
        )
        .await
        .expect_err("a request nobody answers must fail once its timeout elapses");

    assert!(
        matches!(err, ZmqError::RequestTimeout),
        "an unanswered request must report the timeout, got: {err}",
    );

    broker.shutdown().await.expect("the broker shuts down");
}

/// A DEALER drops what does not correlate, so an answer that loses the id leaves the request
/// waiting. Reproducing that keeps a responder that forgets to echo the id from passing here and
/// hanging in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_drops_the_correlation_id_never_resolves_the_request() {
    let broker = rpc().await;
    let mut responder = broker.subscribe("echo").await.expect("the responder");
    let replies = broker.publisher();

    let answering = tokio::spawn(async move {
        let mut stream = pin!(responder.stream());
        let request = tokio::time::timeout(WAIT, stream.next())
            .await
            .expect("the request arrives")
            .expect("the stream is open")
            .expect("the delivery is ok");
        let reply_to = request
            .headers()
            .reply_to()
            .expect("a request carries the address to answer")
            .to_owned();
        // Deliberately no correlation-id: the answer is addressed but unmatched.
        replies
            .publish(
                OutgoingMessage::new(reply_to.as_str(), b"pong".as_slice()),
                None,
            )
            .await
            .expect("the reply is routed to the address the request carried");
    });

    let err = broker
        .publisher()
        .request(OutgoingMessage::new("echo", b"ping".as_slice()), NOTHING)
        .await
        .expect_err("an uncorrelated answer must not resolve the request");
    assert!(
        matches!(err, ZmqError::RequestTimeout),
        "the request must time out rather than take the wrong answer, got: {err}",
    );

    answering.await.expect("the responder task joins");
    broker.shutdown().await.expect("the broker shuts down");
}

#[derive(Debug, Deserialize, Serialize)]
struct Job {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Done {
    id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Note {
    id: u64,
}

#[subscriber("jobs", publish("results"))]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[subscriber("results")]
async fn first_worker(done: &Done) -> HandlerOutcome {
    let _ = done.id;
    HandlerOutcome::ack()
}

#[subscriber("results")]
async fn second_worker(done: &Done) -> HandlerOutcome {
    let _ = done.id;
    HandlerOutcome::ack()
}

/// The production queue policy pairs against the stand-in, so this mount site is the one a
/// service ships, and the result reaches the subscription that holds the results queue.
///
/// The results travel on a queue of their own. On PUSH/PULL a name is frame 0 rather than an
/// address, so a result published on the queue the `jobs` subscription holds would come back to
/// that subscription; the stand refuses it there, as the socket does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_policy_mounts_and_a_result_is_worked_once() {
    let results = ZmqTestBroker::queue().bindable();
    let to_results = results.bind(ZmqQueuePublish);
    let app = RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled("jobs", ZmqTestBroker::queue(), |b| {
            b.include(work).out(Reply, to_results);
        })
        .with_broker_labeled("results", results, |b| {
            b.include(first_worker);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker_named("jobs")
        .publish("jobs", &Job { id: 7 })
        .await
        .expect("the injection drives the reaction to a standstill");

    tb.broker_named("results")
        .subscriber("results")
        .assert_called_once();

    tb.shutdown().await.expect("the app shuts down");
}

/// Two workers that dial one ventilator compete for its pushes, so a result is worked once, by
/// one of them. Each mount names where its retry copies go, because the peer they read from
/// takes nothing back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_workers_dialing_one_peer_work_a_result_once() {
    let retries = ZmqTestBroker::queue().bindable();
    let first_copies = retries.bind(ZmqQueuePublish);
    let second_copies = retries.bind(ZmqQueuePublish);
    let app = RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled("results", ZmqTestBroker::queue().dialing(), |b| {
            b.include(first_worker)
                .out_retry(first_copies)
                .to("results");
            b.include(second_worker)
                .out_retry(second_copies)
                .to("results");
        })
        .with_broker_labeled("retries", retries, |_b| {});
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker_named("results")
        .publish("results", &Done { id: 3 })
        .await
        .expect("the injection drives the reaction to a standstill");

    tb.broker_named("results")
        .subscriber("results")
        .assert_called_once();

    tb.shutdown().await.expect("the app shuts down");
}

#[subscriber("events", publish("audit.high"))]
async fn watch(event: &Event) -> Note {
    Note { id: event.id }
}

#[subscriber("audit")]
async fn auditor(note: &Note) -> HandlerOutcome {
    let _ = note.id;
    HandlerOutcome::ack()
}

/// A mount that names no reply policy takes its own pattern's default, so a fan-out reply leaves
/// through the fan-out.
///
/// A stand that answered the queue's default instead would refuse this reply, because a queue
/// sends only under the name of the subscription holding it, and the mount would only find that
/// out on deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_mount_that_names_no_reply_policy_still_fans_out() {
    let app = RustStream::new(AppInfo::new("watcher", "0.1.0")).with_broker(
        ZmqTestBroker::fanout(),
        |b| {
            b.include(watch);
        },
    );
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.publish("events", &Event { id: 5 })
        .await
        .expect("the injection drives the reaction to a standstill");

    tb.broker::<ZmqTestBroker<Fanout>>()
        .published::<Note>("audit.high")
        .assert_called_once();

    tb.shutdown().await.expect("the app shuts down");
}

/// The production fan-out policy pairs against the stand-in and keeps the pattern's filter: a
/// subscription on `audit` receives what was published to `audit.high`. The audit fan-out is an
/// endpoint of its own, bound by the subscription that reads it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fanout_policy_mounts_and_the_prefix_subscription_receives() {
    let audit = ZmqTestBroker::fanout().bindable();
    let to_audit = audit.bind(ZmqFanoutPublish);
    let app = RustStream::new(AppInfo::new("watcher", "0.1.0"))
        .with_broker_labeled("events", ZmqTestBroker::fanout(), |b| {
            b.include(watch).out(Reply, to_audit);
        })
        .with_broker_labeled("audit", audit, |b| {
            b.include(auditor);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker_named("events")
        .publish("events", &Event { id: 3 })
        .await
        .expect("the injection drives the reaction to a standstill");

    tb.broker_named("audit")
        .subscriber("audit")
        .assert_called_once();

    tb.shutdown().await.expect("the app shuts down");
}

#[subscriber("reports")]
async fn file_report(done: &Done) -> HandlerOutcome {
    let _ = done.id;
    HandlerOutcome::ack()
}

/// A bound endpoint belongs to the subscription that bound it, so a second registration on the
/// same broker is refused at startup, on the socket and on the stand in the same words. A stand
/// that started it would pass a routes file whose second subscription never receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_subscription_on_a_bound_endpoint_is_refused_on_the_stand_as_on_the_socket() {
    let socket = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(first_worker);
            b.include(file_report);
        },
    );
    let stand =
        RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(ZmqTestBroker::queue(), |b| {
            b.include(first_worker);
            b.include(file_report);
        });

    let on_socket = socket
        .start()
        .await
        .expect_err("the second subscription on the bound endpoint must not start")
        .to_string();
    let on_stand = stand
        .start()
        .await
        .expect_err("the stand must refuse what the socket refuses")
        .to_string();

    assert!(
        on_socket.contains("'results'") && on_socket.contains("'reports'"),
        "the refusal must name both subscriptions, got: {on_socket}",
    );
    assert_eq!(on_socket, on_stand);
}

#[derive(Debug, Deserialize, Serialize)]
struct Greeting {
    who: String,
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Answer {
    text: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct AskFor {
    who: String,
}

/// The example's transform, verbatim: the reply goes to the address the request carried, under
/// the id it was asked with. It writes no per-message setting, so it is generic over the options
/// type.
struct ReplyToRequester;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(reply_to) = cx.headers().get_shared("reply-to")
            && let Ok(reply_to) = Str::try_from(reply_to)
        {
            out.set_name(reply_to);
        }
        if let Some(correlation) = cx.headers().get_shared("correlation-id") {
            out.headers_mut()
                .insert(Str::from_static("correlation-id"), correlation);
        }
    }
}

#[subscriber("greeter", publish("reply"))]
async fn greet(request: &Greeting) -> Answer {
    Answer {
        text: format!("hello {}", request.who),
    }
}

/// Carries what the requesting handler heard back to the test.
static ANSWERS: OnceLock<mpsc::UnboundedSender<String>> = OnceLock::new();

/// The handler that binds the capability: it asks, and it is mountable only on a policy whose
/// live publisher answers requests.
#[subscriber("asks")]
async fn ask(request: &AskFor, Out(rpc): Out<impl RequestReply>) -> HandlerOutcome {
    let Ok(encoded) = JsonCodec.encode(&Greeting {
        who: request.who.clone(),
    }) else {
        return HandlerOutcome::drop();
    };
    let Ok(reply) = rpc
        .request(
            OutgoingMessage::new("greeter", encoded.as_ref()),
            Duration::from_secs(5),
        )
        .await
    else {
        return HandlerOutcome::drop();
    };
    let Ok(answer) = JsonCodec.decode::<Answer>(reply.payload()) else {
        return HandlerOutcome::drop();
    };
    ANSWERS
        .get()
        .expect("the test installs the sender before the app starts")
        .send(answer.text)
        .expect("the test holds the receiver");
    HandlerOutcome::ack()
}

/// The whole request-reply wiring under the harness: a handler binding `Out<impl RequestReply>`
/// mounts on the stand-in, its request reaches the responder, and the responder's answer comes
/// back correlated to the caller that asked.
///
/// The asks arrive on a queue of their own: the responder binds the exchange's endpoint, and a
/// bound endpoint serves the one subscription that bound it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_reply_pair_runs_under_the_harness() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    ANSWERS.set(tx).expect("one request-reply app per binary");

    let exchange = ZmqTestBroker::rpc().bindable();
    let requests = exchange.bind(ZmqRpcPublish);
    let app = RustStream::new(AppInfo::new("greeter", "0.1.0"))
        .with_broker_labeled("exchange", exchange, |b| {
            // A responder addresses no retry copies, so every mount on it names where they go -
            // the same line the deployment writes, and the stand asks for it because the socket
            // does.
            b.include(greet)
                .out(Reply, ZmqRpcPublish)
                .transform(ReplyToRequester)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        })
        .with_broker_labeled("asks", ZmqTestBroker::queue(), |b| {
            b.include(ask).out(DefaultSlot, requests).build();
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker_named("asks")
        .publish(
            "asks",
            &AskFor {
                who: "world".to_owned(),
            },
        )
        .await
        .expect("the injection drives the exchange to a standstill");

    let answer = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("the requesting handler received its answer")
        .expect("the app holds the sender");
    assert_eq!(answer, "hello world");

    tb.shutdown().await.expect("the app shuts down");
}

/// A foreign PUSH peer hands each message to one of the sockets that dialed it, whatever their
/// names, so an injection on a dialing queue stand reaches the differently named workers in turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injection_reaches_the_dialing_workers_whatever_their_names() {
    let stand = ZmqTestBroker::queue()
        .dialing()
        .connect()
        .await
        .expect("the stand connects");
    let mut billing = stand.subscribe("billing").await.expect("opens");
    let mut shipping = stand.subscribe("shipping").await.expect("opens");
    for payload in [b"one".as_slice(), b"two"] {
        TestableBroker::inject(&stand, OutgoingMessage::new("jobs", payload));
    }
    for worker in [&mut billing, &mut shipping] {
        let mut stream = pin!(worker.stream());
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("each worker takes one job")
            .expect("stream is open")
            .expect("delivery is ok");
    }
}
