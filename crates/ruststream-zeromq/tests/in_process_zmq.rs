//! The in-process mode answering the way the sockets do, on the production brokers connected with
//! `connect_in_process`.
//!
//! The subject is the transport, so these drive the connected forms directly; a service's tests
//! drive its app through `TestApp` (see `harness_zmq.rs`). Where the sockets refuse something, the
//! refusal is taken from a socket in the same test and compared word for word, so an in-process
//! answer cannot drift from the one a deployment gives.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, RequestReply,
    Subscribe, Subscriber,
};
use ruststream_zeromq::{
    Connect, ConnectedZmqFanout, ConnectedZmqQueue, ConnectedZmqRpc, ZmqEndpoint, ZmqError,
    ZmqFanout, ZmqQueue, ZmqRpc,
};

/// Long enough that a slow machine does not fail a delivery that is already queued.
const WAIT: Duration = Duration::from_secs(1);

/// Deliveries are enqueued before a publish resolves, so a message that has not arrived by now
/// was never routed here. Short enough to keep the negative cases quick.
const NOTHING: Duration = Duration::from_millis(100);

/// Where this service binds: an ephemeral loopback port the in-process transport never opens.
const LOOPBACK: &str = "tcp://127.0.0.1:0";

/// Where this service dials a peer; nothing in process reaches it.
const PEER: &str = "tcp://127.0.0.1:5555";

async fn bound_queue() -> ConnectedZmqQueue {
    ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))
        .connect_in_process()
        .await
        .expect("the queue connects in process")
}

async fn bound_fanout() -> ConnectedZmqFanout {
    ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))
        .connect_in_process()
        .await
        .expect("the fan-out connects in process")
}

async fn bound_rpc() -> ConnectedZmqRpc {
    ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK))
        .connect_in_process()
        .await
        .expect("the exchange connects in process")
}

/// Takes the next delivery, or fails the test rather than hanging.
async fn next<S: Subscriber>(subscriber: &mut S) -> S::Message {
    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("a delivery arrives")
        .expect("the stream is open");
    let Ok(delivery) = delivery else {
        panic!("the delivery is not well formed");
    };
    delivery
}

async fn next_payload<S: Subscriber>(subscriber: &mut S) -> Vec<u8> {
    next(subscriber).await.payload().to_vec()
}

/// Fails if anything is waiting for this subscription.
async fn expect_idle<S: Subscriber>(subscriber: &mut S, why: &str) {
    let mut stream = pin!(subscriber.stream());
    assert!(
        tokio::time::timeout(NOTHING, stream.next()).await.is_err(),
        "{why}",
    );
}

/// A configuration `connect` refuses is refused in process too: the transition validates the
/// endpoint the same way before it builds anything.
#[tokio::test]
async fn an_endpoint_connect_refuses_is_refused_in_process() {
    let on_socket = ZmqQueue::new(ZmqEndpoint::bind("inproc://jobs"))
        .connect()
        .await
        .expect_err("inproc is not a transport this crate serves")
        .to_string();
    let in_process = ZmqQueue::new(ZmqEndpoint::bind("inproc://jobs"))
        .connect_in_process()
        .await
        .expect_err("the in-process mode refuses what connect refuses")
        .to_string();
    assert_eq!(in_process, on_socket);
}

/// A publisher handed out before the shutdown aliases the transport and outlives it, so it must
/// report the closed connection rather than route into a dead broker - the aliasing half of the
/// broker contract, which the ladder cannot make a compile error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_after_shutdown_errors() {
    let queue = bound_queue().await;
    let queue_publisher = queue.publisher();
    let fanout = bound_fanout().await;
    let fanout_publisher = fanout.publisher();
    // A publisher built before `connect` shares the broker's state, the handle a service keeps
    // in its own state.
    let early = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK));
    let early_publisher = early.publisher();
    let early = early.connect_in_process().await.expect("connects");
    queue.shutdown().await.expect("the queue shuts down");
    fanout.shutdown().await.expect("the fan-out shuts down");
    early.shutdown().await.expect("the queue shuts down");

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
        (
            "early publish",
            early_publisher
                .publish(OutgoingMessage::new("jobs", b"late".as_slice()), None)
                .await
                .expect_err("a publisher built before connect fails after shutdown too"),
        ),
    ] {
        assert!(
            matches!(error, ZmqError::NotConnected),
            "{label} through a closed transport must report NotConnected, got: {error}",
        );
    }
}

/// The responder's two directions answer the same way, and a reply address minted while the
/// transport was live keeps the refusal about the shutdown rather than about the destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answering_after_shutdown_errors() {
    let broker = bound_rpc().await;
    let publisher = broker.publisher();
    let mut responder = broker.subscribe("echo").await.expect("the responder");
    let asking = broker.publisher();
    let request = tokio::spawn(async move {
        let _ = asking
            .request(OutgoingMessage::new("echo", b"ping".as_slice()), NOTHING)
            .await;
    });
    let reply_to = next(&mut responder)
        .await
        .headers()
        .reply_to()
        .expect("a request carries the address to answer")
        .to_owned();
    // The requester gives up on its own timeout; nothing answers it, which is not this test's
    // subject.
    request.await.expect("the request task joins");

    broker.shutdown().await.expect("the exchange shuts down");

    for (label, error) in [
        (
            "reply publish",
            publisher
                .publish(
                    OutgoingMessage::new(reply_to.as_str(), b"late".as_slice()),
                    None,
                )
                .await
                .expect_err("a reply after shutdown must fail"),
        ),
        (
            "request",
            publisher
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

/// The PULL socket a subscription binds takes whatever a peer pushes into it: the name frame is
/// data, not an address, so a message named anything reaches the subscription and says what it
/// was named.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_queue_takes_whatever_a_peer_pushes_whatever_its_name() {
    let broker = bound_queue().await;
    let mut jobs = broker.subscribe("jobs").await.expect("the subscription");

    broker.inject(OutgoingMessage::new("reports", b"pushed".as_slice()));

    let delivery = next(&mut jobs).await;
    assert_eq!(delivery.name(), "reports");
    assert_eq!(delivery.payload(), b"pushed");
}

/// A bound endpoint is held while its subscription is open and free again once it closes, as a
/// socket's listener is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_endpoint_is_free_again_once_its_subscription_closes() {
    let broker = bound_queue().await;
    let first = broker.subscribe("jobs").await.expect("the first binds");
    assert!(
        broker.subscribe("reports").await.is_err(),
        "the endpoint is held while the first subscription is open"
    );
    drop(first);
    broker
        .subscribe("reports")
        .await
        .expect("the endpoint is free again");
}

/// PUSH hands each message to one of the peers connected to it, whatever the message's name, so
/// a second worker dialing the ventilator spreads the load instead of doubling the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ventilator_hands_each_message_to_one_worker_in_turn() {
    let broker = ZmqQueue::new(ZmqEndpoint::connect(PEER))
        .connect_in_process()
        .await
        .expect("the queue connects in process");
    let mut first = broker.subscribe("jobs").await.expect("the first worker");
    let mut second = broker
        .subscribe("reports")
        .await
        .expect("the second worker");

    for payload in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        broker.inject(OutgoingMessage::new("jobs", payload));
    }

    assert_eq!(next_payload(&mut first).await, b"one");
    assert_eq!(next_payload(&mut second).await, b"two");
    assert_eq!(next_payload(&mut first).await, b"three");
    expect_idle(
        &mut second,
        "a pushed message goes to one worker, so the second must not see `three` too",
    )
    .await;
}

/// PUB/SUB filters by name prefix: a message from the PUB peer the watchers dial reaches every
/// subscription whose name prefixes it, and none other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_peer_reaches_every_prefix_match_and_nothing_else() {
    let broker = ZmqFanout::new(ZmqEndpoint::connect(PEER))
        .connect_in_process()
        .await
        .expect("the fan-out connects in process");
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
}

/// The fan-out's own publisher dials the listener its subscription bound and keeps the same
/// filter, and drops what the subscription does not match rather than failing the publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fanout_publisher_keeps_the_prefix_and_drops_the_rest() {
    let broker = bound_fanout().await;
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
}

/// The header frame is text, so a value that is not UTF-8 has no representation on the wire. The
/// in-process publisher frames a message the way the socket publisher does, so it refuses the
/// value in the same words rather than carrying it to a handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_the_wire_cannot_carry_is_refused_as_on_the_socket() {
    let mut headers = HeaderMap::new();
    headers.insert("x-binary", [0xff, 0xfe].as_slice());
    let message = || OutgoingMessage::new("jobs", b"{}".as_slice()).with_headers(headers.clone());

    let socket = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the queue connects");
    let _jobs = socket
        .subscribe("jobs")
        .await
        .expect("the subscription binds");
    let on_socket = socket
        .publisher()
        .publish(message(), None)
        .await
        .expect_err("the socket cannot frame the value")
        .to_string();

    let in_process = bound_queue().await;
    let mut jobs = in_process
        .subscribe("jobs")
        .await
        .expect("the subscription");
    let refused = in_process
        .publisher()
        .publish(message(), None)
        .await
        .expect_err("the in-process transport cannot frame it either")
        .to_string();

    assert!(
        on_socket.contains("x-binary"),
        "names the header: {on_socket}"
    );
    assert_eq!(refused, on_socket);
    expect_idle(&mut jobs, "a refused publish reaches nobody").await;
    socket.shutdown().await.expect("the queue shuts down");
}

/// The reply publisher answers the address a request carried, and refuses anything else - the
/// socket publisher's rule, so a responder mounted without a reply-routing transform fails here
/// as it would fail on deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reply_publisher_refuses_what_it_cannot_route() {
    let broker = bound_rpc().await;
    let publisher = broker.publisher();

    let plain = publisher
        .publish(OutgoingMessage::new("reply", b"answer".as_slice()), None)
        .await
        .expect_err("a literal destination is not a reply address");
    assert!(
        matches!(&plain, ZmqError::Send { name, .. } if name == "reply"),
        "the error must name the destination it refused, got: {plain}",
    );

    let detached = publisher
        .publish(OutgoingMessage::new("zmq-reply:00", b"{}".as_slice()), None)
        .await
        .expect_err("nothing can be routed before a responder attaches")
        .to_string();
    assert!(
        detached.contains("no responder subscription is attached"),
        "the refusal must name the missing responder, got: {detached}",
    );
}

/// An answer to a peer no request of this service is waiting at is refused in the ROUTER's own
/// words, taken here from a real ROUTER: a responder learns its answer reached nobody.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_to_a_peer_that_is_gone_is_refused_as_on_the_socket() {
    let answer = || OutgoingMessage::new("zmq-reply:deadbeef", b"{}".as_slice());

    let socket = ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the exchange connects");
    let _responder = socket.subscribe("greeter").await.expect("the responder");
    let on_socket = socket
        .publisher()
        .publish(answer(), None)
        .await
        .expect_err("no peer carries this identity")
        .to_string();

    let in_process = bound_rpc().await;
    let _responder = in_process
        .subscribe("greeter")
        .await
        .expect("the responder");
    let refused = in_process
        .publisher()
        .publish(answer(), None)
        .await
        .expect_err("no peer carries this identity in process either")
        .to_string();

    assert_eq!(refused, on_socket);
    socket.shutdown().await.expect("the exchange shuts down");
}

/// A request a foreign peer sends arrives stamped with that peer, and an answer to the stamp is
/// taken: the peer stays connected, as a DEALER that asked does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peers_request_is_stamped_and_its_answer_taken() {
    let broker = bound_rpc().await;
    let mut responder = broker.subscribe("greeter").await.expect("the responder");

    // A peer that writes its own `reply-to` does not choose where the answer goes: the ROUTER
    // stamps the identity it received the request from over it.
    let mut claimed = HeaderMap::new();
    claimed.insert("reply-to", "zmq-reply:ffff");
    broker.inject(OutgoingMessage::new("greeter", b"ping".as_slice()).with_headers(claimed));

    let request = next(&mut responder).await;
    let reply_to = request
        .headers()
        .reply_to()
        .expect("the ROUTER stamps the peer")
        .to_owned();
    assert!(reply_to.starts_with("zmq-reply:"), "stamped: {reply_to}");
    assert_ne!(
        reply_to, "zmq-reply:ffff",
        "the stamp replaces what the peer wrote"
    );

    broker
        .publisher()
        .publish(
            OutgoingMessage::new(reply_to.as_str(), b"pong".as_slice()),
            None,
        )
        .await
        .expect("the peer that asked is still connected");
    assert_eq!(broker.published(&reply_to).len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_times_out_when_nothing_answers() {
    let err = bound_rpc()
        .await
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
}

/// A request of a service whose responders dial the endpoint goes to the peer there, over a
/// DEALER that dials it too, and never to the service's own responder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_on_a_dialed_exchange_goes_to_the_peer_not_the_own_responder() {
    let broker = ZmqRpc::new(ZmqEndpoint::connect(PEER))
        .connect_in_process()
        .await
        .expect("the exchange connects in process");
    let mut responder = broker.subscribe("greeter").await.expect("the responder");

    let err = broker
        .publisher()
        .request(OutgoingMessage::new("greeter", b"ping".as_slice()), NOTHING)
        .await
        .expect_err("the peer at the endpoint lives in another process");
    assert!(matches!(err, ZmqError::RequestTimeout), "got: {err}");
    expect_idle(
        &mut responder,
        "the request left for the peer, so the service's own responder must not receive it",
    )
    .await;
}

/// A DEALER drops what does not correlate, so an answer that loses the id leaves the request
/// waiting. Reproducing that keeps a responder that forgets to echo the id from passing here and
/// hanging in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_drops_the_correlation_id_never_resolves_the_request() {
    let broker = bound_rpc().await;
    let mut responder = broker.subscribe("echo").await.expect("the responder");
    let replies = broker.publisher();

    let answering = tokio::spawn(async move {
        let request = next(&mut responder).await;
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
}

/// The side is part of the broker's type, and the in-process transport reads it off the
/// production broker: a worker that dials refuses a publish once it has dialed, in the words the
/// socket publisher uses (compared with a socket's in `redelivery.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialing_worker_refuses_to_publish_to_the_peer_it_reads_from() {
    let broker: ConnectedZmqQueue<Connect> = ZmqQueue::new(ZmqEndpoint::connect(PEER))
        .connect_in_process()
        .await
        .expect("the queue connects in process");
    let _worker = broker.subscribe("jobs").await.expect("the worker dials");

    let err = broker
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"copy".as_slice()), None)
        .await
        .expect_err("the ventilator takes nothing")
        .to_string();
    assert!(
        err.contains("'jobs'") && err.contains("PUSH"),
        "the refusal names the subscription and the peer, got: {err}",
    );
}

/// Which subscriptions a publish of this service reaches, as a live `TestApp` asks it to know what
/// a settle waits for: the pattern's own rule, read off the side the broker's type names.
#[tokio::test]
async fn the_routes_are_the_patterns_own_rule() {
    let subscriptions = ["jobs"];
    assert_eq!(
        bound_queue().await.routes("anything", &subscriptions),
        [0],
        "a subscription that binds a queue takes whatever is pushed to it, whatever its name",
    );
    let dialing = ZmqQueue::new(ZmqEndpoint::connect(PEER))
        .connect_in_process()
        .await
        .expect("connects");
    assert!(
        dialing.routes("jobs", &subscriptions).is_empty(),
        "a publish on an endpoint the service dials leaves for the peer there",
    );

    let fanout = bound_fanout().await;
    assert_eq!(fanout.routes("orders.eu", &["orders"]), [0]);
    assert!(fanout.routes("shipments", &["orders"]).is_empty());

    let rpc = bound_rpc().await;
    assert_eq!(rpc.routes("greeter", &["greeter"]), [0]);
    assert!(
        rpc.routes("zmq-reply:00", &["greeter"]).is_empty(),
        "an answer goes to the peer that asked, not to a subscription",
    );
}
