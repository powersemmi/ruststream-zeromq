//! A publisher of this service on an endpoint a subscription of the same broker binds, used in the
//! order a starting or stopping service uses it.
//!
//! The runtime opens subscriptions one at a time and runs each handler as soon as its subscription
//! opens, so a handler mounted first can publish into a queue whose subscription opens later (a
//! worker that dead-letters into a queue of its own service, fed by a ventilator that is already
//! pushing). The test harness starts every subscription before it injects anything, so it cannot
//! reach that order; the subject is the transport, so these drive the connected forms, live over
//! the loopback and in process, and hold the two to the same answers.

#![cfg(feature = "testing")]

use std::net::TcpListener;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{
    Broker, BytesMut, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscribe,
    Subscriber,
};
use ruststream_zeromq::{ConnectedZmqFanout, ConnectedZmqQueue, ZmqEndpoint, ZmqFanout, ZmqQueue};

/// Long enough that a slow machine does not fail a delivery that is already on its way.
const WAIT: Duration = Duration::from_secs(10);

/// A loopback address with a port the operating system just handed out and took back, so a
/// fixed-port endpoint does not collide with another test.
fn fixed_port() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free loopback port");
    let port = listener.local_addr().expect("the port").port();
    format!("tcp://127.0.0.1:{port}")
}

async fn live(address: &str) -> ConnectedZmqQueue {
    ZmqQueue::new(ZmqEndpoint::bind(address))
        .connect()
        .await
        .expect("the queue connects")
}

async fn in_process(address: &str) -> ConnectedZmqQueue {
    ZmqQueue::new(ZmqEndpoint::bind(address))
        .connect_in_process()
        .await
        .expect("the queue connects in process")
}

fn job(payload: &'static [u8]) -> OutgoingMessage<'static, BytesMut> {
    OutgoingMessage::produced("jobs", BytesMut::from(payload))
}

/// A publish made before the subscription that binds the endpoint opens does not keep the
/// publisher away from it: the subscription opens, and what is published afterwards reaches it.
async fn a_subscription_that_opens_after_a_publish_receives_what_follows(
    connected: ConnectedZmqQueue,
) {
    let publisher = connected.publisher();
    // Its outcome is not the subject: nothing in this service reads the endpoint yet.
    let _ = publisher.publish(job(b"early"), None).await;

    let mut subscriber = connected
        .subscribe("jobs")
        .await
        .expect("the subscription binds the endpoint");
    publisher
        .publish(job(b"late"), None)
        .await
        .expect("the subscription now reads the endpoint");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the publish reaches the subscription")
        .expect("the stream is open")
        .expect("the message reads");
    assert_eq!(message.payload(), b"late");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_opens_after_a_publish_receives_what_follows_on_an_ephemeral_port() {
    a_subscription_that_opens_after_a_publish_receives_what_follows(
        live("tcp://127.0.0.1:0").await,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_opens_after_a_publish_receives_what_follows_on_a_fixed_port() {
    a_subscription_that_opens_after_a_publish_receives_what_follows(live(&fixed_port()).await)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_opens_after_a_publish_receives_what_follows_in_process() {
    a_subscription_that_opens_after_a_publish_receives_what_follows(
        in_process("tcp://127.0.0.1:0").await,
    )
    .await;
}

async fn live_fanout(address: &str) -> ConnectedZmqFanout {
    ZmqFanout::new(ZmqEndpoint::bind(address))
        .connect()
        .await
        .expect("the fan-out connects")
}

async fn in_process_fanout(address: &str) -> ConnectedZmqFanout {
    ZmqFanout::new(ZmqEndpoint::bind(address))
        .connect_in_process()
        .await
        .expect("the fan-out connects in process")
}

fn order(payload: &'static [u8]) -> OutgoingMessage<'static, BytesMut> {
    OutgoingMessage::produced("orders", BytesMut::from(payload))
}

/// The same on the fan-out: a subscription that binds the endpoint after the first publish
/// receives what is published afterwards.
async fn a_fanout_subscription_that_opens_after_a_publish_receives_what_follows(
    connected: ConnectedZmqFanout,
) {
    let publisher = connected.publisher();
    // A message no subscriber matches is dropped, as the pattern drops it.
    let _ = publisher.publish(order(b"early"), None).await;

    let mut subscriber = connected
        .subscribe("orders")
        .await
        .expect("the subscription binds the endpoint");
    publisher
        .publish(order(b"late"), None)
        .await
        .expect("the subscription now reads the endpoint");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the publish reaches the subscription")
        .expect("the stream is open")
        .expect("the message reads");
    assert_eq!(message.payload(), b"late");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fanout_subscription_that_opens_after_a_publish_receives_what_follows_on_a_fixed_port() {
    a_fanout_subscription_that_opens_after_a_publish_receives_what_follows(
        live_fanout(&fixed_port()).await,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fanout_subscription_that_opens_after_a_publish_receives_what_follows_in_process() {
    a_fanout_subscription_that_opens_after_a_publish_receives_what_follows(
        in_process_fanout("tcp://127.0.0.1:0").await,
    )
    .await;
}
