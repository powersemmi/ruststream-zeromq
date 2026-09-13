//! Conformance: the routing suite against the in-process transport, plus the lifecycle and
//! request/reply suites over real sockets on the loopback - no external broker exists to
//! need, which is the point of this crate.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_zeromq::testing::ZmqTestBroker;
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue, ZmqRpc};

/// Every stand answers the routing contract, so a pattern cannot drift from it while the crate
/// only ever tested one of the three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_stand_passes_the_conformance_suite() {
    harness::run_suite(ZmqTestBroker::queue).await;
    harness::run_suite(ZmqTestBroker::fanout).await;
    harness::run_suite(ZmqTestBroker::rpc).await;
}

/// A publish under the subscribe name has to come back on the subscription that reported it, on
/// the two stands that declare they address their own copies.
///
/// The real PUB/SUB socket is left out of this suite - it drops what it sends before the
/// subscriber's filter has propagated - but its stand has no filter table and no slow joiner, so
/// the promise is checked here.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_addressed_stands_pass_redelivery_address() {
    harness::redelivery_address(
        ZmqTestBroker::queue,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
    harness::redelivery_address(
        ZmqTestBroker::fanout,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The responder stand answers the same request-reply contract the sockets do, the leg where
/// nobody answers included, so a handler that binds the capability is testable in process.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_responder_stand_passes_the_request_reply_suite() {
    capabilities::request_reply(
        ZmqTestBroker::rpc,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    )
    .await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_lifecycle() {
    // The subscription binds an ephemeral loopback port; the publisher dials the resolved
    // address (the loopback arrangement).
    harness::lifecycle(
        || ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// A queue addresses its own subscription, and this is where that promise is held: a publish to
/// the address the descriptor reports has to come back on the subscription that reported it.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_redelivery_address() {
    harness::redelivery_address(
        || ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// An operator who writes a password into the endpoint URL must not publish it. The document a
/// service generates is shared, so the scan covers both halves this crate contributes: the server
/// coordinate and the bindings on it.
#[test]
fn zmq_describes_itself_without_credentials() {
    harness::describes_without_credentials(
        &ZmqQueue::new(ZmqEndpoint::connect("tcp://ops:hunter2@broker:5555")),
        &Name::new("jobs"),
        "hunter2",
    );
}

/// The batches are assembled on the client, so this is where the size the subscription was opened
/// with is proved to cap them - the suite opens at a size smaller than the run.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_batch_suite() {
    capabilities::batches(
        || ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_rpc_passes_request_reply_suite() {
    capabilities::request_reply(
        || ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    )
    .await;
}
