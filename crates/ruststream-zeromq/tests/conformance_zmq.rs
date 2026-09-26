//! Conformance: the routing suite against each production broker connected in process, and the
//! lifecycle, redelivery-address, batch and request/reply suites twice, over real sockets on the
//! loopback and in process - no external broker exists to need, which is the point of this crate.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqRpc};

/// Where a service binds its end of each pattern in these suites: an ephemeral loopback port,
/// which the sockets resolve and the in-process transport never opens.
const LOOPBACK: &str = "tcp://127.0.0.1:0";

/// Where a service dials a peer. The in-process suites never reach it.
const PEER: &str = "tcp://127.0.0.1:5555";

/// Every production broker answers the routing contract in process, on both sides of its
/// endpoint, so a pattern or a side cannot drift from it while the crate tested another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_broker_passes_the_conformance_suite_in_process() {
    harness::run_suite(|| ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))).await;
    harness::run_suite(|| ZmqQueue::new(ZmqEndpoint::connect(PEER))).await;
    harness::run_suite(|| ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))).await;
    harness::run_suite(|| ZmqFanout::new(ZmqEndpoint::connect(PEER))).await;
    harness::run_suite(|| ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK))).await;
    harness::run_suite(|| ZmqRpc::new(ZmqEndpoint::connect(PEER))).await;
}

/// The ladder holds in process too, aliasing included: a publisher paired before the shutdown
/// reports the dead transport rather than succeeding against it.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_lifecycle_in_process() {
    harness::lifecycle(
        || InProcessBroker::new(ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// A publish under the subscribe name has to come back on the subscription that reported it, on
/// the two patterns that declare they address their own copies.
///
/// The real PUB/SUB socket is left out of the socket pass - it drops what it sends before the
/// subscriber's filter has propagated - and the in-process transport has no filter table to
/// propagate, so the fan-out's promise is checked here.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_addressed_patterns_pass_redelivery_address_in_process() {
    harness::redelivery_address(
        || InProcessBroker::new(ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
    harness::redelivery_address(
        || InProcessBroker::new(ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The responder answers the same request-reply contract in process that it answers over
/// sockets, the leg where nobody answers included, so a handler that binds the capability is
/// testable in process.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_rpc_passes_request_reply_suite_in_process() {
    capabilities::request_reply(
        || InProcessBroker::new(ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    )
    .await;
}

/// The batches are assembled on the client over either transport, to the size the subscription
/// was opened with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_batch_suite_in_process() {
    capabilities::batches(
        || InProcessBroker::new(ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
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
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
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
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
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
    let endpoint = || ZmqEndpoint::connect("tcp://ops:hunter2@broker:5555");
    harness::describes_without_credentials(
        &ZmqQueue::new(endpoint()),
        &Name::new("jobs"),
        "hunter2",
    );
    harness::describes_without_credentials(
        &ZmqFanout::new(endpoint()),
        &Name::new("events"),
        "hunter2",
    );
    harness::describes_without_credentials(
        &ZmqRpc::new(endpoint()),
        &Name::new("greeter"),
        "hunter2",
    );
}

/// The batches are assembled on the client, so this is where the size the subscription was opened
/// with is proved to cap them - the suite opens at a size smaller than the run.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_batch_suite() {
    capabilities::batches(
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_rpc_passes_request_reply_suite() {
    capabilities::request_reply(
        || ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK)),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    )
    .await;
}
