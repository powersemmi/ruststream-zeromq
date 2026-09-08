//! Conformance: the routing suite against the in-process transport, plus the lifecycle and
//! request/reply suites over real sockets on the loopback - no external broker exists to
//! need, which is the point of this crate.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_zeromq::testing::ZmqTestBroker;
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue, ZmqRpc};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_test_broker_passes_conformance_suite() {
    harness::run_suite(ZmqTestBroker::new).await;
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
