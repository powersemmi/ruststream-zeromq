//! Conformance: the core's contract suites, each run twice where both transports carry it, over
//! real sockets on the loopback and in process - no external broker exists to need, which is the
//! point of this crate.
//!
//! The request/reply pattern is held to the request/reply suite alone: its publisher answers a
//! request, and a request travels through `RequestReply`, so the suites that publish to a
//! subscription by name have nothing to publish there.

#![cfg(feature = "testing")]
// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use std::time::Duration;

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::{capabilities, harness, message_shape, retry, settlement};
use ruststream_zeromq::{
    ConnectedZmqFanout, ConnectedZmqQueue, ConnectedZmqRpc, ZmqEndpoint, ZmqFanout,
    ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish,
};

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

/// The subscription binds an ephemeral loopback port and the publisher dials the resolved
/// address: the loopback arrangement, which is the one a service publishing to its own
/// subscription runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_lifecycle() {
    harness::lifecycle(
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The ladder holds in process too, aliasing included: a publisher paired before the shutdown
/// reports the dead transport rather than succeeding against it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_one_way_patterns_pass_lifecycle_in_process() {
    harness::lifecycle(
        || InProcessBroker::new(ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
    harness::lifecycle(
        || InProcessBroker::new(ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// A queue addresses its own subscription, and this is where that promise is held: a publish to
/// the address the name reports has to come back on the subscription that reported it. The name
/// is the descriptor of every pattern here, so one call covers both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_redelivery_address() {
    retry::redelivery_address(
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The two patterns that address their own copies keep that promise in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_addressed_patterns_pass_redelivery_address_in_process() {
    retry::redelivery_address(
        || InProcessBroker::new(ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
    retry::redelivery_address(
        || InProcessBroker::new(ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// ZMTP has no frame to settle a delivery with, so every settlement answers `Unsupported`, over
/// the sockets and in process alike: a test on the in-process transport never sees a retry the
/// sockets cannot make.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_settles_in_process_as_over_sockets() {
    settlement::matches_in_process(
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        Duration::ZERO,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_fanout_passes_settlement_in_process() {
    settlement::suite(
        || InProcessBroker::new(ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        Duration::ZERO,
    )
    .await;
}

/// A bound endpoint is one listening socket, so a second subscription on it is refused by both
/// transports, on every pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_subscription_on_a_bound_endpoint_is_refused_like_the_sockets_refuse_it() {
    let second = |name: &str| Refusal::Conflicting {
        open: Name::new(name.to_owned()),
        refused: Name::new(name.to_owned()),
    };
    in_process::refuses_like_the_server(
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |connected| connected.publisher(),
        [second("jobs")],
    )
    .await;
    in_process::refuses_like_the_server(
        || ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK)),
        |connected| connected.publisher(),
        [second("events")],
    )
    .await;
    in_process::refuses_like_the_server(
        || ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK)),
        |connected| connected.publisher(),
        [second("greeter")],
    )
    .await;
}

/// An operator who writes a password into the endpoint URL must not publish it. The document a
/// service generates is shared, so the scan covers everything this crate contributes: the server
/// coordinate, the bindings of each descriptor and of each publish policy.
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
    message_shape::publishes_without_credentials::<ConnectedZmqQueue, _>(
        &ZmqQueuePublish,
        "hunter2",
    );
    message_shape::publishes_without_credentials::<ConnectedZmqFanout, _>(
        &ZmqFanoutPublish,
        "hunter2",
    );
    message_shape::publishes_without_credentials::<ConnectedZmqRpc, _>(&ZmqRpcPublish, "hunter2");
}

/// An endpoint is one address, so a deployment given several picks one; whichever it is, the
/// document names it without its userinfo.
#[test]
fn zmq_describes_its_address_without_credentials() {
    message_shape::describes_addresses_without_credentials(
        |addrs| ZmqQueue::new(ZmqEndpoint::connect(addrs[0])),
        "tcp",
    );
    message_shape::describes_addresses_without_credentials(
        |addrs| ZmqFanout::new(ZmqEndpoint::connect(addrs[0])),
        "tcp",
    );
    message_shape::describes_addresses_without_credentials(
        |addrs| ZmqRpc::new(ZmqEndpoint::connect(addrs[0])),
        "tcp",
    );
}

/// The batches are assembled on the client, so this is where the size the subscription was opened
/// with is proved to cap them - the suite opens at a size smaller than the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_queue_passes_batch_suite() {
    capabilities::batches(
        || ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The batches are assembled on the client over either transport, to the size the subscription
/// was opened with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_one_way_patterns_pass_batch_suite_in_process() {
    capabilities::batches(
        || InProcessBroker::new(ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
    capabilities::batches(
        || InProcessBroker::new(ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

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

/// The responder answers the same request-reply contract in process that it answers over
/// sockets, the leg where nobody answers included, so a handler that binds the capability is
/// testable in process.
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
