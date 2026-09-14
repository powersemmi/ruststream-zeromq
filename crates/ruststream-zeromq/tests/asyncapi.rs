//! What a generated `AsyncAPI` document says about a `ZeroMQ` service.
//!
//! The specification has no `ZeroMQ` binding and its protocol keys are a closed list, so
//! everything this crate reports travels in the `x-ruststream-zeromq` extension. These tests pin
//! the bodies, because they are the contract a peer implementer reads.

#![cfg(all(feature = "asyncapi", feature = "testing"))]

use ruststream::asyncapi::build_spec;
use ruststream::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_zeromq::{
    ZmqEndpoint, ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
struct Job {
    id: u64,
}

#[derive(Serialize, Outgoing)]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish("results"))]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[subscriber("events", publish("audit"))]
async fn watch(job: &Job) -> Done {
    Done { id: job.id }
}

#[subscriber("greeter", publish("reply"))]
async fn greet(job: &Job) -> Done {
    Done { id: job.id }
}

/// Answers the peer that asked, which is what puts a reply address on the operation.
struct ReplyToRequester;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(reply_to) = cx.headers().reply_to() {
            out.set_name(reply_to.to_owned());
        }
    }
}

fn document(app: &RustStream) -> Value {
    let json = build_spec(app)
        .to_json()
        .expect("the document must serialize");
    serde_json::from_str(&json).expect("the document is valid JSON")
}

/// True when every field of `expected` appears in `actual` with the same value.
///
/// The excerpts below are what the documentation shows, so they name the fields this crate fills
/// and stay silent about the rest of the document; a plain equality check would pin the core's
/// output along with them.
fn contains(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(actual), Value::Object(expected)) => expected
            .iter()
            .all(|(key, value)| actual.get(key).is_some_and(|found| contains(found, value))),
        (actual, expected) => actual == expected,
    }
}

fn assert_excerpt(app: &RustStream, excerpt: &str) {
    let expected: Value = serde_json::from_str(excerpt).expect("the excerpt is valid JSON");
    let actual = document(app);
    assert!(
        contains(&actual, &expected),
        "the document does not carry the excerpt.\nexpected: {expected:#}\ngot: {actual:#}",
    );
}

/// A PUSH/PULL worker: the server says how to attach to the endpoint, and the channel the worker
/// publishes to says which socket pair its messages travel over and what their name frame holds.
///
/// The name frame is `results`, the destination the mount site declared, not `jobs`, the name the
/// handler subscribes under.
///
/// The documentation shows this file, so it is pinned here rather than written inline.
const QUEUE_DOCUMENT: &str = include_str!("documents/queue.json");

#[test]
fn a_queue_service_reports_its_endpoint_and_its_socket_pair() {
    let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker_labeled(
        "jobs",
        ZmqQueue::new(ZmqEndpoint::connect("tcp://worker:5555")),
        |b| {
            b.include(work).out_reply(ZmqQueuePublish);
        },
    );

    assert_excerpt(&app, QUEUE_DOCUMENT);
}

/// The prefix a SUB peer subscribes with is the destination, so it travels in the binding.
#[test]
fn a_fan_out_service_reports_its_own_socket_pair() {
    let app = RustStream::new(AppInfo::new("watcher", "0.1.0")).with_broker_labeled(
        "events",
        ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
        |b| {
            b.include(watch).out_reply(ZmqFanoutPublish);
        },
    );

    assert_excerpt(
        &app,
        r#"{
          "servers": {
            "events": {
              "host": "0.0.0.0:5556",
              "protocolVersion": "3.0",
              "bindings": {
                "x-ruststream-zeromq": {
                  "transport": "tcp",
                  "endpoint": "0.0.0.0:5556",
                  "role": "bind"
                }
              }
            }
          },
          "channels": {
            "audit": {
              "bindings": {
                "x-ruststream-zeromq": { "socketPair": "PUB/SUB", "nameFrame": "audit" }
              }
            }
          }
        }"#,
    );
}

/// A responder addresses each answer per request, so the document reports the reply channel
/// without an address and points at the header the address travels in.
const REPLY_ADDRESS: &str = include_str!("documents/reply-address.json");

#[test]
fn a_responder_reports_where_a_client_reads_the_reply_address() {
    let app = RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker_labeled(
        "rpc",
        ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
        |b| {
            b.include(greet)
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester);
        },
    );

    assert_excerpt(&app, REPLY_ADDRESS);
}

/// A reply travels to the identity the ROUTER supplies, so the responder's channel reports no
/// name frame: a peer that sent the channel's own name would reach nobody.
#[test]
fn a_responder_channel_reports_no_name_frame() {
    let app = RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker_labeled(
        "rpc",
        ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
        |b| {
            b.include(greet)
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester);
        },
    );

    let document = document(&app);
    let binding = &document["channels"]["reply"]["bindings"]["x-ruststream-zeromq"];
    assert_eq!(binding["socketPair"], "DEALER/ROUTER");
    assert!(
        binding.get("nameFrame").is_none(),
        "the responder claims an address a peer cannot send to: {binding:#}",
    );
}

/// An `ipc` endpoint names a filesystem path rather than an authority, and the binding keeps it
/// whole.
#[test]
fn an_ipc_endpoint_is_reported_as_a_socket_path() {
    let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker_labeled(
        "jobs",
        ZmqQueue::new(ZmqEndpoint::bind("ipc:///tmp/orders")),
        |b| {
            b.include(work).out_reply(ZmqQueuePublish);
        },
    );

    assert_excerpt(
        &app,
        r#"{
          "servers": {
            "jobs": {
              "host": "/tmp/orders",
              "bindings": {
                "x-ruststream-zeromq": { "transport": "ipc", "endpoint": "/tmp/orders" }
              }
            }
          }
        }"#,
    );
}
