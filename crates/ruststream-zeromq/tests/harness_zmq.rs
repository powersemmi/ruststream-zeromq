//! A service's wiring under the `TestApp` harness: the production app, on the production
//! brokers, connected in process. Each test builds the app the way `main` builds it and addresses
//! a broker by its production type or by its label.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a reply
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{DefaultSlot, Outgoing, PublishContext};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, RequestReply};
use ruststream_zeromq::{
    Connect, ZmqEndpoint, ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc,
    ZmqRpcPublish,
};
use serde::{Deserialize, Serialize};

/// Where the service binds: an ephemeral loopback port the in-process transport never opens.
const LOOPBACK: &str = "tcp://127.0.0.1:0";

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Done {
    id: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Event {
    id: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
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

/// The worker and the queue its results travel on, each an endpoint of its own: on PUSH/PULL a
/// name is frame 0 rather than an address, so a result published on the queue the `jobs`
/// subscription binds would come back to that subscription.
fn worker() -> RustStream {
    let results = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
    let to_results = results.bind(ZmqQueuePublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled("jobs", ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)), |b| {
            b.include(work).out_reply(to_results);
        })
        .with_broker_labeled("results", results, |b| {
            b.include(first_worker);
        })
}

/// The result reaches the subscription that binds the results queue, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_is_worked_once_on_a_queue_of_its_own() {
    let tb = TestApp::start(worker()).await.expect("the app starts");

    tb.broker_named("jobs")
        .message(&Job { id: 7 })
        .to("jobs")
        .publish()
        .await
        .expect("the job drives the reaction to a standstill");

    tb.broker_named("results")
        .subscriber("results")
        .assert_called_once()
        .with(&Done { id: 7 });

    tb.shutdown().await.expect("the app shuts down");
}

/// Two workers that dial one ventilator compete for its pushes, so a result is worked once, by
/// one of them. Each mount names where its retry copies go, because the peer they read from
/// takes nothing back.
fn two_dialing_workers() -> RustStream {
    let retries = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
    let first_copies = retries.bind(ZmqQueuePublish);
    let second_copies = retries.bind(ZmqQueuePublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
            |b| {
                b.include(first_worker)
                    .out_retry(first_copies)
                    .to("results");
                b.include(second_worker)
                    .out_retry(second_copies)
                    .to("results");
            },
        )
        .with_broker(retries, |_b| {})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_workers_dialing_one_peer_work_a_result_once() {
    let tb = TestApp::start(two_dialing_workers())
        .await
        .expect("the app starts");

    tb.broker::<ZmqQueue<Connect>>()
        .message(&Done { id: 3 })
        .to("results")
        .publish()
        .await
        .expect("the push drives the reaction to a standstill");

    tb.broker::<ZmqQueue<Connect>>()
        .subscriber("results")
        .assert_called_once()
        .with(&Done { id: 3 });

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
/// through the fan-out and is not refused the way a queue refuses a name its subscription does
/// not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_mount_that_names_no_reply_policy_still_fans_out() {
    let app = RustStream::new(AppInfo::new("watcher", "0.1.0")).with_broker(
        ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK)),
        |b| {
            b.include(watch);
        },
    );
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<ZmqFanout>()
        .message(&Event { id: 5 })
        .to("events")
        .publish()
        .await
        .expect("the event drives the reaction to a standstill");

    tb.broker::<ZmqFanout>()
        .published::<Note>("audit.high")
        .assert_called_once()
        .with(&Note { id: 5 });

    tb.shutdown().await.expect("the app shuts down");
}

/// The fan-out keeps the pattern's filter: a subscription on `audit` receives what was published
/// to `audit.high`. The audit fan-out is an endpoint of its own, bound by the subscription that
/// reads it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_prefix_subscription_receives_what_the_fan_out_publishes() {
    let audit = ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
    let to_audit = audit.bind(ZmqFanoutPublish);
    let app = RustStream::new(AppInfo::new("watcher", "0.1.0"))
        .with_broker_labeled("events", ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK)), |b| {
            b.include(watch).out_reply(to_audit);
        })
        .with_broker_labeled("audit", audit, |b| {
            b.include(auditor);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker_named("events")
        .message(&Event { id: 3 })
        .to("events")
        .publish()
        .await
        .expect("the event drives the reaction to a standstill");

    tb.broker_named("audit")
        .subscriber("audit")
        .assert_called_once()
        .with(&Note { id: 3 });

    tb.shutdown().await.expect("the app shuts down");
}

#[subscriber("reports")]
async fn file_report(done: &Done) -> HandlerOutcome {
    let _ = done.id;
    HandlerOutcome::ack()
}

/// Two subscriptions on the endpoint one broker binds.
fn two_on_one_endpoint() -> RustStream {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)),
        |b| {
            b.include(first_worker);
            b.include(file_report);
        },
    )
}

/// A bound endpoint belongs to the subscription that bound it, so a second registration on the
/// same broker is refused at startup, in process and over the socket in the same words. The
/// in-process leg alone would pass a routes file whose second subscription never receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_subscription_on_a_bound_endpoint_is_refused_in_both_modes() {
    let Err(live) = TestApp::start_live(two_on_one_endpoint()).await else {
        panic!("the second subscription on the bound endpoint must not start over the socket");
    };
    let Err(in_process) = TestApp::start(two_on_one_endpoint()).await else {
        panic!("the in-process mode must refuse what the socket refuses");
    };

    let live = live.to_string();
    assert!(
        live.contains("'results'") && live.contains("'reports'"),
        "the refusal must name both subscriptions, got: {live}",
    );
    assert_eq!(in_process.to_string(), live);
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Greeting {
    who: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Answer {
    text: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct AskFor {
    who: String,
}

/// What the asking handler heard back.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "heard")]
struct Heard {
    text: String,
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

/// Asks the greeter and reports what it heard; it binds the capability, so it mounts only on a
/// policy whose live publisher answers requests.
#[subscriber("asks", publish)]
async fn ask(request: &AskFor, Out(rpc): Out<impl RequestReply>) -> Heard {
    let text = match JsonCodec.encode(&Greeting {
        who: request.who.clone(),
    }) {
        Ok(encoded) => match rpc
            .request(
                OutgoingMessage::new("greeter", encoded.as_ref()),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(reply) => JsonCodec.decode::<Answer>(reply.payload()).map_or_else(
                |err| format!("undecodable answer: {err}"),
                |answer| answer.text,
            ),
            Err(err) => format!("no answer: {err}"),
        },
        Err(err) => format!("unencodable request: {err}"),
    };
    Heard { text }
}

/// The whole request-reply wiring under the harness: a handler binding `Out<impl RequestReply>`
/// asks the responder this service binds, and the answer comes back correlated to the caller that
/// asked. The asks arrive on a queue of their own, and what the handler heard leaves on another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_reply_pair_runs_under_the_harness() {
    let exchange = ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
    let requests = exchange.bind(ZmqRpcPublish);
    let heard = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
    let to_heard = heard.bind(ZmqQueuePublish);
    let app = RustStream::new(AppInfo::new("greeter", "0.1.0"))
        .with_broker_labeled("exchange", exchange, |b| {
            // A responder addresses no retry copies, so every mount on it names where they go.
            b.include(greet)
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        })
        .with_broker_labeled("asks", ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)), |b| {
            b.include(ask)
                .out_reply(to_heard)
                .out(DefaultSlot, requests)
                .build();
        })
        .with_broker_labeled("heard", heard, |_b| {});
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker_named("asks")
        .message(&AskFor {
            who: "world".to_owned(),
        })
        .to("asks")
        .publish()
        .await
        .expect("the ask drives the exchange to a standstill");

    tb.broker_named("exchange")
        .subscriber("greeter")
        .assert_called_once()
        .with(&Greeting {
            who: "world".to_owned(),
        });
    tb.broker_named("heard")
        .published::<Heard>("heard")
        .assert_called_once()
        .with(&Heard {
            text: "hello world".to_owned(),
        });

    tb.shutdown().await.expect("the app shuts down");
}
