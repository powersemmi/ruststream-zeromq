//! Where a reply lands on this transport: at the name its own type declares, or at the name the
//! mount site supplies to a type that declares none, over a queue of its own. Both resolutions run
//! through the `TestApp` harness on the in-process transport, and the queue a reply cannot use -
//! the one its own subscription holds - is refused on the stand and on the socket alike.

#![cfg(feature = "testing")]

use std::io;
use std::net::TcpListener;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a reply
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Bound, BrokerScope, Outgoing, PublishContext};
use ruststream::testing::TestApp;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscribe, Subscriber,
};
use ruststream_zeromq::testing::{Queue, ZmqTestBroker};
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Job {
    id: u64,
}

// The result queue is a property of the message, so the type names it and the clause stays bare.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish)]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Greeting {
    who: String,
}

// An answer has no destination of its own on the request/reply form, so the type declares none
// and the mount site supplies the name.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Answer {
    text: String,
}

#[subscriber("greeter", publish("answers"))]
async fn greet(request: &Greeting) -> Answer {
    Answer {
        text: format!("hello {}", request.who),
    }
}

/// The worker and the queue its results leave through: two stands, because on PUSH/PULL a
/// second kind of message needs an endpoint of its own.
fn worker_and_results<Def>(def: Def) -> RustStream
where
    Def: FnOnce(
        &mut BrokerScope<ZmqTestBroker<Queue>>,
        Bound<ZmqTestBroker<Queue>, ZmqQueuePublish>,
    ),
{
    let results = ZmqTestBroker::queue().bindable();
    let to_results = results.bind(ZmqQueuePublish);
    RustStream::new(AppInfo::new("zmq-reply", "0.0.0"))
        .with_broker_labeled("worker", ZmqTestBroker::queue(), |b| def(b, to_results))
        .with_broker_labeled("results", results, |_b| {})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_names_its_queue_publishes_there() {
    let app = worker_and_results(|b, to_results| {
        b.include(work).out(Reply, to_results);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker_named("worker")
        .message(&Job { id: 7 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker_named("worker")
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 7 });
    // No name appears at the mount site, so this one can only come from the reply type.
    tb.broker_named("results")
        .published::<Done>("results")
        .assert_called_once()
        .with(&Done { id: 7 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_without_a_name_publishes_where_the_mount_site_says() {
    let app = worker_and_results(|b, to_results| {
        b.include(greet).out(Reply, to_results);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker_named("worker")
        .message(&Greeting {
            who: "world".to_owned(),
        })
        .to("greeter")
        .publish()
        .await
        .expect("the request is published");

    tb.broker_named("worker")
        .subscriber("greeter")
        .assert_called_once()
        .with(&Greeting {
            who: "world".to_owned(),
        });
    // `Answer` declares nothing, so this name is the one the `publish("answers")` clause supplied.
    tb.broker_named("results")
        .published::<Answer>("answers")
        .assert_called_once()
        .with(&Answer {
            text: "hello world".to_owned(),
        });
}

/// A reply bound to the queue its own subscription holds has nowhere to go but back to that
/// subscription, so the stand refuses it the way the socket does: the job is worked once and the
/// result is published nowhere. The live test below shows the socket; this is the harness catching
/// the same mount before it ships.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_on_the_queue_its_subscription_holds_is_refused_under_the_harness() {
    let app = RustStream::new(AppInfo::new("zmq-looping-reply", "0.0.0")).with_broker(
        ZmqTestBroker::queue(),
        |b| {
            b.include(work).out(Reply, ZmqQueuePublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Job { id: 8 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker::<ZmqTestBroker<Queue>>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 8 });
    tb.broker::<ZmqTestBroker<Queue>>()
        .published::<Done>("results")
        .assert_not_called();
}

/// The stand refuses in the words the socket uses, so a test read against it teaches the fix the
/// deployment needs: a publisher on the queue a subscription of this service binds reaches that
/// subscription, under its own name, and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stand_refuses_a_publish_the_bound_queue_would_hand_back_in_the_sockets_words() {
    let socket = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the queue connects");
    let _jobs = socket
        .subscribe("jobs")
        .await
        .expect("the subscription binds the queue");
    let stand = ZmqTestBroker::queue()
        .connect()
        .await
        .expect("the stand connects");
    let _stand_jobs = stand
        .subscribe("jobs")
        .await
        .expect("the subscription opens");

    let on_socket = socket
        .publisher()
        .publish(OutgoingMessage::new("results", b"{}".as_slice()), None)
        .await
        .expect_err("the subscription that bound the queue would receive it")
        .to_string();
    let on_stand = stand
        .publisher()
        .publish(OutgoingMessage::new("results", b"{}".as_slice()), None)
        .await
        .expect_err("the stand refuses what the socket refuses")
        .to_string();

    assert!(
        on_socket.contains("'results'") && on_socket.contains("'jobs'"),
        "the refusal must name the destination and the subscription, got: {on_socket}",
    );
    assert_eq!(on_socket, on_stand);

    stand.shutdown().await.expect("the stand shuts down");
    socket.shutdown().await.expect("the queue shuts down");
}

/// A second subscription under another name cannot bind the queue endpoint the first one holds:
/// the socket refuses it as it opens, and the stand refuses it there too, so a test finds the
/// invalid mount at startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_name_on_a_bound_queue_is_refused_as_it_opens() {
    // A fixed port: a second bind of port 0 would take another ephemeral port and succeed.
    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port();
    let socket = ZmqQueue::new(ZmqEndpoint::bind(format!("tcp://127.0.0.1:{port}")))
        .connect()
        .await
        .expect("the queue connects");
    let _jobs = socket.subscribe("jobs").await.expect("the first binds");
    assert!(
        socket.subscribe("other").await.is_err(),
        "the socket refuses a second bind"
    );

    let stand = ZmqTestBroker::queue()
        .connect()
        .await
        .expect("the stand connects");
    let _stand_jobs = stand.subscribe("jobs").await.expect("the first opens");
    assert!(
        stand.subscribe("other").await.is_err(),
        "the stand refuses what the socket refuses"
    );

    stand.shutdown().await.expect("the stand shuts down");
    socket.shutdown().await.expect("the queue shuts down");
}

// -- Where a reply lands on a real ROUTER ------------------------------------------------------
//
// The two resolutions above are the framework's, and the stand answers them the way the socket
// does. What only a socket can answer is the third one: on DEALER/ROUTER a reply is addressed to
// the peer identity the ROUTER derived from the request, and nothing in process derives that
// identity.

/// Long enough for a handshake and a round trip on a loaded machine.
const LIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Routes a reply back to the peer that asked.
///
/// The transform reads the delivery being answered, so it names [`ForReply`], and it supplies the
/// destination per delivery, so it declares [`Names`]. That right is on offer because [`Answer`]
/// declares no destination of its own.
#[derive(Debug, Clone, Copy)]
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

/// The whole responder path over sockets: the ROUTER stamps the requesting peer on the request,
/// the transform turns that stamp into the reply's destination, and the answer reaches the DEALER
/// that asked. The mount-site name `answers` is only the fallback the document reports; no peer
/// listens on it, so an answer that ignored the stamp would never arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_reaches_the_peer_the_router_stamped_on_the_request() {
    // A publisher handed out before `connect` shares the pattern's state, which is how the test
    // reaches the ephemeral port the responder binds.
    let rpc = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
    let requester = rpc.publisher();

    let app = RustStream::new(AppInfo::new("zmq-live-reply", "0.0.0")).with_broker(rpc, |b| {
        b.include(greet)
            .out_reply(ZmqRpcPublish)
            .transform(ReplyToRequester)
            .out_retry(ZmqRpcPublish)
            .to("greeter.retry");
    });
    let running = app.start().await.expect("the responder starts");

    let request = serde_json::to_vec(&Greeting {
        who: "world".to_owned(),
    })
    .expect("the request encodes");
    let reply = requester
        .request(
            OutgoingMessage::new("greeter", request.as_slice()),
            LIVE_TIMEOUT,
        )
        .await
        .expect("the answer comes back to the peer that asked");

    assert_eq!(
        serde_json::from_slice::<Answer>(reply.payload()).expect("the answer decodes"),
        Answer {
            text: "hello world".to_owned(),
        },
    );
    // Frame 0 of a reply is the literal `reply`: the ROUTER identity frame in front of it is what
    // addressed this peer, so the name position carries nothing to route on.
    assert_eq!(reply.name(), "reply");

    running.shutdown().await.expect("the app shuts down");
}

/// What the reply publisher refuses, on the socket rather than on the stand.
///
/// Each refusal is a mount that would otherwise publish into the void: a plain name has no peer
/// behind it, an address that is not hex names no identity, and a pattern with no responder
/// attached has no ROUTER to route through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reply_publisher_refuses_what_it_cannot_route() {
    let connected = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the responder connects");
    let publisher = connected.publisher();

    let plain = publisher
        .publish(OutgoingMessage::new("greeter", b"{}".as_slice()), None)
        .await
        .expect_err("a plain name is not an address on this pattern")
        .to_string();
    assert!(
        plain.contains("zmq-reply:") && plain.contains("request()"),
        "the refusal must name the address shape and the way out, got: {plain}",
    );

    let detached = publisher
        .publish(OutgoingMessage::new("zmq-reply:00", b"{}".as_slice()), None)
        .await
        .expect_err("nothing can be routed before a responder attaches its ROUTER")
        .to_string();
    assert!(
        detached.contains("no responder subscription is attached"),
        "the refusal must name the missing responder, got: {detached}",
    );

    let mut subscriber = connected
        .subscribe("greeter")
        .await
        .expect("the responder subscription opens");
    let malformed = publisher
        .publish(OutgoingMessage::new("zmq-reply:zz", b"{}".as_slice()), None)
        .await
        .expect_err("an address that is not hex names no peer identity")
        .to_string();
    assert!(
        malformed.contains("malformed reply address"),
        "the refusal must say the address is malformed, got: {malformed}",
    );

    let _ = &mut subscriber;
    connected
        .shutdown()
        .await
        .expect("the responder shuts down");
}

/// An answer whose requester has gone.
///
/// The address still parses, so the refusal comes from the ROUTER rather than from the
/// publisher, and it is a refusal rather than a silent drop: a responder learns that its answer
/// reached nobody. That is the opposite of the fan-out, where an unmatched broadcast is dropped
/// without a word.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_to_a_peer_that_is_gone_is_refused_rather_than_dropped() {
    let connected = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the responder connects");
    let _subscriber = connected
        .subscribe("greeter")
        .await
        .expect("the responder subscription opens");

    let err = connected
        .publisher()
        .publish(
            // Well formed, and no peer ever carried this identity.
            OutgoingMessage::new("zmq-reply:deadbeef", b"{}".as_slice()),
            None,
        )
        .await
        .expect_err("no peer carries this identity")
        .to_string();
    assert!(
        err.contains("zmq-reply:deadbeef"),
        "the failure must name the reply address it could not reach, got: {err}",
    );

    connected
        .shutdown()
        .await
        .expect("the responder shuts down");
}

// -- Where a reply lands on a real queue -------------------------------------------------------
//
// On PUSH/PULL a name is frame 0, not an address: the PULL socket a subscription binds takes
// whatever is pushed into it. A publisher on the queue this service's own subscription binds
// therefore reaches that subscription and nothing else.

/// What the worker saw, reported over a queue of its own so the test reads it off a socket.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "seen")]
struct Seen {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Seen)]
struct Sightings;

/// Reports every job it is handed, then answers it with a result named `results`.
#[subscriber("jobs", publish)]
async fn work_and_report(job: &Job, Out(seen): Out<impl Publisher, Sightings>) -> Done {
    seen.message(&Seen { id: job.id })
        .publish()
        .await
        .expect("the sighting reaches the sink queue");
    Done { id: job.id }
}

/// How long the sink has to stay quiet before the worker counts as done.
const SILENCE: Duration = Duration::from_millis(500);

/// A worker that binds its queue and answers on it hands its answer back to itself: the result
/// named `results` would arrive on the PULL socket the `jobs` subscription bound, decode as the
/// next job, and be answered again, forever. The publish is refused instead, so the job is worked
/// once and the refusal names the way out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_on_the_queue_its_subscription_binds_does_not_come_back_to_it() {
    let sink = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect()
        .await
        .expect("the sink queue connects");
    let mut sightings = sink
        .subscribe("sink")
        .await
        .expect("the sink subscription opens");
    let egress = ZmqQueue::new(ZmqEndpoint::connect(
        sink.bound_address()
            .expect("the sink subscription bound a port"),
    ))
    .bindable();
    let slot = egress.bind(ZmqQueuePublish);

    let app = RustStream::new(AppInfo::new("zmq-live-queue-reply", "0.0.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")), |b| {
            b.include(work_and_report)
                .out_reply(ZmqQueuePublish)
                .out(Sightings, slot)
                .build();
            b.after_startup(ZmqQueuePublish, async move |publisher| -> io::Result<()> {
                publisher
                    .message(&Job { id: 21 })
                    .to("jobs")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        })
        .with_broker(egress, |_b| {});
    let running = app.start().await.expect("the worker starts");

    let mut stream = pin!(sightings.stream());
    let first = timeout(LIVE_TIMEOUT, stream.next())
        .await
        .expect("the worker reports the job before the deadline")
        .expect("the sink stream is open")
        .expect("the report is well formed");
    assert_eq!(
        serde_json::from_slice::<Seen>(first.payload()).expect("the report decodes"),
        Seen { id: 21 },
    );
    assert!(
        timeout(SILENCE, stream.next()).await.is_err(),
        "the worker was handed its own result as another job",
    );

    running.shutdown().await.expect("the worker shuts down");
    sink.shutdown().await.expect("the sink shuts down");
}
