//! Where a reply lands on this transport: at the name its own type declares, or at the name the
//! mount site supplies to a type that declares none. Both resolutions run through the `TestApp`
//! harness on the in-process transport.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a reply
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::testing::TestApp;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscribe};
use ruststream_zeromq::testing::{Queue, ZmqTestBroker};
use ruststream_zeromq::{ZmqEndpoint, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish};
use serde::{Deserialize, Serialize};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_names_its_queue_publishes_there() {
    let app = RustStream::new(AppInfo::new("zmq-declared-reply", "0.0.0")).with_broker(
        ZmqTestBroker::queue(),
        |b| {
            b.include(work).out(Reply, ZmqQueuePublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Job { id: 7 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker::<ZmqTestBroker<Queue>>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 7 });
    // No name appears at the mount site, so this one can only come from the reply type.
    tb.broker::<ZmqTestBroker<Queue>>()
        .published::<Done>("results")
        .assert_called_once()
        .with(&Done { id: 7 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_without_a_name_publishes_where_the_mount_site_says() {
    let app = RustStream::new(AppInfo::new("zmq-mounted-reply", "0.0.0")).with_broker(
        ZmqTestBroker::queue(),
        |b| {
            b.include(greet).out(Reply, ZmqQueuePublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker<Queue>>()
        .message(&Greeting {
            who: "world".to_owned(),
        })
        .to("greeter")
        .publish()
        .await
        .expect("the request is published");

    tb.broker::<ZmqTestBroker<Queue>>()
        .subscriber("greeter")
        .assert_called_once()
        .with(&Greeting {
            who: "world".to_owned(),
        });
    // `Answer` declares nothing, so this name is the one the `publish("answers")` clause supplied.
    tb.broker::<ZmqTestBroker<Queue>>()
        .published::<Answer>("answers")
        .assert_called_once()
        .with(&Answer {
            text: "hello world".to_owned(),
        });
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
        if let Some(reply_to) = cx.headers().reply_to() {
            out.set_name(reply_to.to_owned());
        }
        if let Some(correlation) = cx.headers().correlation_id() {
            out.headers_mut()
                .insert("correlation-id", correlation.to_owned());
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
