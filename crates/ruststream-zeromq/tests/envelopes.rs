//! Every built-in envelope on every socket pattern: a message read through it reaches the handler
//! with its payload and, where the layout carries them, its headers, and what the handler sends on
//! leaves through it and reads back the same, in process and over loopback sockets.
#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::codec::{Codec, JsonCodec};
use ruststream::prelude::*;
use ruststream::runtime::{DefaultSlot, Outgoing, PublishContext};
use ruststream::testing::TestApp;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, RequestReply, Subscribe,
    Subscriber,
};
use ruststream_zeromq::envelope::{Framed, Frames, NoOp};
use ruststream_zeromq::{
    ZmqEndpoint, ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish,
};
use serde::{Deserialize, Serialize};
use zeromq::prelude::*;

const LOOPBACK: &str = "tcp://127.0.0.1:0";

const TENANT: &str = "x-tenant";

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, Outgoing)]
struct Draft {
    id: u64,
}

/// The draft passed on, with the tenant its header named, if it carried one.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "notes")]
struct Note {
    id: u64,
    tenant: Option<String>,
}

#[derive(OutSlot)]
#[publishes(Note)]
struct Notes;

/// Passes a draft on with the tenant header it arrived with; a refused publish drops the draft.
#[subscriber("drafts")]
async fn pass_on(
    draft: &Draft,
    ctx: &mut Context,
    Out(notes): Out<impl Publisher, Notes>,
) -> HandlerOutcome {
    let tenant = ctx.headers().get_str(TENANT).map(str::to_owned);
    let mut headers = HeaderMap::new();
    if let Some(tenant) = &tenant {
        headers.insert(TENANT, tenant.clone());
    }
    let note = Note {
        id: draft.id,
        tenant,
    };
    match notes.message(&note).with_headers(headers).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::drop(),
    }
}

/// Takes a note whose header arrived as the note says it was sent.
#[subscriber("notes")]
async fn take_note(note: &Note, ctx: &mut Context) -> HandlerOutcome {
    if ctx.headers().get_str(TENANT) == note.tenant.as_deref() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// Publishes a draft into the relay, with the tenant header where the envelope carries one, and
/// checks both hops: the relay read it, and the note it sent on arrived through the envelope.
async fn a_draft_is_passed_on(tb: TestApp<()>, tenant: Option<&str>) {
    let mut headers = HeaderMap::new();
    if let Some(tenant) = tenant {
        headers.insert(TENANT, tenant.to_owned());
    }
    tb.broker_named("relay")
        .message(&Draft { id: 7 })
        .to("drafts")
        .with_headers(headers)
        .publish()
        .await
        .expect("the draft reaches the relay");
    tb.settle()
        .await
        .expect("the draft and its note are handled");

    tb.broker_named("relay")
        .subscriber("drafts")
        .assert_called_once()
        .with(&Draft { id: 7 })
        .settled(HandlerOutcome::ack());
    tb.broker_named("notes")
        .subscriber("notes")
        .assert_called_once()
        .with(&Note {
            id: 7,
            tenant: tenant.map(str::to_owned),
        })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("the service stops");
}

/// A relay and a note taker, each on a broker of one pattern and one envelope; the relay's slot
/// publishes to the note taker's endpoint through that envelope.
macro_rules! relay_app {
    ($broker:ident, $policy:expr, $envelope:expr) => {{
        let notes = $broker::new(ZmqEndpoint::bind(LOOPBACK))
            .envelope($envelope)
            .bindable();
        let to_notes = notes.bind($policy);
        RustStream::new(AppInfo::new("zmq-envelopes", "0.0.0"))
            .with_broker_labeled("notes", notes, |b| {
                b.include(take_note);
            })
            .with_broker_labeled(
                "relay",
                $broker::new(ZmqEndpoint::bind(LOOPBACK)).envelope($envelope),
                |b| {
                    b.include(pass_on).out(Notes, to_notes).build();
                },
            )
    }};
}

/// The round trip of one envelope on the one-way patterns, in process and over sockets.
macro_rules! one_way_round_trips {
    ($module:ident, $envelope:expr, $tenant:expr) => {
        mod $module {
            use super::*;

            #[tokio::test(start_paused = true)]
            async fn on_the_queue_in_process() {
                let app = relay_app!(ZmqQueue, ZmqQueuePublish, $envelope);
                let tb = TestApp::start(app).await.expect("the harness starts");
                a_draft_is_passed_on(tb, $tenant).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn on_the_queue_live() {
                let app = relay_app!(ZmqQueue, ZmqQueuePublish, $envelope);
                let tb = TestApp::start_live(app)
                    .await
                    .expect("the harness starts over loopback sockets");
                a_draft_is_passed_on(tb, $tenant).await;
            }

            #[tokio::test(start_paused = true)]
            async fn on_the_fan_out_in_process() {
                let app = relay_app!(ZmqFanout, ZmqFanoutPublish, $envelope);
                let tb = TestApp::start(app).await.expect("the harness starts");
                a_draft_is_passed_on(tb, $tenant).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn on_the_fan_out_live() {
                let app = relay_app!(ZmqFanout, ZmqFanoutPublish, $envelope);
                let tb = TestApp::start_live(app)
                    .await
                    .expect("the harness starts over loopback sockets");
                a_draft_is_passed_on(tb, $tenant).await;
            }
        }
    };
}

one_way_round_trips!(frames, Frames, Some("acme"));
one_way_round_trips!(framed, Framed::new(JsonCodec), Some("acme"));
one_way_round_trips!(no_op, NoOp, None);

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
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

/// The reply goes to the address the request carried, under the id it was asked with when it
/// carried one.
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

#[subscriber("greeter", reply("reply"))]
async fn greet(request: &Greeting) -> Answer {
    Answer {
        text: format!("hello {}", request.who),
    }
}

/// Asks the greeter and reports what it heard.
#[subscriber("asks", reply)]
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

#[subscriber("heard")]
async fn take_heard(_heard: &Heard) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// A handler asks a responder on the envelope, and what it heard leaves on a queue of its own.
macro_rules! exchange_app {
    ($envelope:expr) => {{
        let exchange = ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK))
            .envelope($envelope)
            .bindable();
        let requests = exchange.bind(ZmqRpcPublish);
        let heard = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
        let to_heard = heard.bind(ZmqQueuePublish);
        RustStream::new(AppInfo::new("zmq-envelopes", "0.0.0"))
            .with_broker_labeled("exchange", exchange, |b| {
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
            .with_broker_labeled("heard", heard, |b| {
                b.include(take_heard);
            })
    }};
}

async fn a_request_is_answered(tb: TestApp<()>) {
    tb.broker_named("asks")
        .message(&AskFor {
            who: "world".to_owned(),
        })
        .to("asks")
        .publish()
        .await
        .expect("the ask reaches the asking handler");
    tb.settle().await.expect("the exchange settles");

    tb.broker_named("exchange")
        .subscriber("greeter")
        .assert_called_once()
        .with(&Greeting {
            who: "world".to_owned(),
        });
    tb.broker_named("heard")
        .subscriber("heard")
        .assert_called_once()
        .with(&Heard {
            text: "hello world".to_owned(),
        });
    tb.shutdown().await.expect("the service stops");
}

/// The round trip of one envelope on the exchange, in process and over sockets.
macro_rules! exchange_round_trips {
    ($module:ident, $envelope:expr) => {
        mod $module {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn on_the_exchange_in_process() {
                let tb = TestApp::start(exchange_app!($envelope))
                    .await
                    .expect("the harness starts");
                a_request_is_answered(tb).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn on_the_exchange_live() {
                let tb = TestApp::start_live(exchange_app!($envelope))
                    .await
                    .expect("the harness starts over loopback sockets");
                a_request_is_answered(tb).await;
            }
        }
    };
}

exchange_round_trips!(frames_exchange, Frames);
exchange_round_trips!(framed_exchange, Framed::new(JsonCodec));
exchange_round_trips!(no_op_exchange, NoOp);

/// A message without an envelope has no place for a header, so a publish that carries one fails
/// instead of losing it: the relay drops the draft, and no note leaves.
async fn a_header_without_an_envelope_is_refused(tb: TestApp<()>) {
    tb.broker_named("relay")
        .message(&Draft { id: 7 })
        .to("drafts")
        .publish()
        .await
        .expect("the draft reaches the relay");
    tb.settle().await.expect("the draft is handled");

    tb.broker_named("relay")
        .subscriber("drafts")
        .assert_called_once()
        .settled(HandlerOutcome::drop());
    tb.broker_named("notes")
        .subscriber("notes")
        .assert_not_called();
    tb.shutdown().await.expect("the service stops");
}

/// The relay reads its drafts in the default layout, and stamps each note with a tenant, which
/// the bare-payload policy of its slot has no place for.
fn stamping_app() -> RustStream {
    let notes = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)).bindable();
    let to_notes = notes.bind(ZmqQueuePublish.envelope(NoOp));
    RustStream::new(AppInfo::new("zmq-envelopes", "0.0.0"))
        .with_broker_labeled("notes", notes, |b| {
            b.include(take_note);
        })
        .with_broker_labeled("relay", ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK)), |b| {
            b.include(stamp).out(Notes, to_notes).build();
        })
}

/// Passes a draft on stamped with a tenant; a refused publish drops the draft.
#[subscriber("drafts")]
async fn stamp(draft: &Draft, Out(notes): Out<impl Publisher, Notes>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert(TENANT, "acme");
    let note = Note {
        id: draft.id,
        tenant: Some("acme".to_owned()),
    };
    match notes.message(&note).with_headers(headers).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::drop(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_header_without_an_envelope_is_refused_in_process() {
    let tb = TestApp::start(stamping_app())
        .await
        .expect("the harness starts");
    a_header_without_an_envelope_is_refused(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_header_without_an_envelope_is_refused_live() {
    let tb = TestApp::start_live(stamping_app())
        .await
        .expect("the harness starts over loopback sockets");
    a_header_without_an_envelope_is_refused(tb).await;
}

/// A plain PUSH peer sends its payload as one frame; a queue reads it as that payload with no
/// headers, whichever envelope it reads through.
macro_rules! a_plain_peer_is_read_as_its_payload {
    ($name:ident, $envelope:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            let connected = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))
                .envelope($envelope)
                .connect()
                .await
                .expect("the queue connects");
            let mut subscriber = connected
                .subscribe("jobs")
                .await
                .expect("the subscription binds");
            let address = connected.bound_address().expect("the subscription bound");
            let mut plain = zeromq::PushSocket::new();
            plain.connect(&address).await.expect("the plain peer dials");
            plain
                .send(zeromq::ZmqMessage::from(r#"{"id":7}"#))
                .await
                .expect("the plain peer sends");

            let mut stream = pin!(subscriber.stream());
            let message = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("the subscription answers")
                .expect("the stream is open")
                .expect("the frame reads as a message");
            assert_eq!(message.payload(), br#"{"id":7}"#);
            assert_eq!(message.headers(), &HeaderMap::new());
            assert_eq!(message.name(), "");
            connected.shutdown().await.expect("the queue shuts down");
        }
    };
}

a_plain_peer_is_read_as_its_payload!(a_plain_peer_is_read_as_its_payload_through_frames, Frames);
a_plain_peer_is_read_as_its_payload!(
    a_plain_peer_is_read_as_its_payload_through_framed,
    Framed::new(JsonCodec)
);
a_plain_peer_is_read_as_its_payload!(a_plain_peer_is_read_as_its_payload_through_no_op, NoOp);
