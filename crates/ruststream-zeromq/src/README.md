`ZeroMQ` transport for `RustStream`: a service joins a `ZeroMQ` topology over a documented frame
layout, so the peer on the other side does not have to be Rust.

There is no server in the middle. Two processes talk directly, and which side listens is a
deployment decision stated on the [`ZmqEndpoint`]. Three socket patterns cover three messaging
shapes over the pure-Rust [`zeromq`](https://docs.rs/zeromq) implementation, on the `tcp://` and
`ipc://` transports.

What the transport does not do is worth knowing before the first line of code. Delivery is at most
once and nothing is stored, so [`ack`](ruststream::IncomingMessage::ack) and
[`nack`](ruststream::IncomingMessage::nack) report
[`AckError::Unsupported`](ruststream::AckError::Unsupported) and a delayed retry is a copy this
service publishes. A fan-out subscriber that attaches after a publisher has started misses what was
sent before it arrived. There is no encryption layer. Order is kept per socket pair and nowhere
else.

The framework itself - handlers, routers, codecs, middleware, the generated `main` - is documented
in [`ruststream`](https://docs.rs/ruststream/latest/ruststream/); this page covers what is
`ZeroMQ` about a service.

# A service

A worker on the PUSH/PULL queue, with the pattern's own prelude as its one import:

```
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    println!("working on job {}", job.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(handle);
        },
    )
}
```

`cargo run -- run` starts it, and any peer that composes the three frames pushes work into it.
Runnable programs for all three patterns are in
[`examples/`](https://github.com/powersemmi/ruststream-zeromq/tree/main/crates/ruststream-zeromq/examples).

# The three patterns

Each pattern is a broker of its own: a synchronous constructor, a connected form, a publish policy
that builds its live publisher, and a subscriber.

| Broker | Sockets | Shape | Publish policy | Where a retry copy goes |
| --- | --- | --- | --- | --- |
| [`ZmqQueue`] | PUSH/PULL | Competing consumers, round-robin: one message reaches one consumer. | [`ZmqQueuePublish`] | Bound: the subscription addresses it. Dialed: the mount site names it. |
| [`ZmqFanout`] | PUB/SUB | Broadcast: one message reaches every subscription whose name is a prefix of it. | [`ZmqFanoutPublish`] | Bound: the subscription addresses it. Dialed: the mount site names it. |
| [`ZmqRpc`] | DEALER/ROUTER | Request and reply, answered per requesting peer. | [`ZmqRpcPublish`] | The mount site names it. |

Each pattern's policy is also the default of its connected form, so a handler mounted with no
publish position still publishes its reply the way its own pattern does.

Moving a service between patterns is an import line: every form prelude offers its policy under the
bare name `Publish`, so the mount site does not change. See [the prelude](#the-prelude).

# Endpoints

[`ZmqEndpoint`] is an address plus the side this process takes on it, because the pattern decides
who receives and the endpoint decides who listens:

* `ZmqEndpoint::bind("tcp://0.0.0.0:5555")` - this process listens.
* `ZmqEndpoint::connect("tcp://ml:5555")` - this process dials out.
* `ZmqEndpoint::bind("ipc:///tmp/orders")` - the same host, no network stack.

Any other scheme is refused when the broker connects, before a socket is opened, and the error
names the address.

The side is a type. `ZmqEndpoint::bind` returns a `ZmqEndpoint<Bind>` and `ZmqEndpoint::connect` a
`ZmqEndpoint<Connect>`, and the broker built on it carries the same [`Bind`] or [`Connect`] as its
`Role` parameter: `ZmqQueue::new(ZmqEndpoint::connect(..))` is a `ZmqQueue<Connect>`. A mount site
writes the side only where it names the type, as in `Router::<ZmqQueue<Connect>>::new()`; the
parameter defaults to [`Bind`]. The side decides where a retry copy of a one-way subscription can
go, and [Retries](#retries) shows the two mounts.

An address with port zero (`tcp://127.0.0.1:0`) leaves the port to the operating system. The port
settles when a subscription binds, and `bound_address()` on the connected form reports it, or
`None` until then. A publisher in the same process dials that resolved address by itself, so a
binary holding both ends runs without a fixed port.

A publisher that dials a subscription of its own service reaches that subscription and nothing
else, because the socket the subscription bound takes whatever is sent to it. On [`ZmqQueue`] that
decides what the publisher may send. A message under the subscription's own name goes through: a
retry copy, or a job the service feeds itself. A message under any other name returns
[`ZmqError::Send`](ZmqError::Send), naming it and the subscription, because the subscription would
receive it as its next delivery. A reply, a result or a dead letter therefore leaves through a
queue on an endpoint of its own; [Replies](#replies) shows the mount.

A subscription that dials reads from the sending end of its pattern - a ventilator's PUSH socket,
a publisher's PUB socket - and that peer takes nothing. Once a subscription of this service has
dialed the endpoint, a publish through the same [`ZmqQueue`] or [`ZmqFanout`] returns
[`ZmqError::Send`](ZmqError::Send), naming the subscription, instead of dialing a peer the
handshake would refuse. A broker that dials either consumes or produces; one that only publishes,
such as a producer dialing a sink, sends as usual.

The lifecycle is the framework's ladder of consuming transitions: `ZmqQueue::new(endpoint)` records
configuration and performs no I/O, `connect` hands back [`ConnectedZmqQueue`], and `shutdown`
consumes that, so subscribing or publishing afterwards does not compile. Sockets attach lazily, per
subscription and per publisher. A publisher handed out earlier shares the same state and reports
[`ZmqError::NotConnected`](ZmqError::NotConnected) once the connected form is gone, rather than
succeeding against nothing.

A send retries for five seconds while the ZMTP handshake settles, because a socket with no peer
attached yet hands the message straight back. A send that finds no peer for the whole window
returns [`ZmqError::Send`](ZmqError::Send), naming the destination.

# Subscribing

A subscription is a name, and that name is frame 0 of every message on the channel. There is no
subscription descriptor in this crate: a name is all any of the three patterns needs, so
`#[subscriber("jobs")]` is the whole declaration and the mount site carries the rest.

Only [`ZmqFanout`] filters on the name, and it filters by prefix, which is the protocol's own rule:
a subscription on `events` also receives `events.created`. [`ZmqQueue`] and [`ZmqRpc`] hand a
subscription every message their socket receives, whatever frame 0 says, so a second kind of work
needs an endpoint of its own.

How many subscriptions a broker takes follows from the side. Subscriptions that dial are sockets
of their own on the peer's endpoint: two [`ZmqQueue`] workers dialing one ventilator split its
stream, and two [`ZmqFanout`] watchers dialing one publisher each receive their own prefix. An
endpoint this service binds is one socket, read by the subscription that bound it. A second
subscription on the same broker is refused when it opens, with an error naming both subscriptions,
and mounts on a broker with an endpoint of its own.

Nothing is stored, so there is no position to return to: neither `Seekable` nor `Positioned` is
implemented and `.start_at(..)` does not compile here. This crate adds no per-delivery context key
either; what a handler reads comes from the framework, plus the `reply-to` header a responder
stamps on each request.

## Batches

A handler that takes a slice receives a whole batch, and the mount site caps the size:

```
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn drain(jobs: &[Job]) -> HandlerOutcome {
    println!("working on a batch of {}", jobs.len());
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(drain.batch(nonzero!(32)));
        },
    )
}
```

A receive on a socket yields one multipart message, so there is no batch receive to pass that size
to. The deliveries are collected on this side instead: a batch closes once it holds the size the
mount named, or 20 ms after its first delivery, whichever comes first. The 20 ms is a constant of
this crate, not a setting.

[`ZmqRpc`] is the exception: `.batch(..)` on a responder does not compile. A responder answers at
the address in each request's own `reply-to` header, while a batch carries one publish context for
all of its replies, so answering a batch would send every reply to one peer. [`ZmqRpcSubscriber`]
implements no [`BatchSubscriber`](ruststream::BatchSubscriber), and the compile error names it.

## Retries

Nothing settles a delivery here, so every retry is a copy this service publishes once the delay a
handler asked for is over. The registration declares how many deliveries a message gets and which
publisher the copies leave through:

```
use std::time::Duration;

use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    if job.id == 0 {
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(handle)
                .max_attempts(nonzero!(5u32))
                .out_retry(Publish);
        },
    )
}
```

Neither `max_attempts` nor `dead_letter` is applied by the transport: ZMTP has no delivery counter
and no dead-letter topology, so the framework counts the copies through its own
`x-ruststream-retry-count` header and republishes a spent delivery where the registration said.
[`ZmqMessage`] reports no `redelivery_count` for the same reason. Declaring the dead-letter
destination alone sends every failed delivery straight there.

A destination is a name, and on the one-way patterns a name is frame 0 rather than an address, so a
dead-letter destination on the subscription's own endpoint does not leave the subscription. On
[`ZmqQueue`] the publisher refuses `jobs.dead` with [`ZmqError::Send`](ZmqError::Send), and the
spent delivery is dropped with a warning. On [`ZmqFanout`] `jobs.dead` matches the prefix `jobs`
and arrives on the subscription that gave up on it, so a handler that keeps failing keeps making
copies. A dead-letter destination belongs to a publisher on another endpoint - `.out_retry(token)`
over a second broker - and the retry copies leave through it as well: they reach that endpoint's
subscription, not the worker's queue, so such a registration trades its retries for dead-lettering.
A handler that should see its retries again uses `.out_retry(policy)` on its own endpoint and no
dead-letter destination.

Where a copy goes is a property of the pattern and, on the one-way patterns, of the side, and the
type states it. A [`ZmqQueue`] or [`ZmqFanout`] subscription that binds addresses itself, so
`.out_retry(policy)` binds the publisher and names nothing: a retry on the queue goes back into the
queue and whichever worker is free takes it, and a retry on the fan-out reaches the audience the
original had.

A subscription that dials addresses nothing, because the peer it reads from only sends. Its mount
site names where the copies go. A worker behind a ventilator sends them to a queue it binds itself,
where the same handler takes them:

```
use std::time::Duration;

use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    if job.id == 0 {
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let copies = retries.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")), |b| {
            b.include(handle)
                .max_attempts(nonzero!(5u32))
                .out_retry(copies)
                .to("jobs");
        })
        .with_broker(retries, |b| {
            b.include(handle).max_attempts(nonzero!(5u32));
        })
}
```

The copies carry the framework's counter, so the cap holds across both subscriptions. A
registration on a subscription that dials and names no destination is refused before the
subscription opens, with an error naming the subscription and the fix, and a
[`Router`](ruststream::runtime::Router) chain that names none does not compile. That includes a
registration that never asks for a retry, because the runtime pairs the publisher either way. The
worker's own queue is no destination: its publisher refuses every publish once the subscription has
dialed, as [Endpoints](#endpoints) describes.

[`ZmqRpc`] addresses nothing, because a copy of a request has no address of its own: replies route
to the peer identity the request carried, and the reply publisher refuses a plain name. Every
responder registration therefore names the destination itself, with `.out_retry(policy).to("name")`
or with a transform that names one per delivery, and one that names neither is refused before the
subscription opens. That includes a registration that never asks for a retry, because the runtime
pairs the publisher either way. A plain name reaches nothing on DEALER/ROUTER, so a service that
means the copies to arrive binds a queue on another broker there, and a service that does not names
a destination to say so and lets the requester ask again.

The retry position reads the delivery it is retrying, so a `.transform(..)` there receives a
[`PublishContext`](ruststream::runtime::PublishContext): the subscription the delivery arrived on,
its headers, its context. That is where a service marks a redelivery on a transport that settles
nothing. The copy carries the delivery's own bytes, so a codec named at that position encodes
nothing.

# Publishing

Publishing runs the framework's
[publish pipeline](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#the-publish-pipeline)
unchanged, and this crate adds no step of its own. Each pattern ships one policy, offered as
`Publish` by its form prelude: `.out_reply(Publish)` takes what a `#[subscriber(.., publish)]`
handler returns, and `.out(marker, Publish).build()` does the same for an injected `Out<..>`
publisher.

Bytes a service already holds encoded are the common case here, because the peer on the other side
framed them. Wrap them in a `#[derive(Outgoing, Serialized)]` newtype and publish them through the
same call: no codec runs on them, and the type names a payload that would otherwise be anonymous.

There are no transactions. `ZeroMQ` has none, so no publisher here implements
[`TransactionalPublisher`](ruststream::TransactionalPublisher) and `.transaction()` does not
compile. There is no broker-side partitioning either: PUSH/PULL round-robins across the attached
peers without consulting a key.

## Per-message settings

There are none. A ZMTP send takes the frames and nothing else - no priority, no expiry, no ordering
key - so every publisher here declares `Options = ()` and no publish builder step comes from this
crate. A handler body therefore imports `ruststream::prelude::*` alone, whichever pattern it runs
on, and bounds an injected publisher with the capability it needs, with nothing of this crate in
its signature.

Two things that look like settings are not. Which peer a reply reaches is a destination, and a
publish transform declaring `Destination = Names` supplies it per delivery. How long a send waits
for the ZMTP handshake is a constant of this crate, the same for every message.

## Replies

A reply type that owns its destination names it, and the subscriber clause stays bare. On
[`ZmqQueue`] the reply leaves through a queue on an endpoint of its own, here one a sink binds:

```
use ruststream_zeromq::queue::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish)]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let to_results = results.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
            b.include(work).out_reply(to_results);
        })
        .with_broker(results, |_b| {})
}
```

The jobs arrive on the socket the `jobs` subscription binds, and that socket takes every message
pushed into it. A result published there would come back as the next job, so the worker's own
publisher refuses it, and the mount binds the reply to the second broker instead.

A type that names no destination takes the mount site's, `#[subscriber("jobs", publish("results"))]`.
Both forms reach the generated document as the resolved destination, so a declaration cannot
document one channel and publish to another.

## Request and reply

[`ZmqRpc`] covers both ends of DEALER/ROUTER.

The responder is an ordinary reply handler. Its ROUTER socket stamps a `reply-to` header on each
request, addressing the peer that sent it, and a publish transform rewrites the reply destination to
that address. The answer is addressed per request, so its type declares no destination of its own
and the name in the `publish("..")` clause is the fallback: what the document reports, and where a
delivery the transform left alone is answered. A transform that picks a destination declares
`Destination = Names`, and a position offers that right only where nothing has declared one already,
so a named reply type and a naming transform do not compile together. The address travels from the
request's own buffer: [`HeaderMap::get_shared`](ruststream::HeaderMap::get_shared) hands the header
over as a counted handle and [`Str`](ruststream::Str) checks it is text, so answering where the
request asked costs a reference count rather than a copy.

The requester uses the [`RequestReply`](ruststream::RequestReply) capability on
[`ZmqRpcPublisher`]: `request(msg, timeout)` sends over a DEALER socket of its own and returns the
answer, matched by the `correlation-id` header. A correlation id set on the request is kept, so an
upper layer can match on its own identifier. Nothing answering in time is a timeout error, not a
hang.

An answer whose requester has gone is refused as well, naming the reply address it could not
reach: the ROUTER holds no peer under that identity. A responder therefore learns that its answer
arrived nowhere, which is the opposite of the fan-out, where an unmatched message is dropped
without a word.

```
use std::io;
use std::time::Duration;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{ForReply, Names, Outgoing, PublishContext, PublishTransform};
use ruststream::{IncomingMessage, OutgoingMessage, Str};
use ruststream_zeromq::rpc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct Greeting {
    who: String,
}

// No destination of its own: the ROUTER addresses each answer per request.
#[derive(Deserialize, Serialize, ruststream::Outgoing)]
struct Answer {
    text: String,
}

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
            out.headers_mut().insert(Str::from_static("correlation-id"), correlation);
        }
    }
}

// The literal destination is the fallback the transform replaces per delivery.
#[subscriber("greeter", publish("reply"))]
async fn greet(request: &Greeting) -> Answer {
    Answer {
        text: format!("hello {}", request.who),
    }
}

#[ruststream::app]
fn app() -> impl App {
    // Port zero binds an ephemeral port; the requester dials whatever it resolved to.
    RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
        ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            // A responder addresses no retry copies, so the mount says where they go.
            b.include(greet)
                .out_reply(Publish)
                .transform(ReplyToRequester)
                .out_retry(Publish)
                .to("greeter.retry");

            b.after_startup(Publish, async move |publisher| -> io::Result<()> {
                let request = JsonCodec
                    .encode(&Greeting {
                        who: "world".to_owned(),
                    })
                    .map_err(io::Error::other)?;
                let answer = publisher
                    .request(
                        OutgoingMessage::new("greeter", request.as_ref()),
                        Duration::from_secs(5),
                    )
                    .await
                    .map_err(io::Error::other)?;
                let answer: Answer = JsonCodec
                    .decode(answer.payload())
                    .map_err(io::Error::other)?;
                println!("reply: {}", answer.text);
                Ok(())
            });
        },
    )
}
```

# The wire contract

The frame layout is public, because the peer on the other side composes messages by hand:

```text
frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   encoded by the framework's codec
```

A Python peer pushes work into a [`ZmqQueue`] consumer with
`socket.send_multipart([b"jobs", b"content-type: application/json", payload])`.

On [`ZmqRpc`] a reply is framed differently: frame 0 holds the literal `reply`, and a ROUTER
identity frame in front of it addresses the peer that asked.

Headers are text, and nothing is guessed in either direction. A two-frame message from a minimal
peer reads as headerless, and a blank line inside the header frame is skipped, so a peer that ends
its lines with a newline interoperates. Publishing a header value that is not UTF-8 returns an error
naming the header, because a text frame cannot hold it. A header frame that is not UTF-8, a name
frame that is not UTF-8, and a header line with no `:` each return
[`ZmqError::Wire`](ZmqError::Wire) rather than losing the header silently.

The payload frame is whatever the framework's codec produced, so the peer only has to agree on the
codec: with the default JSON codec it is the document a handler's input type deserializes from.

# The prelude

A routes file on one pattern imports that pattern's prelude - [`queue::prelude`],
[`fanout::prelude`] or [`rpc::prelude`] - and gets the framework's prelude, [`ZmqEndpoint`], the
pattern's broker type, and its publish policy under the bare name `Publish`. [`rpc::prelude`] adds
[`RequestReply`](ruststream::RequestReply), which only that pattern implements.

A file that mounts more than one pattern imports [`prelude`] instead. Three forms cannot share one
bare `Publish` in a single glob, so that one carries the prefixed names [`ZmqQueuePublish`],
[`ZmqFanoutPublish`] and [`ZmqRpcPublish`], plus the three broker types and the three form modules.

Two vocabularies, two files. A mount site names a policy and wants one of these globs; a handler
body bounds its injected publisher with a framework capability trait and imports
`ruststream::prelude::*` alone. The exception other broker crates make - importing the broker's
prelude into a handler body to adjust a per-message setting - does not arise here, because this
transport has no setting to adjust.

# The generated document

With the `asyncapi` feature a service describes its `ZeroMQ` side in the generated document. The
specification has no `ZeroMQ` binding and its protocol keys are a closed list, so everything this
crate reports travels in one extension, `x-ruststream-zeromq`, beside the standard keys.

The server says how to attach: the transport, the coordinate, which side of the endpoint this
service takes, and `3.0`, the ZMTP version the implementation greets every peer with. The
coordinate is the credential-free one [`DescribeServer`](ruststream::DescribeServer) reports, so a
password an operator wrote into the endpoint is dropped with the scheme and never reaches a
document that is published and shared.

A channel reports the socket pair its messages travel over, and on the one-way patterns also
`nameFrame`, the value frame 0 holds: a peer reads from the document both what to send to reach the
channel and, on PUB/SUB, what prefix to subscribe with. A responder reports no `nameFrame`, because
a reply travels to the identity the ROUTER supplies and the channel's own name addresses nothing.
Its reply channel has no address either, so the document points a client at the `reply-to` header
instead.

What the document does not carry: no subscription binding, because bindings come from a
subscription descriptor and none of the three patterns has one; no operation or message bindings,
because this crate has no producer setting that varies and the frame layout is a constant of the
crate rather than of a channel.

# Testing

A test hands the harness the app `main` runs, and
[`TestApp::start`](https://docs.rs/ruststream/latest/ruststream/testing/index.html) connects each
broker of this crate in process: the production broker, its connected form and its publish
policies, over a transport inside the test process instead of ZMTP sockets. A test addresses a
broker by its production type, `tb.broker::<ZmqQueue>()`, or by the label it was mounted under.
The in-process mode comes with the `testing` feature, which a service enables in its
`[dev-dependencies]`.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_zeromq::Connect;
use ruststream_zeromq::queue::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish)]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

/// The app `main` runs, and the one the tests hand the harness.
pub fn app() -> RustStream {
    let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let to_results = results.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
            b.include(work).out_reply(to_results);
        })
        .with_broker(results, |_b| {})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_job_is_worked_and_its_result_published() {
    let tb = TestApp::start(app()).await.expect("the app starts");

    tb.broker::<ZmqQueue>()
        .message(&Job { id: 7 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is worked");

    tb.broker::<ZmqQueue<Connect>>()
        .published::<Done>("results")
        .assert_called_once()
        .with(&Done { id: 7 });

    tb.shutdown().await.expect("the app shuts down");
}
# }
# fn main() {}
```

The in-process transport carries the frames a socket carries. A publish is framed the way the
socket publisher frames it, and a subscription reads the frames the way its socket does, so a
header value the text frame cannot hold is refused and a request arrives stamped with the peer
that sent it. It has no settings of its own: the side of each endpoint comes from the broker's
type, and every refusal is the socket's, in the socket's words.

Which subscriptions a message reaches is the pattern's own rule:

* Queue. A subscription that binds takes whatever a peer pushes to its endpoint, whatever the name
  frame says. Subscriptions that dial take a peer's pushes one at a time, in turn.
* Fan-out. Every subscription whose name is a prefix of the message's name, on either side.
* Request and reply. A request reaches the responder that binds, or one of the responders that
  dial, in turn. The answer reaches the peer that asked and no subscription.
* A publish of this service reaches its own subscription only through the listener that
  subscription binds. On an endpoint the service dials, or binds with no subscription on it, the
  message leaves for a peer in another process: in process it is recorded in `published` and
  reaches no subscription.

A test's input reaches a broker the way a foreign peer sends it. The service's own publishers and
subscriptions meet the socket's refusals in process: a publish a bound queue would hand back to its
own subscription, any publish on an endpoint a subscription dials, a second subscription on an
endpoint the broker binds, an answer to a peer that is gone. Settlement is the socket's as well:
`ack` and `nack` report [`AckError::Unsupported`](ruststream::AckError::Unsupported) and nothing
comes back, so a handler that settles by retrying fails its test instead of losing its message in
production.

[`TestApp::start_live`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.start_live)
runs the same test body over real sockets. `ZeroMQ` needs no server, so a live test binds loopback
ports and runs in every `cargo test`. Live, a test's input leaves through the broker's own
publisher, so on a bound queue it carries the subscription's own name. Delivery guarantees,
back-pressure and the fan-out's slow joiner belong to the sockets: a live fan-out test publishes
until the filter has reached the publisher, and an in-process one needs no such loop.

# Operations

* Transports: `tcp://` and `ipc://`. Anything else is refused when the broker connects.
* Authentication and encryption: none. The implementation carries no CURVE or ZAP layer, so run the
  service on a trusted network or inside an existing tunnel.
* Connection settings: the endpoint and its role, and nothing else. The handshake retry window
  (five seconds), the batch deadline (20 ms) and the ZMTP version are constants of this crate.
* Back-pressure: a subscription reads at most 1000 deliveries ahead of its handler, or the bound
  its descriptor sets with `read_ahead` ([`ZmqQueue::read_ahead`], [`ZmqFanout::read_ahead`],
  [`ZmqRpc::read_ahead`]). Past it the subscription stops reading, the socket buffers fill, and the
  sender waits, so a slow handler slows the sender down instead of growing this process's memory.
  On PUB/SUB the waiting sender is the PUB socket, held back by its slowest matching subscriber; a
  message that matches no subscriber is still dropped without an error.
* Cancel safety: a queue publish is cancel-safe. One dropped mid-send, while the peer applies
  back-pressure, leaves its message with the publisher, and the next publish completes that send
  before its own. A fan-out publish and a reply are not: dropping one can leave a message
  half-handed to the socket. Dropping a `request` future closes the DEALER the request went out on.
  Publish those from a task of its own and give up through the request timeout, not by cancelling
  a `select!` arm.
* Durability: none. Nothing is stored, so a subscriber that attaches late has no backlog to read
  and a restart replays nothing.
