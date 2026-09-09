# ZeroMQ

`ruststream-zeromq` connects a RustStream service to a ZeroMQ topology, over the pure-Rust
[`zeromq`](https://docs.rs/zeromq) implementation on the TCP and IPC transports. There is no server
in the middle: two processes talk to each other directly. The frame layout is part of the crate's
public contract, so a non-Rust peer can take either side. For framework concepts (writing
subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

Which of the framework's optional capability traits this transport implements:

| Capability | Native | Reason |
| --- | --- | --- |
| `Subscribe` | Yes | All three connected forms subscribe by name. See [The three patterns](#the-three-patterns). |
| Acknowledgement (`ack` / `nack`) | No | Both return `AckError::Unsupported`. Delivery is at most once: once a socket has handed a message over, no protocol frame settles it and there is no store to redeliver from. |
| `BatchSubscriber` | Client-side, on the one-way patterns | A socket has no batch receive, so `ZmqQueue` and `ZmqFanout` assemble the batches in the client, to the size the mount site named. `ZmqRpc` does not implement it at all, deliberately: `.batch(..)` on a responder does not compile. See [Batches](#batches). |
| `TransactionalPublisher` | No | ZeroMQ has no transactions. |
| `OwnedTransactions` | No | ZeroMQ has no transactions. |
| `RequestReply` | Yes, on `ZmqRpc` | `ZmqRpcPublisher` implements it over DEALER/ROUTER, matching the answer by the `correlation-id` header. `ZmqQueue` and `ZmqFanout` are one-way patterns with no return path, so their publishers do not implement it. See [Request and reply](#request-and-reply). |
| `Partitioned` | No | There is no broker-side partitioning. PUSH/PULL round-robins across the attached peers without consulting a key. |
| `Seekable` / `Positioned` | No | Nothing is stored, so there is no position to return to. |
| `DescribeServer` | Yes | Each pattern reports its endpoint address and the `zeromq` protocol, which is what the AsyncAPI schema records. |

## Scope

- Delivery is at most once, and nothing is stored.
- A fan-out subscriber that attaches after a publisher has started misses what was sent before it
  arrived (the slow joiner).
- There is no high-water-mark setting. A slow reader exerts raw TCP back-pressure on senders, and a
  fan-out message that no subscriber matches is dropped without an error.
- There is no encryption layer, so run the service on a trusted network or inside an existing
  tunnel.

## The three patterns

Each pattern is a broker of its own: a connected form, a publish policy that constructs its live
publisher, and its own subscriber.

| Broker | Sockets | Shape | Publish policy |
| --- | --- | --- | --- |
| `ZmqQueue` | PUSH/PULL | Competing consumers, round-robin: each message reaches one consumer. | `ZmqQueuePublish` |
| `ZmqFanout` | PUB/SUB | Broadcast: each message reaches every subscriber whose name prefix matches. | `ZmqFanoutPublish` |
| `ZmqRpc` | DEALER/ROUTER | Request and reply. | `ZmqRpcPublish` |

The mount site names one of those policies: `.out(Reply, policy)` publishes what a
`#[subscriber(.., publish)]` handler returns, and `.out(marker, policy)` does the same for an
injected `Out<..>` publisher. Each pattern's policy is also the default of its connected form, so a
handler mounted without `.out` publishes its reply through it anyway.

A subscription is named, and the name is the first frame. `ZmqFanout` filters on it: the name is
the subscription prefix, so a subscriber on `events` also receives `events.created`. `ZmqQueue` and
`ZmqRpc` hand a subscription every message its socket receives, whatever the first frame says. Two
`ZmqQueue` subscriptions on one endpoint therefore split one stream of work between them, and a
second kind of work needs its own endpoint.

Each pattern ships its own prelude, and that is the one import a routes file needs:
`ruststream_zeromq::queue::prelude::*`, `fanout::prelude::*` or `rpc::prelude::*`. It re-exports the
framework's prelude, the endpoint, the pattern's descriptor and the pattern's publish policy under
the name `Publish`; `rpc::prelude` adds `RequestReply`. Every pattern names its policy `Publish`, so
moving a service between patterns changes the import line and nothing at the mount site.

Two vocabularies, two files. The mount site names a policy; a handler bounds its injected publisher
with a broker capability trait (`Publisher`, `RequestReply`) and imports `ruststream::prelude::*`
alone.

A file that mounts more than one pattern imports `ruststream_zeromq::prelude::*` instead. That glob
re-exports the three descriptors and the policies under their prefixed names, `ZmqQueuePublish`,
`ZmqFanoutPublish` and `ZmqRpcPublish`.

A worker on the queue pattern, with the pattern's prelude as its one import:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:handler"
```

The mount site names the pattern and includes the handler:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:app"
```

## Batches

A handler that takes a slice receives a whole batch of jobs, and the mount site caps the size:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_batches.rs:handler"
```

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_batches.rs:app"
```

A receive on a socket yields one multipart message, so there is no batch receive to pass that size
to. The deliveries are collected in the client instead: a batch closes once it holds the size the
mount named, or 20 ms after its first delivery, whichever comes first. The 20 ms is fixed, not a
setting.

`ZmqRpc` is the exception: `.batch(..)` on a responder registration does not compile. A responder
answers at the address in each request's own `reply-to` header, while a batch has a single publish
context for all of its replies, so answering a batch would send every reply to one peer.
`ZmqRpcSubscriber` implements no `BatchSubscriber`, and the compile error names it.

## Endpoints

With no server in the middle, which side listens is a deployment decision rather than a property of
the transport. `ZmqEndpoint` is an address plus an explicit role:

- `ZmqEndpoint::bind("tcp://0.0.0.0:5555")` - this process listens.
- `ZmqEndpoint::connect("tcp://ml:5555")` - this process dials out.
- `ZmqEndpoint::bind("ipc:///tmp/orders")` - same host, no network stack.

`ZmqEndpoint` serves the `tcp://` and `ipc://` transports. Connecting a pattern on any other address
returns an error before any socket is opened, and the message names the address.

The role is independent of which way messages go. A `ZmqQueue` consumer can bind and let producers
dial in, or dial out to a producer that binds: the pattern decides who receives, the endpoint
decides who listens.

### Ephemeral binds

An address with port zero (`tcp://127.0.0.1:0`) leaves the port to the operating system. The port is
settled when a subscription binds its socket, and `bound_address()` on the connected form returns
the concrete address, or `None` until then.

A publisher in the same process dials that resolved address by itself, so a service that holds both
ends - responder and requester in one binary - runs without a fixed port. The request and reply
example below relies on it.

## The lifecycle

Every pattern is a ladder of consuming transitions, and each state is a distinct type:

```text
ZmqQueue::new(endpoint)     configuration only, synchronous, no I/O
  .connect()   ->  ConnectedZmqQueue   sockets attach lazily per subscription and publisher
  .shutdown()             ->           the terminal witness; aliased handles trip the closed flag
```

A ZeroMQ service is assembled by the synchronous `#[ruststream::app]` builder. Because `shutdown`
consumes the connected form, publishing or subscribing after it does not compile. A publisher handed
out earlier shares that state, so it returns `ZmqError::NotConnected` after shutdown.

A send retries for five seconds while the ZMTP handshake settles: a socket with no peer attached yet
hands the message straight back. A send that finds no peer for the whole window returns
`ZmqError::Send`, naming the destination.

## The wire contract

The frame layout is public and stable across versions: the peer on the other side composes messages
by hand.

```text
frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   encoded by the framework's codec
```

On `ZmqRpc` a reply is framed differently: frame 0 holds the literal `reply`, and a ROUTER identity
frame in front of it addresses the peer that asked.

A Python peer pushes work into a `ZmqQueue` consumer with:

```python
socket.send_multipart([b"jobs", b"content-type: application/json", payload])
```

Headers are text. A message that carries none leaves the header frame empty, a two-frame message
from a minimal peer reads as headerless, and a blank line inside the frame is skipped, so a peer
that ends its lines with a newline interoperates.

Nothing is guessed in either direction. Publishing a header value that is not UTF-8 returns an
error naming the header, because a text frame has no way to hold it. A header frame that is not
UTF-8 returns a wire error, and so does a line with no `:` between the name and the value. A name
frame that is not UTF-8 returns a wire error as well.

The payload frame is whatever the framework's codec produced, so the peer only has to agree on the
codec: with the default JSON codec, `payload` is the JSON document a handler's input type
deserializes from.

## Publishing

Publishing runs the framework's publish builder, and this transport adds no step of its own.
`message(..)` takes the value; the destination, the headers and the codec resolve from the most
specific level that names one. The framework's own publishing guide applies unchanged.

Bytes a service already holds encoded are the common case here, because the peer on the other side
framed them. You wrap them in a `#[derive(Outgoing, Serialized)]` newtype and publish them through
the same `message(..)` call. No codec runs on them, and the type names a payload that would
otherwise be anonymous.

## Request and reply

`ZmqRpc` covers both ends of DEALER/ROUTER.

The requester side uses the `RequestReply` capability on `ZmqRpcPublisher`. `request(msg, timeout)`
sends over a DEALER socket and returns the answer, matched by the `correlation-id` header. When
nothing answers in time it returns a timeout error instead. You can set the correlation id on the
request yourself and the transport keeps it, so an upper layer can match on its own identifier.

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:request"
```

The responder side is an ordinary reply handler. The ROUTER socket adds a `reply-to` header to each
request, addressing the peer that sent it, and a publish transform rewrites the reply destination to
that address. An answer is addressed per request, so its type declares no destination of its own and
the name in the `publish("..")` clause is the placeholder the transform replaces:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:transform"
```

The mount site names that policy at the reply position and attaches the transform to it:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:responder"
```

The runnable program is
[`examples/zmq_request_reply.rs`](https://github.com/powersemmi/ruststream-zeromq/blob/main/crates/ruststream-zeromq/examples/zmq_request_reply.rs) -
responder and requester in one process, over an ephemeral bind.

## Testing

The `testing` feature ships `ZmqTestBroker`: an in-process stand-in that reproduces the crate's core
routing with no sockets and no network. It follows the same ladder as the real patterns. It delivers
one message at a time and assembles batches in the client exactly as `ZmqQueue` does, so a batch
handler that runs in production also runs under the harness.

Drive it through the `TestApp` harness. `TestApp::start(app)` connects the app's brokers in process,
and `tb.broker::<ZmqTestBroker>()` on the started harness is this transport's handle. From that
handle, `.message(&job).to("jobs").publish()` puts a job in,
`.subscriber("jobs").assert_called_once().with(&job)` asserts what the handler received, and
`.published::<Done>("results").assert_called_once().with(&done)` asserts what a publishing handler
sent. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

Socket-level behaviour needs no external service either: the crate's own suites run on loopback
sockets, so `just test` covers the whole crate with nothing to start first.
