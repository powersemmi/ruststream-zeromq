# ZeroMQ

`ruststream-zeromq` is the ZeroMQ transport, built on the pure-Rust
[`zeromq`](https://docs.rs/zeromq) implementation over the TCP and IPC transports. There is no
server in the middle: two processes talk to each other directly, and the frame layout on the wire
is part of the crate's public contract so a non-Rust peer can take either side. For framework
concepts (writing subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

Which of the framework's optional capability traits this transport implements natively, and where
acknowledgement lands:

| Capability | Native | Reason |
| --- | --- | --- |
| `Subscribe` | Yes | All three connected forms open a subscription by name, and the name is the first frame on the wire. See [The three patterns](#the-three-patterns). |
| Acknowledgement (`ack` / `nack`) | No | Both report `AckError::Unsupported`. Delivery is at most once: once a socket has handed a message over, no protocol frame exists to settle it, and there is no store to redeliver from, so emulating a result would report a guarantee the transport does not provide. |
| `BatchSubscriber` | No | A socket yields one multipart message at a time; there is no batch receive. |
| `TransactionalPublisher` | No | ZeroMQ has no transactions. |
| `OwnedTransactions` | No | ZeroMQ has no transactions. |
| `RequestReply` | Yes, on `ZmqRpc` | `ZmqRpcPublisher` implements it over DEALER/ROUTER: a request goes out on a DEALER socket and the answer is matched by the `correlation-id` header, or the call fails on timeout. `ZmqQueue` and `ZmqFanout` are one-way patterns with no return path, so their publishers do not implement it. See [Request and reply](#request-and-reply). |
| `Partitioned` | No | There is no broker-side partitioning. PUSH/PULL round-robins across attached peers without consulting a key. |
| `Seekable` / `Positioned` | No | The transport keeps no history. Nothing is stored, so there is no position to return to. |
| `DescribeServer` | Yes | Each of the three brokers reports its endpoint address and the `zeromq` protocol, which is what the AsyncAPI schema records. |

## Scope

Stated up front rather than discovered:

- Delivery is at most once and there is no durability. Acknowledgement reports
  `AckError::Unsupported` on both `ack` and `nack`, never emulated.
- A fan-out subscriber that attaches after a publisher has started misses what was sent before it
  arrived (the slow joiner), and a fan-out message with no matching subscriber is dropped silently.
- There is no high-water-mark configuration. A slow reader exerts raw TCP back-pressure on senders,
  except on fan-out, where unmatched messages are dropped by the pattern.
- There is no encryption layer: trusted networks, or an existing tunnel.
- No consumer groups, no dead-lettering, no retry policies, no transactions.

## The three patterns

Each pattern is a broker in its own right, with its own connected form, publish policy, and live
publisher:

| Broker | Sockets | Shape | Publish policy |
| --- | --- | --- | --- |
| `ZmqQueue` | PUSH/PULL | Competing consumers, round-robin: each message reaches one consumer. | `ZmqQueuePublish` |
| `ZmqFanout` | PUB/SUB | Broadcast: each message reaches every subscriber whose name prefix matches. | `ZmqFanoutPublish` |
| `ZmqRpc` | DEALER/ROUTER | Request and reply. | `ZmqRpcPublish` |

Each policy is also the `DefaultPublish` policy of its connected form, so a
`#[subscriber(.., publish("dest"))]` handler mounted without an explicit publisher sends through
it.

A subscription is named, and the name is the first frame on the wire. For `ZmqFanout` that name is
also the subscription prefix the socket filters on, so a subscriber on `events` receives
`events.created` as well.

Each pattern ships its own prelude, and that is the one import a service file needs:
`ruststream_zeromq::queue::prelude::*`, `fanout::prelude::*` or `rpc::prelude::*`. It carries the
framework's own prelude, the endpoint, the pattern's descriptor, and the pattern's publish policy
under the name `Publish`; `rpc::prelude` also carries `RequestReply`.

A file that names more than one pattern imports `ruststream_zeromq::prelude::*` and qualifies:
`queue::Publish`, `rpc::Publish`.

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:handler"
```

Wiring the handler onto a pattern is identical to any other broker:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:app"
```

## Endpoints

`ZmqEndpoint` is an address plus an explicit role, because with no server in the middle which side
listens is a deployment decision rather than a property of the transport:

- `ZmqEndpoint::bind("tcp://0.0.0.0:5555")` - this process listens.
- `ZmqEndpoint::connect("tcp://ml:5555")` - this process dials out.
- `ZmqEndpoint::bind("ipc:///tmp/orders")` - same host, no network stack.

Only the `tcp://` and `ipc://` transports are served; anything else is rejected by the descriptor
before any I/O, with a message naming the address.

The role is independent of the direction messages travel. A `ZmqQueue` consumer can bind and let
producers dial in, or dial out to a producer that binds; the pattern decides who receives, the
endpoint decides who listens.

### Ephemeral binds

An address with port zero (`tcp://127.0.0.1:0`) leaves the port to the operating system. It
resolves when a subscription attaches its socket, and `bound_address()` on the connected form
reports the concrete address it settled on, or `None` while no subscription has bound yet.

A publisher in the same process dials that resolved address automatically, which makes a
self-contained service - responder and requester in one binary - work without a fixed port. The
request/reply example below relies on it.

## The lifecycle

Every pattern is a ladder of consuming transitions, so each state is a distinct type:

```text
ZmqQueue::new(endpoint)     configuration only, synchronous, no I/O
  .connect()   ->  ConnectedZmqQueue   sockets attach lazily per subscription and publisher
  .shutdown()             ->           the terminal witness; aliased handles trip the closed flag
```

`new` performs no I/O, so a ZeroMQ service is assembled with the same `#[ruststream::app]` macro as
any other broker. Because `shutdown` consumes the connected form, publishing or subscribing after
it does not compile. A publisher handed out earlier still aliases the same state and reports
`ZmqError::NotConnected` once it is closed rather than sending into a dead socket.

Sends retry for a bounded window while the ZMTP handshake settles. The implementation hands a
message straight back when no peer is attached yet, which is routine immediately after connecting,
so nothing is lost by retrying; a send that finds no peer for the whole window fails with a
`ZmqError::Send` naming the destination.

## The wire contract

The frame layout is public and stable across versions, because the peer on the other side composes
messages by hand:

```text
frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   encoded by the framework's codec
```

A Python peer pushes work into a `ZmqQueue` consumer with:

```python
socket.send_multipart([b"jobs", b"content-type: application/json", payload])
```

Headers are text. A message carrying none leaves an empty header frame, and a two-frame message
from a minimal peer reads as headerless, so the simplest possible producer interoperates. A name
frame that is not UTF-8 is a wire error rather than a lossy guess.

Because the payload frame is whatever the framework's codec produced, the peer only has to agree on
the codec: with the default JSON codec, `payload` is the JSON document a handler's input type
deserializes from.

## Publishing

Every publish runs through the framework's publish builder, whichever surface holds the publisher:
`message(..)` for a value, `raw(..)` for bytes, with the destination, the headers and the codec
resolved from the most specific level that names one. This transport adds no step of its own to
that chain, so the framework's own publishing guide applies unchanged.

## Request and reply

`ZmqRpc` covers both ends of DEALER/ROUTER.

The requester side uses the framework's `RequestReply` capability on `ZmqRpcPublisher`:
`request(msg, timeout)` sends over a DEALER socket and resolves with the answer, correlated by the
`correlation-id` header, or fails with a timeout when nothing answers in time. A caller-supplied
correlation id is respected, so an upper layer can match on its own identifier.

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:request"
```

The responder side is an ordinary reply handler. The ROUTER socket stamps each incoming request
with a `reply-to` header addressing the peer that sent it, and a publish transform rewrites the
reply destination to that address, so the answer routes back to the requester instead of to the
literal destination in the decorator:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:transform"
```

Mounting the handler with that transform on its publisher is the whole wiring:

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:responder"
```

The runnable program is
[`examples/zmq_request_reply.rs`](https://github.com/powersemmi/ruststream-zeromq/blob/main/crates/ruststream-zeromq/examples/zmq_request_reply.rs) -
responder and requester in one process, over an ephemeral bind.

## Testing

The `testing` feature ships `ZmqTestBroker`: an in-process stand-in that reproduces the crate's core
routing with no sockets and no network. It follows the same ladder as the real patterns, and its
connected form implements `ruststream::testing::TestableBroker`, so the same broker drives the
`TestApp` harness and the framework's conformance suite; inject traffic with
`broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

Socket-level behaviour needs no external service. The conformance routing suite, the lifecycle
ladder, the request/reply capability, and a wire-layout check driven by a raw foreign-style peer all
run on loopback sockets, so `just test` covers the whole crate with nothing to start first.
