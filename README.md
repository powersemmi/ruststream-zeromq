<h1 align="center">ruststream-zeromq</h1>

<p align="center">
  <i>The ZeroMQ transport for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: typed handlers and codecs over sockets shared with Python, C++, and other non-Rust peers.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-zeromq/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-zeromq/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-zeromq"><img src="https://img.shields.io/crates/v/ruststream-zeromq.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-zeromq"><img src="https://img.shields.io/crates/dr/ruststream-zeromq" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-zeromq"><img src="https://img.shields.io/docsrs/ruststream-zeromq" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-zeromq/">Documentation</a></b>
</p>

---

`ruststream-zeromq` implements the RustStream broker contract over the pure-Rust [`zeromq`](https://crates.io/crates/zeromq) implementation (TCP and IPC transports). Unlike the other RustStream broker crates, there is no server in the middle: a Rust service can join a ZeroMQ topology an existing Python worker or C++ daemon already speaks, without dropping out of the framework.

## Patterns

Three socket patterns cover three messaging shapes:

- **`ZmqQueue`** - PUSH/PULL: competing consumers, round-robin.
- **`ZmqFanout`** - PUB/SUB: broadcast, prefix filtering by name.
- **`ZmqRpc`** - DEALER/ROUTER: request and reply. One publisher covers both directions: `RequestReply::request` issues a request over its own DEALER socket, and a plain publish routes a reply back through the responder's ROUTER, addressed by the `reply-to` header the ROUTER stamps on the request.

A socket hands over one message per receive, so a `&[T]` batch handler on `ZmqQueue` or `ZmqFanout` is served by assembling its batches on the client, to the size the mount site names (`b.include(drain.batch(nonzero!(32)))`). `ZmqRpc` does not batch: a batch carries one publish context for all of its replies, and a responder answers each requester at its own `reply-to` address, so `.batch(..)` there is a compile error rather than a run of misrouted replies.

Because there is no server, the role is explicit - which side listens is a deployment decision:

```rust
use ruststream_zeromq::ZmqEndpoint;

let listener = ZmqEndpoint::bind("tcp://0.0.0.0:5555");   // this process listens
let dialer = ZmqEndpoint::connect("tcp://ml:5555");       // this process dials out
let local = ZmqEndpoint::bind("ipc:///tmp/orders");       // same host, no network stack
```

An ephemeral bind (`tcp://127.0.0.1:0`) resolves at subscribe; `bound_address()` reports it, and a same-process publisher dials it automatically (the loopback arrangement).

## The wire contract

The frame layout is part of the crate's public contract, because the peer on the other side composes messages by hand:

```text
frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   encoded by the framework's codec
```

A Python peer sends `socket.send_multipart([b"orders", b"content-type: application/json", payload])`. A two-frame message from a minimal peer reads as headerless. The layout is stable across versions.

The payload frame is whatever the framework's codec produced, so the peer only has to agree on the codec. Bytes a service already holds framed - the common case when the foreign peer chose the encoding - skip the codec entirely: a `#[derive(Outgoing, Serialized)]` newtype travels through the same `message(..).publish()` call and reaches the wire untouched.

## Scope and limits

- Delivery is **at most once** and there is no durability; acknowledgement is reported as `AckError::Unsupported`, never emulated.
- A subscriber that connects after a publisher has started **misses what was sent before it arrived** (the slow joiner), and a fan-out message with no matching subscriber is dropped silently.
- The implementation exposes **no high-water-mark configuration**: a slow reader exerts raw TCP back-pressure on senders.
- There is **no encryption layer**: use it on trusted networks, or inside an existing tunnel.
- No consumer groups, no dead-lettering, no retry policies, no transactions.
- **Nothing settles a delivery**, so a `retry_after` is served only by the copy the runtime publishes through the `retry_via` publisher. The one-way patterns report the subscription name as the address for it; a `ZmqRpc` responder reports none, and a scope that wires a retry over one is refused at startup.
- **A send takes the frames and nothing else**: no priority, no expiry, no ordering key. Every publisher declares `Options = ()`, this crate adds no publish builder step, and a handler body imports `ruststream::prelude::*` alone.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-zeromq = { version = "0.7", features = ["testing"] }
```

## Write a service

Each pattern ships its own prelude: the framework's, plus `ZmqEndpoint`, the pattern's descriptor, and its publish policy under the bare name `Publish` (`rpc::prelude` adds `RequestReply`). Switching pattern is then an import line, not a rewrite of the mount site:

```rust
use ruststream_zeromq::queue::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish("results"))]
async fn handle(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(handle).out(Reply, Publish);
        },
    )
}
```

`.out(marker, policy)` is the one mount verb: `Reply` names the position the handler's return value goes to, and a slot marker names an injected `Out<..>` publisher. A handler file needs none of this - it imports `ruststream::prelude::*` alone and bounds an injected publisher with a capability trait - which is what leaves the bare `Publish` free for the routes file. A file that mounts two patterns imports `ruststream_zeromq::prelude::*` instead, where the policies keep their prefixed names (`ZmqQueuePublish`, `ZmqFanoutPublish`, `ZmqRpcPublish`), because three of them cannot share one bare name.

## Test it

The `testing` feature ships `ZmqTestBroker`: an in-process stand-in with the same routing and the same lifecycle ladder, no sockets. Build the app around it and drive it with the framework's `TestApp` harness - the handlers, the mount verb and the publish policy are the production ones, and only the broker changes. The harness encodes what it injects and decodes what it asserts on, so a test build adds `Outgoing` and `Serialize` to the input type and `Deserialize` plus `PartialEq` to the reply:

```rust
use ruststream::testing::TestApp;
use ruststream_zeromq::ZmqQueuePublish;
use ruststream_zeromq::testing::ZmqTestBroker;

let app = RustStream::new(AppInfo::new("worker", "0.1.0"))
    .with_broker(ZmqTestBroker::new(), |b| {
        b.include(handle).out(Reply, ZmqQueuePublish);
    });
let tb = TestApp::start(app).await?;

// A foreign peer's push; the injection returns once the handler has settled.
tb.broker::<ZmqTestBroker>()
    .publish("jobs", &Job { id: 1 })
    .await?;

tb.broker::<ZmqTestBroker>()
    .published::<Done>("results")
    .assert_called_once()
    .with(&Done { id: 1 });
```

Socket-level behaviour needs no stand-in and no server: the conformance routing suite, the lifecycle ladder, the batch and request-reply capabilities, and a wire-layout check driven by a raw foreign-style peer all run on loopback sockets, so `just test` covers the whole crate with nothing to start first.

## Layout

```
ruststream-zeromq/
├── crates/
│   └── ruststream-zeromq/      the published crate
│       └── examples/           runnable zmq_* examples
├── docs/                       the documentation site
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # the full suite, loopback sockets included
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
