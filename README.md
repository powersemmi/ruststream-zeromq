<h1 align="center">ruststream-zeromq</h1>

<p align="center">
  <i>The ZeroMQ transport for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: typed handlers and codecs over sockets shared with Python, C++, and other non-Rust peers.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-zeromq/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-zeromq/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-zeromq"><img src="https://img.shields.io/crates/v/ruststream-zeromq.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-zeromq"><img src="https://img.shields.io/crates/dr/ruststream-zeromq" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-zeromq"><img src="https://img.shields.io/docsrs/ruststream-zeromq" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-zeromq/latest/">Documentation</a></b>
</p>

---

`ruststream-zeromq` connects a RustStream service to a ZeroMQ topology over the pure-Rust
[`zeromq`](https://crates.io/crates/zeromq) implementation, on TCP and IPC. Peers talk directly,
so a Rust service joins sockets a Python worker or a C++ daemon already speaks.
Handlers, routing, codecs and middleware come from the framework; this crate is the transport.

## Features

- **Three socket patterns:** `ZmqQueue` (PUSH/PULL, competing consumers), `ZmqFanout` (PUB/SUB,
  prefix filtering) and `ZmqRpc` (DEALER/ROUTER, request and reply).
- **An explicit role:** an endpoint either binds or connects, which is a deployment decision.
- **A stable wire contract** that a foreign peer composes by hand.
- **Batches** assembled on the client for the one-way patterns.
- **Retry caps and dead letters** applied by the framework.
- **AsyncAPI** with the transport, role and socket pair, behind the `asyncapi` feature.
- **Tests without sockets:** the service's own app runs with this crate's brokers in process.

Delivery is at most once and nothing is stored: acknowledgement reports `AckError::Unsupported`,
and a subscriber that connects late misses what was sent before it arrived. Traffic travels in the
clear, so a service runs on a trusted network or inside a tunnel.

## The wire contract

```text
frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   encoded by the framework's codec
```

A Python peer sends `socket.send_multipart([b"orders", b"content-type: application/json", payload])`.
A two-frame message reads as one without headers. A single frame, what a plain ZeroMQ peer
sends, is the payload, whole, with an empty name.

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

```rust
use ruststream_zeromq::queue::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Job {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Done {
    id: u64,
}

#[subscriber("jobs", reply("results"))]
async fn handle(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    // The results travel on a queue of their own, which a sink binds.
    let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
    let to_results = results.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
            b.include(handle).out_reply(to_results);
        })
        .with_broker(results, |_b| {})
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`. Each
pattern has its own prelude (`queue`, `fanout`, `rpc`), so switching pattern changes the import.

## Test it

`TestApp` runs the service's own app with this crate's brokers in process, with no sockets;
`TestApp::start_live(app())` runs the same body over loopback sockets. Enable the `testing`
feature in `[dev-dependencies]`.

```rust
use ruststream::testing::TestApp;
use ruststream_zeromq::Connect;

let tb = TestApp::start(app()).await?;

// A foreign peer's push; the injection returns once the handler has settled.
tb.broker::<ZmqQueue>()
    .message(&Job { id: 1 })
    .to("jobs")
    .publish()
    .await?;

tb.broker::<ZmqQueue<Connect>>()
    .published::<Done>("results")
    .assert_called_once()
    .with(&Done { id: 1 });
```

## Documentation

- [Getting started](https://powersemmi.github.io/ruststream-zeromq/latest/guides/getting-started/) - installation and a first
  worker fed by a Python peer.
- [Guides](https://powersemmi.github.io/ruststream-zeromq/latest/) - pipelines, broadcast, request and reply, retries, peers in
  other languages, testing and production.
- [Components](https://powersemmi.github.io/ruststream-zeromq/latest/components/) - one page per part of the crate: the brokers
  and endpoints, subscriptions, acknowledgement, publishing, request-reply, the wire format and testing.
- [Architecture](https://powersemmi.github.io/ruststream-zeromq/latest/architecture/) - how the sockets work and where the crate
  sits between them and the runtime.
- [Benchmarks](https://powersemmi.github.io/ruststream-zeromq/latest/architecture/benchmarks/) - what the crate costs over the
  raw client.
- [API reference](https://docs.rs/ruststream-zeromq) - every item on docs.rs.
- The framework: <https://powersemmi.github.io/ruststream/latest/>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
