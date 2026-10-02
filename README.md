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

`ruststream-zeromq` connects a RustStream service to a ZeroMQ topology over the pure-Rust
[`zeromq`](https://crates.io/crates/zeromq) implementation, on TCP and IPC. There is no server in
the middle, so a Rust service joins sockets a Python worker or a C++ daemon already speaks.
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

Delivery is at most once, with no durability: acknowledgement reports `AckError::Unsupported`. A
subscriber that connects late misses what was sent before it arrived, and there is no encryption
layer.

## The wire contract

```text
frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   encoded by the framework's codec
```

A Python peer sends `socket.send_multipart([b"orders", b"content-type: application/json", payload])`.
A two-frame message reads as one without headers.

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

#[subscriber("jobs", publish("results"))]
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

- This crate: <https://docs.rs/ruststream-zeromq>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
