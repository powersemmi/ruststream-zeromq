<h1 align="center">ruststream-zeromq</h1>

<p align="center">
  <i>The ZeroMQ transport for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: typed handlers and codecs over sockets shared with Python, C++, and other non-Rust peers.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-zeromq/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-zeromq/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-zeromq` implements the RustStream broker contract over the pure-Rust [`zeromq`](https://crates.io/crates/zeromq) implementation (TCP and IPC transports). Unlike the other RustStream broker crates, there is no server in the middle: a Rust service can join a ZeroMQ topology an existing Python worker or C++ daemon already speaks, without dropping out of the framework.

## Patterns

Three socket patterns cover three messaging shapes:

- **`ZmqQueue`** - PUSH/PULL: competing consumers, round-robin.
- **`ZmqFanout`** - PUB/SUB: broadcast, prefix filtering by name.
- **`ZmqRpc`** - DEALER/ROUTER: request and reply (`RequestReply` on the publisher; replies route back through the responder's `reply-to` header).

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

## Scope and limits

- Delivery is **at most once** and there is no durability; acknowledgement is reported as `AckError::Unsupported`, never emulated.
- A subscriber that connects after a publisher has started **misses what was sent before it arrived** (the slow joiner), and a fan-out message with no matching subscriber is dropped silently.
- The implementation exposes **no high-water-mark configuration**: a slow reader exerts raw TCP back-pressure on senders.
- There is **no encryption layer**: use it on trusted networks, or inside an existing tunnel.
- No consumer groups, no dead-lettering, no retry policies, no transactions.

## Write a service

```rust
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerResult {
    println!("working on job {}", job.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
            b.include(handle);
        })
}
```

## Status

Implemented, on the `ruststream` 0.6 line. The whole suite (conformance routing, the lifecycle ladder, the request/reply capability, and the wire-layout check driven by a raw foreign-style peer) runs on loopback sockets in CI, no external broker required. Published on crates.io as `ruststream-zeromq = "0.6"`. Design and scope are tracked in [powersemmi/ruststream#192](https://github.com/powersemmi/ruststream/issues/192).

## Test it

The `testing` feature runs handlers against an in-process stand-in - no sockets, same routing. The socket-level behaviour is covered by the loopback suite: `just test` runs everything, no broker to start.

## Layout

```
ruststream-zeromq/
├── crates/
│   └── ruststream-zeromq/      the published crate
│       └── examples/           runnable zmq_* examples
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # the full suite, loopback sockets included
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
