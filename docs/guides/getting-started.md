---
sidebar_position: 1
description: "Install ruststream-zeromq, run a first worker on a PULL socket, feed it from a Python peer and test it without opening a socket."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-zeromq`, runs a first worker and feeds it from a peer in another process.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-zeromq = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-zeromq` is the ZeroMQ broker.
Its `testing` feature adds the [in-process mode](../components/testing/index.md) that tests run on.

The broker crate has these features:

| Feature | What it gives |
|---|---|
| `asyncapi` | the [`x-ruststream-zeromq` extension](../components/asyncapi/index.md) in the generated AsyncAPI document |
| `testing` | the in-process mode behind `TestApp` |

ZeroMQ has no server, so there is nothing to install or start.
The crate speaks ZMTP through [zeromq](https://docs.rs/zeromq), a client written in Rust: the build needs no libzmq and no C compiler.
Two processes talk to each other directly, over `tcp://` or over `ipc://`, a Unix socket on one host.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_job` reads JSON jobs on `jobs`.
It acknowledges a job with items and drops one without.
[`ZmqQueue`](../components/broker/zmq-queue.md) is the PUSH/PULL pattern: each job reaches one worker.
[`ZmqEndpoint::bind`](../components/broker/endpoint/bind.md) says that this process listens on the address.
`ZmqQueue::new` only records the endpoint; the socket opens when the service starts.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

## The peer

Any process that speaks ZMTP can push jobs to the worker.
A message is three frames: the name, the headers and the payload ([Wire format](../components/wire/index.md)).
This Python peer uses [pyzmq](https://pyzmq.readthedocs.io/):

```python title="push_job.py"
import json
import zmq

push = zmq.Context().socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send_multipart([b"jobs", b"", json.dumps({"id": 1, "items": 3}).encode()])
push.close(linger=1000)
```

Run it from another terminal:

```bash
python3 push_job.py
```

The service logs `job taken` with the job's id and item count.
A warning follows it: `ack / nack failed`, acknowledgement is not supported by this broker.
ZeroMQ has no acknowledgement on the wire, so the job counts as done once the worker reads it ([Acknowledgement](../components/settlement/index.md)).
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.
The peer can be a RustStream service too: [Pipelines with PUSH/PULL](pipelines.md) builds one that pushes its results to another.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, pushes a job and checks how `take_job` settled it.
The broker runs in process there, so the tests open no socket:

```bash
cargo test
```

[Testing a ZeroMQ service](testing.md) walks through the harness and the same test over real sockets.

## Coming from pyzmq or libzmq

A service on a ZeroMQ binding opens its sockets and writes its own receive loop.
Here is where each part goes in RustStream:

| pyzmq or libzmq | RustStream |
|---|---|
| `ctx.socket(zmq.PULL)` and `bind` | [`ZmqQueue::new(ZmqEndpoint::bind(..))`](../components/broker/zmq-queue.md) and a `#[subscriber]` handler |
| `connect` | [`ZmqEndpoint::connect`](../components/broker/endpoint/connect.md); the side is part of the broker's type |
| a `recv_multipart` loop | the handler, called once per message |
| decoding the payload frame | the handler's parameter type, decoded by the codec |
| `send_multipart` | a reply or an `Out` parameter, sent through a [publish policy](../components/publishing/index.md) |
| `setsockopt(zmq.SUBSCRIBE, prefix)` | the subscription name on [`ZmqFanout`](../components/broker/zmq-fanout.md) ([Broadcast with PUB/SUB](broadcast.md)) |
| REQ and REP | [`ZmqRpc`](../components/broker/zmq-rpc.md) over DEALER and ROUTER ([Request and reply](request-reply.md)) |
| `tcp://*:5555` | `tcp://0.0.0.0:5555` |
| `inproc://` | [`TestApp`](../components/testing/index.md) for tests; `ipc://` between processes on one host |
| `SNDHWM` and `RCVHWM` | [`read_ahead`](../components/subscriptions/read-ahead.md) ([Running in production](production.md)) |
| a `zmq.Poller` over several sockets | one subscription per socket, each on a task of its own |
