# ruststream-zeromq

**`ruststream-zeromq`** connects a [RustStream](https://powersemmi.github.io/ruststream/) service to
a ZeroMQ topology, over the pure-Rust [`zeromq`](https://docs.rs/zeromq) implementation on the TCP
and IPC transports.

There is no server in the middle: two processes talk to each other directly. The frame layout is
documented, so the peer on the other side composes messages by hand. A Rust service joins a
topology an existing Python worker or C++ daemon already speaks.

Three socket patterns cover three messaging shapes: `ZmqQueue` (PUSH/PULL, competing consumers),
`ZmqFanout` (PUB/SUB, broadcast with prefix filtering), and `ZmqRpc` (DEALER/ROUTER, request and
reply).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-horizontal: **[ZeroMQ guide](zeromq.md)** - the three patterns, endpoints, the wire contract, request/reply, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-zeromq)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the ZeroMQ transport only. Framework concepts that apply to every broker
(writing subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).
