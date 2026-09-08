# ruststream-zeromq

**`ruststream-zeromq`** is the ZeroMQ transport for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, built on the pure-Rust
[`zeromq`](https://docs.rs/zeromq) implementation over the TCP and IPC transports.

Unlike every other broker crate there is no server in the middle, and that is the reason it exists:
a Rust service can join a ZeroMQ topology an existing Python worker or C++ daemon already speaks,
without leaving the framework. Handlers, routers, codecs, and middleware come from the framework;
this crate supplies the sockets and a documented frame layout the peer on the other side can
compose by hand.

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
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to ZeroMQ and link back to the framework docs where the two meet.
