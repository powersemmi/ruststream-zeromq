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

The crate's reference and its guide are one page: the
[`ruststream-zeromq` overview on docs.rs](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html) covers
[the three patterns](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-three-patterns), [endpoints](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#endpoints),
[subscribing](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#subscribing) with its batches and retries,
[publishing](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#publishing), [the wire contract](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-wire-contract) a non-Rust peer
composes messages against, [the generated document](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-generated-document),
[testing](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#testing) the production app, in process or over loopback sockets, and [operations](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#operations).

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-horizontal: **[ZeroMQ transport](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html)** - the three patterns, endpoints, the wire contract, request/reply, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-zeromq)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the ZeroMQ transport only. Framework concepts that apply to every broker
(writing subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).
