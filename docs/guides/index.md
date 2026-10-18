---
slug: /
sidebar_position: 0
description: "Guides to building ZeroMQ services with ruststream-zeromq, the ZeroMQ broker for RustStream."
---

# Guides

`ruststream-zeromq` is the ZeroMQ broker for [RustStream](https://powersemmi.github.io/ruststream/latest/),
built on the pure-Rust [zeromq](https://docs.rs/zeromq) client. ZeroMQ has no server in the middle:
services talk to each other directly over `tcp://` and `ipc://` sockets.

The crate offers one broker per socket pattern. `ZmqQueue` runs PUSH/PULL, where competing
consumers share the work. `ZmqFanout` runs PUB/SUB, where every subscriber receives a copy.
`ZmqRpc` runs DEALER/ROUTER, which carries a request and its reply.
