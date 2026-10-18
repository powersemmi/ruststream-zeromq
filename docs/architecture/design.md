---
title: Design
sidebar_position: 2
description: "Where ruststream-zeromq sits: between the RustStream runtime and the zeromq client, with a socket per subscription and per publisher, and no server."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the zeromq client and the peers, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": ["Runtime: dispatch, codecs, retries,", "batches assembled on the client"],
  "crate": "ruststream-zeromq",
  "subscriptions": ["Subscription: a task", "reads the socket into", "a bounded channel"],
  "publishers": ["Publisher: a socket", "attached on the", "first publish"],
  "brokers": ["ZmqQueue, ZmqFanout,", "ZmqRpc"],
  "lifecycle": ["Shared state: the", "endpoint, the bound", "listener, closed"],
  "wire": ["Frame layout", "name, headers, payload"],
  "client": "zeromq client",
  "receiving": ["PULL, SUB, ROUTER"],
  "sending": ["PUSH, PUB, XPUB,", "ROUTER, DEALER"],
  "peers": ["Peers: other processes, any language,", "no server between them"],
  "testing": ["Under the testing feature, an in-process transport", "can take the place of the sockets"],
  "mounted": "mounted",
  "polls": "polls"
};

# Design

Where `ruststream-zeromq` sits: between the RustStream runtime and the `zeromq` client, with a
socket per subscription and per publisher, and no server.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over [`zeromq`](https://docs.rs/zeromq/0.6.0/zeromq/),
a ZeroMQ client written in Rust. The runtime above it is the same for every broker. Everything
ZeroMQ-specific is in this crate: [three brokers](../components/broker/index.md), one per socket
pattern, [the frame layout](zeromq/frames.md), [what an ack means](../components/settlement/index.md)
and [where a retry copy goes](../components/settlement/out-retry.md).

Each broker carries the side of its endpoint as a type, `Bind` or `Connect`. The side decides
whether a subscription can address its own retry copies, and the type makes that a compile-time
fact. [`ZmqRpc`](../components/broker/zmq-rpc.md) has no side parameter: its copies always go
where the mount names.

A [subscription](../components/subscriptions/index.md) is a name, the broker carries
`read_ahead`, and the endpoint is the rest of the configuration. A
[publish](../components/publishing/index.md) carries its frames alone, because a ZMTP send takes
frames and nothing else.

Each subscription opens a socket of its own and a task that owns it. The task reads one message
at a time into a channel the handler takes from, as [Backpressure](backpressure.md) draws. Each
publisher attaches a socket of its own on its first publish. A publisher on an endpoint this
service binds for one of its subscriptions dials that subscription's listener. On the fan-out it
is an XPUB that waits for the subscription's filter once, at the attach, so its first message is
not dropped.

Every socket attaches, and every task runs, on the runtime the broker connected on, whichever
runtime asked. A handler on a thread of its own can publish, and the socket it attaches outlives
that thread's runtime.

A queue send survives a dropped publish. The client takes the peer out of its rotation while a
send to it waits, so a send dropped at that moment would lose the peer for good. The publisher
keeps the send in flight beside its socket, and the next publish completes it before its own. The
crate forbids `unsafe` code, so the `selfie` and `aliasable` crates hold the send that borrows its
socket. The socket is boxed once per publisher, and a publish adds no allocation to what the
client spends.

## In process

The `testing` feature adds a second transport, which [`TestApp`](../components/testing/index.md)
connects instead of the sockets. Each broker keeps its connected form, its bookkeeping and its
publish policies; only the socket underneath changes. A publish is framed the way a socket
publisher frames it, and a subscription reads the frames the way its socket task reads them.

Who receives a message is the pattern's rule. A bound queue subscription takes everything pushed
to it, and queue subscriptions that dial take turns. A fan-out message reaches every subscription
whose name is a prefix of it. A request reaches a responder behind a peer identity, and the
answer goes back to the request that asked.

The refusals the sockets make hold in process too, in the same words. Settlement answers
`Unsupported` there as well.

The in-process transport tests the service's logic, not the transport. How far a subscription
reads ahead, back-pressure and the real handshake run over loopback sockets, in the
[live](../components/testing/live.md) mode of the same test. A production build carries none of
it: the crate checks the size of a delivery and of every handle at compile time.
