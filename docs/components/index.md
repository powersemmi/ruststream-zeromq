---
sidebar_position: 0
description: "The parts of ruststream-zeromq a ZeroMQ service uses, one page per component."
---

# Components

The parts of `ruststream-zeromq` a ZeroMQ service uses, one page per component.

The brokers come first: [`ZmqQueue`](api:ruststream_zeromq::queue::ZmqQueue),
[`ZmqFanout`](api:ruststream_zeromq::fanout::ZmqFanout) and
[`ZmqRpc`](api:ruststream_zeromq::rpc::ZmqRpc), the
[`ZmqEndpoint`](api:ruststream_zeromq::ZmqEndpoint) each binds or connects, their startup,
shutdown, preludes and errors. Subscriptions follow, with their prefixes, batches and read-ahead.
Acknowledgement covers what a delivery settles on ZeroMQ, retries and dead letters. Publishing
covers the publish policies, replies and cancellation, and Request-reply the requester and the
responder. Wire format shows the frames a peer in another language sends, AsyncAPI what the
generated document reports, and Testing the in-process transport a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.
