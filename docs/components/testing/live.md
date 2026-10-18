---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test over loopback sockets in every cargo test, for back-pressure and the real frames."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test over loopback sockets in every `cargo test`, for back-pressure and the real frames.

It connects each broker through its ordinary connect, so every subscription binds or dials a real socket.
ZeroMQ needs no server, so a live test needs no stand and no switch, and runs wherever `cargo test` runs.
The body is the one an in-process test runs; only the start call differs.

A live test binds `tcp://127.0.0.1:0`: the system picks a free port, so tests that run in parallel never collide.
An `ipc://` path unique to the test serves the same purpose.
A test's input leaves through the broker's own publisher, so on a bound queue it carries the subscription's own name.
The fan-out's publisher reads its own subscription's filter before its first send, so a live fan-out test publishes once, as an in-process one does.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what reached it, ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process transport leaves out: the [read-ahead](../subscriptions/read-ahead.md) bound and the back-pressure it puts on a sender, a port another process holds, and the frames a peer in another language sends.

[Testing a ZeroMQ service](../../guides/testing.md) runs the same test in process and over sockets.

## Example

The two tests share one body: one in process, one over loopback sockets.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_testing_live.rs#both"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_testing_live.rs#both"
```

</TabItem>
</Tabs>
