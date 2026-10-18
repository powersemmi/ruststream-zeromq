---
title: Testing
description: "TestApp::start connects each ZeroMQ broker in process: the pattern's routing, the socket's refusals, and no settlement."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects each ZeroMQ broker in process: the pattern's routing, the socket's refusals, and no settlement.

A test hands the harness the app `main` runs, unchanged, and addresses a broker by its production type.
`tb.broker::<ZmqQueue>()` names a queue this service binds, `tb.broker::<ZmqQueue<Connect>>()` one it dials, and `tb.broker_named(..)` a broker mounted under a label.
The `testing` feature in `[dev-dependencies]` adds the in-process mode and turns on the core's harness with it.

The in-process transport carries the frames a socket carries.
A publish is framed as the socket publisher frames it, and a subscription reads the frames as its socket reads them.
So a header value the text frame cannot hold is refused, and a request arrives stamped with the peer that sent it.
A test's input arrives the way a peer in another process sends it.

Which subscriptions a message reaches is the pattern's own rule:

- Queue: a subscription that binds takes whatever a peer pushes to its endpoint, and subscriptions that dial take a peer's messages in turn.
- Fan-out: every subscription whose name is a [prefix](../subscriptions/prefixes.md) of the message's name, on either side.
- Request and reply: a request reaches the responder, and its answer reaches the peer that asked and no subscription.

A publish of this service reaches its own subscription only through the endpoint that subscription binds.
On an endpoint the service dials, or binds with no subscription, the message leaves for a peer in another process: the test reads it with `published`, and no subscription receives it.
The socket's refusals hold too: a publish a bound queue would hand back to its own subscription, any publish on an endpoint a subscription dials, a second subscription on a bound endpoint, an answer to a peer that is gone.
[Settlement](../settlement/index.md) is the socket's as well, so a handler that settles by a plain retry fails its test instead of losing its message in production.
What only sockets show, such as the [read-ahead](../subscriptions/read-ahead.md) bound or a port another process holds, is for a [live test](./live.md).

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-zeromq = { version = "0.7", features = ["testing"] }
```

The worker binds its queue and sends each result to a sink it dials.
The test pushes a job and reads the result the worker published.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_testing.rs#test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_testing.rs#test"
```

</TabItem>
</Tabs>
