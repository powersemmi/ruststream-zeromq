---
title: Subscriptions
description: "A subscription is a name, and the name is frame 0 of every message; #[subscriber(\"jobs\")] is the whole declaration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A subscription is a name, and the name is frame 0 of every message; `#[subscriber("jobs")]` is the whole declaration.

Every message travels as three [frames](../wire/index.md), and the first one holds the name the publisher sent it under.
That name is the whole declaration of a subscription, and the mount site carries every other setting.
The manual form passes the name to [`subscriber(name, handler)`](api:ruststream::runtime::subscriber).

Only [`ZmqFanout`](../broker/zmq-fanout.md) reads the name: it hands a subscription every message whose name starts with the subscription's, as [Prefixes](./prefixes.md) describes.
A [`ZmqQueue`](../broker/zmq-queue.md) or [`ZmqRpc`](../broker/zmq-rpc.md) subscription takes every message its socket receives, whatever frame 0 says.
So a second kind of work takes an endpoint of its own.

How many subscriptions a broker takes follows from the side.
An endpoint this service [binds](../broker/endpoint/bind.md) is one socket, read by the subscription that bound it, and a second subscription there is refused when it opens.
Subscriptions that [dial](../broker/endpoint/connect.md) are sockets of their own, so several of them dial one peer.

Nothing is stored on the way.
A subscription that attaches late has no backlog to read, a restart replays nothing, and `.start_at(..)` does not compile on these brokers.

## Example

A worker binds `tcp://0.0.0.0:5555` and names its subscription `jobs`.
A peer that pushes a message under another name to that endpoint reaches the same handler.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
