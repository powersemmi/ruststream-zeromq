---
sidebar_position: 1
title: bind
description: "ZmqEndpoint::bind makes this process listen: one subscription reads the bound socket, and port zero lets the system pick."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# bind

[`ZmqEndpoint::bind`](api:ruststream_zeromq::ZmqEndpoint::bind) makes this process listen: one subscription reads the bound socket, and port zero lets the system pick.

A bound endpoint is one listening socket, and it belongs to the subscription that bound it.
A second subscription on the same broker fails startup with an error that names both subscriptions.
So two kinds of work take two brokers, each on an endpoint of its own.
When the subscription closes, the endpoint is free for the next one.

The subscription binds its socket as it opens, and a port another process holds fails startup, as [Startup](../startup.md) describes.
With port zero, as in `tcp://127.0.0.1:0`, the system picks the port when the subscription binds.
A publisher of the same service dials the port the system picked, so a service that holds both ends runs with no fixed port.

A broker that binds an endpoint and has no subscription on it listens through its [publisher](../../publishing/index.md), and the peers dial it.
When a subscription of the same broker binds the endpoint later, the publisher gives the endpoint up first, on a fixed port and on port zero alike, and its next publish dials that subscription.
So a handler that publishes into a queue whose subscription opens after its own reaches that subscription once it is open.
On a bound endpoint that a subscription of this service reads, a [`ZmqQueue`](../zmq-queue.md) publisher sends only under the subscription's own name.

[Pipelines with PUSH/PULL](../../../guides/pipelines.md) decides which side of a pipeline binds.

## Example

A worker listens for jobs on one endpoint and for reports on another.
Each broker is labeled, so a test addresses it by name.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_bind.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_bind.rs#app"
```

</TabItem>
</Tabs>
