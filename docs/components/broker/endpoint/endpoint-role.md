---
sidebar_position: 3
title: EndpointRole
description: "EndpointRole makes the side a type: ZmqQueue<Connect> dials, the parameter defaults to Bind, and a mount site writes it where it names the type."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# EndpointRole

[`EndpointRole`](api:ruststream_zeromq::EndpointRole) makes the side a type: `ZmqQueue<Connect>` dials, the parameter defaults to `Bind`, and a mount site writes it where it names the type.

`ZmqEndpoint::bind` returns a `ZmqEndpoint<Bind>`, and `ZmqEndpoint::connect` returns a `ZmqEndpoint<Connect>`.
[`ZmqQueue`](../zmq-queue.md) and [`ZmqFanout`](../zmq-fanout.md) carry the same [`Bind`](api:ruststream_zeromq::Bind) or [`Connect`](api:ruststream_zeromq::Connect) as their `Role` parameter, so `ZmqQueue::new(ZmqEndpoint::connect(..))` is a `ZmqQueue<Connect>`.
The parameter defaults to `Bind`, so `ZmqQueue` alone names a queue this process binds.
[`ZmqRpc`](../zmq-rpc.md) has no `Role` parameter.

A mount site writes the side only where it names the type: a [`Router`](api:ruststream::runtime::Router) for a queue it dials is a `Router<ZmqQueue<Connect>>`, and a test addresses that broker as `tb.broker::<ZmqQueue<Connect>>()`.
The side decides where a retry copy can go, and the compiler holds a routes file to it.
On `ZmqQueue<Connect>`, a router chain whose `out_retry` names no destination does not compile.

`EndpointRole` is the bound a helper writes to build a broker on either side.
The trait is sealed: `Bind` and `Connect` are the whole set.

## Example

A worker's routes on the queue it dials send their retry copies to a queue the worker binds.
One helper builds both queues with the same read-ahead, whichever side the endpoint takes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_role.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_role.rs#routes"
```

</TabItem>
</Tabs>
