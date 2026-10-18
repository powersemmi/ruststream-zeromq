---
title: Broker
description: "Three brokers, one per socket pattern: new records the endpoint with no I/O, and the runtime connects each at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Broker

Three brokers, one per socket pattern: `new` records the endpoint with no I/O, and the runtime connects each at startup.

ZeroMQ has no server in the middle.
Two processes talk directly, over a socket pair the pattern names.
The pattern decides who receives a message, and the [endpoint](./endpoint/index.md) decides which side listens.

| Broker | Sockets | Who receives a message | Publish policy |
| --- | --- | --- | --- |
| [`ZmqQueue`](./zmq-queue.md) | PUSH/PULL | one of the competing consumers, in turn | [`ZmqQueuePublish`](../publishing/index.md) |
| [`ZmqFanout`](./zmq-fanout.md) | PUB/SUB | every subscription whose name is a prefix of the message's | [`ZmqFanoutPublish`](../publishing/index.md) |
| [`ZmqRpc`](./zmq-rpc.md) | DEALER/ROUTER | the responder, and its answer the peer that asked | [`ZmqRpcPublish`](../publishing/index.md) |

Each broker's `new` takes a [`ZmqEndpoint`](./endpoint/index.md) and opens nothing, so the whole service fits the synchronous `#[ruststream::app]` builder.
At startup the runtime connects it and receives its connected form, such as [`ConnectedZmqQueue`](api:ruststream_zeromq::queue::ConnectedZmqQueue).
The connect checks the endpoint's scheme and opens no socket: each subscription and each publisher attaches its own socket when it first opens, as [Startup](./startup.md) describes.
At shutdown the runtime consumes the connected form, and a publisher handed out earlier reports an error from then on, as [Shutdown](./shutdown.md) describes.

Each pattern's publish policy is also the default of its connected form.
So a reply handler mounted with no publish position answers through its own pattern.
Each pattern's [prelude](./prelude.md) offers its policy as `Publish`, so moving a service to another pattern changes its import line and leaves the mount site as it is.

## Example

A worker stage of a pipeline binds `tcp://0.0.0.0:5555`, and any peer that pushes the three [frames](../wire/index.md) feeds it.
`cargo run --example zmq_pipeline --features testing -- run` starts it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_pipeline.rs#handler"
```

```rust reference="examples/zmq_pipeline.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_pipeline.rs#handler"
```

```rust reference="examples/manual/zmq_pipeline.rs#app"
```

</TabItem>
</Tabs>
