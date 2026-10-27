---
title: Publishing
description: "Each pattern's Publish sends the three frames and nothing else; a send waits up to five seconds for a peer."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publishing

Each pattern's `Publish` sends the three frames and nothing else; a send waits up to five seconds for a peer.

Each pattern ships one publish policy: [`ZmqQueuePublish`](api:ruststream_zeromq::queue::ZmqQueuePublish), [`ZmqFanoutPublish`](api:ruststream_zeromq::fanout::ZmqFanoutPublish) and [`ZmqRpcPublish`](api:ruststream_zeromq::rpc::ZmqRpcPublish).
The pattern's [prelude](../broker/prelude.md) offers it as `Publish`, and the crate prelude offers all three under their own names.
A mount site names it wherever a registration publishes: `.out_reply(Publish)` for a reply, `.out(Slot, Publish).build()` for a slot, `.out_retry(Publish)` for [retry copies](../settlement/out-retry.md).
The policy holds no connection.
At startup the runtime pairs it with the connected broker into a live publisher, such as [`ZmqQueuePublisher`](api:ruststream_zeromq::queue::ZmqQueuePublisher).
To publish through another broker of the service, the mount site binds the policy to it with `bindable()` and `.bind(Publish)`.
[Pipelines with PUSH/PULL](../../guides/pipelines.md) publishes a worker's results to a sink this way.

A publish sends the message in its [envelope](../wire/envelopes.md), by default the [three frames](../wire/index.md): its name, its headers and the payload the codec wrote.
The payload frame is the buffer the codec wrote, so a payload of any size is sent without a copy.
A ZMTP send takes the frames and nothing else, so every publisher declares `Options = ()`, and the publish builder carries the core's steps alone.
A handler body therefore imports `ruststream::prelude::*` alone and bounds its publisher with the capability it needs.

A publisher on the bind side attaches its socket when the service starts, so a peer that dials it connects before the first message.
A publisher that dials attaches on its first publish and waits for a peer.
When no peer has completed the handshake within five seconds, the publish fails with [`ZmqError::Send`](../broker/errors.md), "no connected peer".
The window is a constant of the crate.

Who receives the message is the pattern's rule.
On [`ZmqQueue`](../broker/zmq-queue.md) one peer takes it, in turn.
On [`ZmqFanout`](../broker/zmq-fanout.md) every subscriber whose filter is a prefix of the name takes a copy, and a message no subscriber matches is dropped without an error.
On [`ZmqRpc`](../broker/zmq-rpc.md) the publisher sends [replies](../request-reply/responders.md) only, and a request goes through [`request`](../request-reply/index.md).
A publish is one send of one message, and a queue publish is safe to cancel, as [Cancellation](./cancellation.md) describes.

## Example

A worker announces each shipped order on a fan-out it binds itself.
A peer that subscribes to `orders.shipped`, or to the prefix `orders`, receives every announcement.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_publishing_publish.rs#handler"
```

```rust reference="examples/zmq_publishing_publish.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_publishing_publish.rs#handler"
```

```rust reference="examples/manual/zmq_publishing_publish.rs#app"
```

</TabItem>
</Tabs>
