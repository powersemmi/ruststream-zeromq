---
sidebar_position: 2
title: Batches
description: "A slice handler on a queue or fan-out gets a batch assembled on this side, closed at the size the mount names or 20 ms after its first delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler on a queue or fan-out gets a batch assembled on this side, closed at the size the mount names or 20 ms after its first delivery.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
A receive on a socket yields one multipart message, so a peer still sends one message at a time, in the same frames.
The subscription collects the deliveries itself.
A batch closes once it holds `n` deliveries, or 20 ms after its first one, whichever comes first.
The 20 ms is a constant of the crate.
So a busy socket fills batches to `n`, and a quiet one hands over batches of one, 20 ms after each message arrived.

One outcome settles every delivery of the batch, and a `Vec<HandlerOutcome>` answers them one by one.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` settles the batch and `Err(outcomes)` settles it delivery by delivery.

A [`ZmqRpc`](../broker/zmq-rpc.md) responder takes no batches, and `.batch(..)` on one does not compile.
A responder answers each request at the peer that sent it, while a batch carries one publish context for all of its replies.
The compile error names `ZmqRpcSubscriber`, which implements no [`BatchSubscriber`](api:ruststream::BatchSubscriber).

## Example

Three jobs pushed at once arrive in one call.
A job pushed alone arrives 20 ms after it was pushed.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_batches.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_batches.rs#handler"
```

</TabItem>
</Tabs>
