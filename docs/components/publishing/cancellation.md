---
sidebar_position: 2
title: Cancellation
description: "A queue publish is cancel-safe; a fan-out publish, a reply and a request are not, so give up through a timeout, not a select! arm."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Cancellation

A queue publish is cancel-safe; a fan-out publish, a reply and a request are not, so give up through a timeout, not a `select!` arm.

A publish waits while the peer applies [back-pressure](../../architecture/backpressure.md), so a caller may drop it before it completes.
On [`ZmqQueue`](../broker/zmq-queue.md) that is safe.
A publish dropped before the socket took its message sends nothing.
One dropped in the middle of the send leaves its message with the publisher, and the next publish through it completes that send before its own.

A fan-out publish and a reply are not cancel-safe.
Dropping one can leave the message half handed to the socket.
On a reply, the requester then waits out its timeout.
Dropping a [request](../request-reply/index.md) closes the DEALER socket it went out on: an answer already on its way is lost, and the request itself may have reached the responder.

So a handler gives up on a request through its timeout, which ends the wait cleanly and returns [`ZmqError::RequestTimeout`](../broker/errors.md).
A fan-out publish or a reply that must not hold up other work runs in a task of its own rather than inside a `select!` arm.

[Request and reply](../../guides/request-reply.md) gives up on a request through its timeout and asks again later.

## Example

The handler asks the pricing service for a quote and waits at most two seconds.
With no quote in time it settles the order as a retry in thirty seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_publishing_cancellation.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_publishing_cancellation.rs#handler"
```

</TabItem>
</Tabs>
