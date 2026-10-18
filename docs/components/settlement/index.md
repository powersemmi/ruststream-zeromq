---
title: Acknowledgement
description: "ZeroMQ settles nothing: ack and nack report Unsupported, a plain retry delivers nothing again, and every retry is a copy this service publishes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

ZeroMQ settles nothing: `ack` and `nack` report `Unsupported`, a plain retry delivers nothing again, and every retry is a copy this service publishes.

ZMTP has no frame that settles a delivery.
A message reaches the subscription once, and the socket keeps no copy of it.
So [`ack`](api:ruststream::IncomingMessage::ack) and `nack` on a delivery report [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs a warning naming the subscription.
An [`ack`](api:ruststream::runtime::HandlerOutcome::ack) and a [`drop`](api:ruststream::runtime::HandlerOutcome::drop) both end the delivery, and nothing comes back after either.

A [`retry`](api:ruststream::runtime::HandlerOutcome::retry) asks the transport to deliver the message again, and the transport has nothing to deliver: the handler is not called again.
A [`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) is served by the runtime instead.
It publishes a copy of the delivery once the delay is over, and the copy arrives as a new message, as [Retries](./retries.md) describes.
Under a [cap](./retries.md) a plain `retry` travels as such a copy too.
Where the copies go follows the side of the endpoint, as [`out_retry`](./out-retry.md) describes.

A delivery reports no [`redelivery_count`](api:ruststream::IncomingMessage::redelivery_count): the framework counts its copies in a header of its own.
Delivery is at most once.
A process that stops loses what its subscriptions had read and not yet handled, and every copy still waiting out its delay.

## Example

A payment of nothing is dropped.
A payment the gateway fails comes back as a copy 10 seconds later, through the queue the service binds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_settlement_at_most_once.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_settlement_at_most_once.rs#settle"
```

</TabItem>
</Tabs>
