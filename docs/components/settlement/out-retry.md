---
sidebar_position: 2
title: out_retry
description: "Where copies go follows the side: a bound subscription takes its own, and one that dials and every responder name a destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

Where copies go follows the side: a bound subscription takes its own, and one that dials and every responder name a destination.

[`out_retry(policy)`](api:ruststream::runtime::Mounting::out_retry) sets the publisher the [retry](./retries.md) copies leave through.
A [`ZmqQueue`](../broker/zmq-queue.md) or [`ZmqFanout`](../broker/zmq-fanout.md) subscription that [binds](../broker/endpoint/bind.md) its endpoint is an address of its own.
Its copies go back to that endpoint, so the mount names no destination, and `.out_retry(Publish)` states the default.

A subscription that [dials](../broker/endpoint/connect.md) reads from a peer that only sends, so it has no address to send copies to.
Its mount site names one with `.out_retry(policy).to("name")`.
The policy is often a queue on another broker, which [`bindable()`](api:ruststream::Broker::bindable) and `.bind(Publish)` turn into a publisher of this mount.
A mount on a dialing subscription that names no destination is refused before the subscription opens, with an error naming the subscription.
A [`Router`](api:ruststream::runtime::Router) chain that names none does not compile.
Both hold for a mount that never asks for a retry, because the runtime pairs the publisher either way.

Every [`ZmqRpc`](../broker/zmq-rpc.md) responder names a destination too.
A request has no address a copy could return to: answers travel to the identity of the peer that asked, and the [reply publisher](../request-reply/responders.md) refuses a plain name.

[Retries and dead letters](../../guides/retries.md) mounts both forms, a worker that binds and a worker that dials.

## Example

The worker dials a ventilator and sends its copies to a queue it binds itself, where the same handler takes them.
The copies carry the count, so the cap of three holds across both subscriptions.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_settlement_out_retry.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_settlement_out_retry.rs#app"
```

</TabItem>
</Tabs>
