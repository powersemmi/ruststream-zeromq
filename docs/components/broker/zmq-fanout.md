---
sidebar_position: 2
title: ZmqFanout
description: "ZmqFanout is PUB/SUB: every subscription whose name prefixes a message gets a copy, and one that attaches late misses what came before."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ZmqFanout

[`ZmqFanout`](api:ruststream_zeromq::fanout::ZmqFanout) is PUB/SUB: every subscription whose name prefixes a message gets a copy, and one that attaches late misses what came before.

The subscription's name is its filter, and the filter is a prefix, the protocol's own rule.
`#[subscriber("orders")]` receives `orders.created` and `orders.shipped`, and never `payments.settled`; the [prefixes](../subscriptions/prefixes.md) page covers the rule.
A message that matches no subscription is dropped, and the publisher gets no error.

Nothing is stored.
A subscription that attaches after a publisher has started misses what that publisher sent before it arrived.
This is the slow joiner of ZeroMQ.
A publisher that reaches a subscription of its own service reads that subscription's filter before its first send, so inside one service nothing is lost to it.

The policy is [`ZmqFanoutPublish`](api:ruststream_zeromq::fanout::ZmqFanoutPublish), offered as `Publish` by [`fanout::prelude`](api:ruststream_zeromq::fanout::prelude).
A PUB socket is held back by its slowest matching subscriber, as [`read_ahead`](../subscriptions/read-ahead.md) describes.

A retry copy on a fan-out the subscription binds goes back to that subscription's listener and reaches the audience the original had.
A subscription that dials reads from a publisher that takes nothing back, so its mount site names where the copies go, as on [`ZmqQueue`](./zmq-queue.md).

## Example

An auditor binds a fan-out, and the order services dial it.
Its subscription on `orders` receives every order event, whatever comes after the prefix.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_fanout.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_fanout.rs#app"
```

</TabItem>
</Tabs>
