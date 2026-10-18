---
sidebar_position: 1
title: Prefixes
description: "On ZmqFanout a subscription named orders receives orders.created too: the name is a prefix filter, the protocol's own rule."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prefixes

On [`ZmqFanout`](api:ruststream_zeromq::fanout::ZmqFanout) a subscription named `orders` receives `orders.created` too: the name is a prefix filter, the protocol's own rule.

A SUB socket subscribes with the subscription's name, and a message reaches it when frame 0 starts with that name.
The comparison runs byte by byte and knows no separator.
So `orders` takes `orders.created` and `orders.shipped`, and `orders-archive` as well.
A name that ends with the separator, such as `orders.`, takes the dotted family and leaves the lookalike out.
A message that matches no subscription is dropped without an error.

The rule is the same on both sides.
A subscription that binds the fan-out and one that dials a publisher filter alike, and each subscription that dials is a socket with its own filter.
A message this service publishes to its own bound fan-out is matched the same way, so a name under the subscription's prefix comes back to it.

In the [generated document](../asyncapi/channels.md), a fan-out publish channel reports `nameFrame`: the prefix a SUB peer subscribes with to receive it.

## Example

The auditor reads every order event and leaves the archive records alone.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_subscriptions_prefixes.rs#prefixes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_subscriptions_prefixes.rs#prefixes"
```

</TabItem>
</Tabs>
