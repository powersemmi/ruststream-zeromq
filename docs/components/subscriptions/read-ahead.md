---
sidebar_position: 3
title: read_ahead
description: "read_ahead bounds how many deliveries a subscription reads ahead of its handler, 1000 by default; past it the sender waits."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# read_ahead

[`read_ahead`](api:ruststream_zeromq::queue::ZmqQueue::read_ahead) bounds how many deliveries a subscription reads ahead of its handler, 1000 by default; past it the sender waits.

A subscription reads its socket on a task of its own and hands the deliveries to its handler through a buffer.
`read_ahead` sets the size of that buffer, counted in deliveries.
When the buffer is full, the subscription stops reading.
The socket buffers fill in turn, and the peer's send waits until the handler catches up.
So a slow handler slows its sender down, and the memory of this process stays bounded.
On PUB/SUB the waiting sender is the PUB socket, held back by its slowest matching subscriber.

The setting belongs to the broker: [`ZmqQueue::read_ahead`](api:ruststream_zeromq::queue::ZmqQueue::read_ahead), [`ZmqFanout::read_ahead`](api:ruststream_zeromq::fanout::ZmqFanout::read_ahead) and [`ZmqRpc::read_ahead`](api:ruststream_zeromq::rpc::ZmqRpc::read_ahead) take a `NonZeroUsize`.
Every subscription mounted on the broker reads with that bound, and a clone of the broker keeps its own value.

The in-process transport hands deliveries over without a bound, so a test sees the bound only over sockets, in a [live test](../testing/live.md).

## Example

Each frame takes the renderer seconds, so it reads four frames ahead and leaves the rest with the sender.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_subscriptions_read_ahead.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_subscriptions_read_ahead.rs#app"
```

</TabItem>
</Tabs>
