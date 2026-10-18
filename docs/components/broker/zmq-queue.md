---
sidebar_position: 1
title: ZmqQueue
description: "ZmqQueue is PUSH/PULL: competing consumers, each message reaches one of them, in turn and without a key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ZmqQueue

[`ZmqQueue`](api:ruststream_zeromq::queue::ZmqQueue) is PUSH/PULL: competing consumers, each message reaches one of them, in turn and without a key.

A pipeline has a ventilator that pushes work, workers that pull it, and a sink that collects the results.
Workers that dial one ventilator compete for its stream: each message reaches one of them, and none reaches two.
PUSH hands the messages out in turn across the attached peers and reads no key, so the crate has no partitioning.
Order holds per socket pair and nowhere else.

A subscription that [binds](./endpoint/bind.md) its endpoint takes every message a peer pushes into it, whatever the name in frame 0 says.
So a second kind of work takes an endpoint of its own.

The policy is [`ZmqQueuePublish`](api:ruststream_zeromq::queue::ZmqQueuePublish), offered as `Publish` by [`queue::prelude`](api:ruststream_zeromq::queue::prelude).
A publish to an endpoint that a subscription of this service binds reaches that subscription and nothing else.
A message under the subscription's own name goes through: a retry copy, or a job the service feeds itself.
A message under any other name is refused with [`ZmqError::Send`](./errors.md), naming it and the subscription, because the subscription would receive it as its next job.
So results and replies leave through a queue on an endpoint of its own, as the [replies](../publishing/replies.md) page shows.

A retry copy is a message this service publishes.
On a bound queue the copy goes back into the queue, and whichever worker is free takes it.
A queue that [dials](./endpoint/connect.md) reads from a ventilator that takes nothing back, so its mount site names where the copies go; [`out_retry`](../settlement/out-retry.md) covers both forms.

## Example

Two workers dial one ventilator, and each job it pushes reaches one of them.
Their retry copies go to a queue this service binds, where the same handler takes them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_queue.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_queue.rs#app"
```

</TabItem>
</Tabs>
