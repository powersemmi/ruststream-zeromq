---
sidebar_position: 2
title: connect
description: "ZmqEndpoint::connect makes this process dial: dialing subscriptions share a broker, and a broker that only publishes sends as usual."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# connect

[`ZmqEndpoint::connect`](api:ruststream_zeromq::ZmqEndpoint::connect) makes this process dial: dialing subscriptions share a broker, and a broker that only publishes sends as usual.

Each subscription that dials is a socket of its own on the peer's endpoint.
So one broker takes several of them: two [`ZmqQueue`](../zmq-queue.md) workers dialing one ventilator split its stream, and two [`ZmqFanout`](../zmq-fanout.md) watchers dialing one publisher each receive their own prefix.

A subscription that dials reads from the sending end of its pattern, a ventilator's PUSH socket or a publisher's PUB socket, and that peer takes nothing.
So once a subscription of this service has dialed the endpoint, a publish through the same `ZmqQueue` or `ZmqFanout` is refused with [`ZmqError::Send`](../errors.md), naming the subscription.
For the same reason a dialing subscription's retry copies go where its mount site names.

A broker that only publishes, such as a worker's link to the sink that collects its results, dials and sends as usual.

## Example

A pipeline worker dials both ends: the ventilator it pulls jobs from and the sink it pushes results to.
Every copy of the worker runs the same app, so the service scales by starting more of them.
The results and the retry copies of failed jobs leave for the sink.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_connect.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_connect.rs#app"
```

</TabItem>
</Tabs>
