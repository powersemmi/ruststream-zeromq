---
sidebar_position: 3
title: ZmqRpc
description: "ZmqRpc is DEALER/ROUTER: a responder answers each request at the peer that sent it, on whichever side the endpoint takes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ZmqRpc

[`ZmqRpc`](api:ruststream_zeromq::rpc::ZmqRpc) is DEALER/ROUTER: a responder answers each request at the peer that sent it, on whichever side the endpoint takes.

A responder is a subscription on a ROUTER socket.
The ROUTER stamps each request with a `reply-to` header that addresses the peer that sent it.
A publish transform sets the answer's destination to that address, so the reply type declares no destination of its own, and the name in `reply("..")` is the fallback the [AsyncAPI document](../asyncapi/index.md) reports.
The [responders](../request-reply/responders.md) page shows the transform.
An answer whose requester has gone is refused with [`ZmqError::Send`](./errors.md), naming the reply address.

`ZmqRpc` carries no side in its type: [`ZmqRpc::new`](api:ruststream_zeromq::rpc::ZmqRpc::new) takes an endpoint that binds or one that dials.
One policy, [`ZmqRpcPublish`](api:ruststream_zeromq::rpc::ZmqRpcPublish), serves both directions.
It routes answers through the responder's ROUTER, and it sends [requests](../request-reply/index.md) over a DEALER socket of its own through the `RequestReply` capability.
[`rpc::prelude`](api:ruststream_zeromq::rpc::prelude) offers it as `Publish`, together with `RequestReply`.

A responder answers each request at its own address, so [`.batch(..)`](../subscriptions/batches.md) on a responder does not compile.

A copy of a request has no address of its own, so a responder subscription addresses no retry copies.
Every responder mount names where they go, with [`.out_retry(policy).to("..")`](../settlement/out-retry.md), and a mount that names nothing fails startup with an error that names the subscription.
A plain name reaches no peer on DEALER/ROUTER.
A service that wants the copies to arrive sends them through a queue on another broker; a service that does not names a destination to say so and lets the requester ask again.

[Request and reply](../../guides/request-reply.md) builds a scoring responder and a checkout service that asks it.

## Example

A greeter binds `tcp://0.0.0.0:5557` and answers each request at the peer that asked.
Its retry copies go to `greeter.retry`, a name that reaches no peer: a requester that gets no answer asks again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_rpc.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_rpc.rs#app"
```

</TabItem>
</Tabs>
