---
sidebar_position: 1
title: Responders
description: "A responder answers at the reply-to address the ROUTER stamps on each request, through a transform that names the destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Responders

A responder answers at the `reply-to` address the ROUTER stamps on each request, through a transform that names the destination.

A responder is an ordinary reply handler on a [`ZmqRpc`](../broker/zmq-rpc.md) subscription.
Its ROUTER socket reads the identity of the peer behind each request and stamps the request with a [`reply-to`](../wire/envelope.md) header that addresses that peer, `zmq-reply:` and the identity in hex.
The reply publisher routes an answer to such an address only.
It refuses any other name with [`ZmqError::Send`](../broker/errors.md), so an answer that misses its address fails where it is sent.

A publish transform on the reply position sets the destination per request.
It reads the delivery being answered, so it implements `PublishTransform<ForReply<C>, Options>`, and it names the destination, so it declares `Destination = Names`.
It copies `reply-to` into the destination and `correlation-id` into the answer's headers, so the [requester](./index.md) matches the answer to its request.
[`get_shared`](api:ruststream::HeaderMap::get_shared) hands each header over as the buffer the request arrived in, so answering costs a reference count rather than a copy.
The answer type derives `Outgoing` without a name: a position offers the naming right only where the type declares no destination.
The name in `reply("reply")` is the fallback that the [document](../asyncapi/reply-address.md) reports.

An answer whose requester has gone is refused with `ZmqError::Send`, naming the reply address: the ROUTER holds no peer under that identity.
A responder addresses no retry copies of its own, so every responder mount names where they go with [`.out_retry(Publish).to("..")`](../settlement/out-retry.md).

[Request and reply](../../guides/request-reply.md) builds a scoring responder with this transform.

## Example

The greeter answers each request at the peer that sent it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_request_reply.rs#responder"
```

</TabItem>
</Tabs>

The mount puts the transform on the reply position.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_request_reply.rs#app"
```

</TabItem>
</Tabs>
