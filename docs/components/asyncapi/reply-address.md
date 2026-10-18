---
sidebar_position: 3
title: Reply address
description: "A responder's reply channel has no address; the document points a client at $message.header#/reply-to instead."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Reply address

A responder's reply channel has no address; the document points a client at `$message.header#/reply-to` instead.

A responder answers each request at the peer that sent it, so no fixed name addresses its answers.
When the reply position carries a transform that names the destination, as a [responder](../request-reply/responders.md) does, the document reports the reply channel with `address: null`.
The receive operation's `reply.address.location` is `$message.header#/reply-to`, the header the ROUTER stamps on each request.
The channel keeps the fallback name from `reply("..")` as its key.

## Example

The greeter's answer names no destination, and a transform names it per request.
The mount on the [AsyncAPI](./index.md) page puts the transform on the reply position.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_asyncapi.rs#replies"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_asyncapi.rs#replies"
```

</TabItem>
</Tabs>

The reply channel and the operation that answers on it:

```json reference="tests/documents/reply-address.json"
```
