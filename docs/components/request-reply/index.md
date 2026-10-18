---
title: Request-reply
description: "request sends over a DEALER of its own and resolves with the answer matching its correlation-id, or RequestTimeout."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request-reply

`request` sends over a DEALER of its own and resolves with the answer matching its `correlation-id`, or `RequestTimeout`.

[`ZmqRpcPublisher`](api:ruststream_zeromq::rpc::ZmqRpcPublisher) implements the core's [`RequestReply`](api:ruststream::RequestReply) capability, and it is the only publisher of the crate that does.
A handler bounds its slot `Out<impl RequestReply>`, and the mount site binds `ZmqRpcPublish` to it.
[`request(message, timeout)`](api:ruststream::RequestReply::request) takes an [`OutgoingMessage`](api:ruststream::OutgoingMessage) of bytes the handler encoded, and its name is the name frame of the request.
The request goes to the broker directly, past the slot's transforms and the app's publish layers.

Each request opens a DEALER socket of its own and dials the broker's endpoint.
The dial waits as long as the timeout allows, ten seconds at most, and a failed dial returns [`ZmqError::Endpoint`](../broker/errors.md).
The request carries a `correlation-id` header: the one the caller set, or a fresh one.
The DEALER keeps reading until an answer carries the same id, and the call returns that answer as a [`ZmqMessage`](api:ruststream_zeromq::ZmqMessage), which the handler decodes.
With no matching answer within the timeout, the call returns `ZmqError::RequestTimeout`.
The [responder](./responders.md) on the other side copies the id onto its answer.

The timeout is the way to give up on a request: dropping the call closes its DEALER, as [Cancellation](../publishing/cancellation.md) describes.
After shutdown a request returns `ZmqError::NotConnected`.

## Example

A handler on a queue asks the greeter for each name it receives, and acknowledges once the answer came back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_request_reply.rs#request"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_request_reply.rs#request"
```

</TabItem>
</Tabs>

The greeter here runs in the same service, on a ROUTER it binds, so the requests dial `tcp://0.0.0.0:5557`.

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
