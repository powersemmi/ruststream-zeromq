---
sidebar_position: 8
title: Errors
description: "ZmqError is the one error type of the crate: endpoint, send, receive, wire, request timeout, not connected, invalid."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`ZmqError`](api:ruststream_zeromq::ZmqError) is the one error type of the crate: endpoint, send, receive, wire, request timeout, not connected, invalid.

Each variant names the step that failed.
The ZeroMQ client's own error is the `source` of `Endpoint`, so the client's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Endpoint` | a socket could not bind or dial its address, such as a port another process holds at [startup](./startup.md), or a peer that did not listen within 30 seconds |
| `Send` | a message could not leave: a publish its own subscription would receive back, a publish to a peer that only sends, an answer to a requester that has gone, a [header](../wire/headers.md) the text frame cannot carry, or no peer within five seconds |
| `Receive` | an open subscription failed to receive |
| `Wire` | a peer sent frames that do not follow the [wire layout](../wire/index.md) |
| `RequestTimeout` | a [request](../request-reply/index.md) got no answer within its timeout |
| `NotConnected` | a publisher, a responder or a request was used after the broker [shut down](./shutdown.md) |
| `Invalid` | an address with a scheme other than `tcp://` or `ipc://`, or a second subscription on an endpoint the broker [binds](./endpoint/bind.md) |

A service meets the error in two places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A result the socket refuses is logged with its cause, and the job is settled as a retry in a second.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
