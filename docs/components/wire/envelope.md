---
sidebar_position: 2
title: Envelope
description: "On DEALER/ROUTER a request carries correlation-id, and its answer travels behind the requester's identity with frame 0 holding reply."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelope

On DEALER/ROUTER a request carries `correlation-id`, and its answer travels behind the requester's identity with frame 0 holding `reply`.

A requester sends the usual [three frames](./index.md) from a DEALER socket, with a `correlation-id` line in the header frame.
The responder's ROUTER puts the DEALER's identity in front of them; ZeroMQ calls that frame the envelope.
The responder reads the identity off and gives the request a `reply-to` header that addresses it.

The answer goes back through the ROUTER as `[identity, "reply", headers, payload]`.
The ROUTER takes the identity frame off to route it, so the DEALER reads three frames: the literal `reply`, the header lines with the request's `correlation-id`, and the payload.
The requester matches the answer to its request by that id, because frame 0 is the same on every answer.
The `correlation-id` reaches the answer through the responder's [transform](../request-reply/responders.md).

[Peers in other languages](../../guides/interop.md) asks a responder from a Python DEALER.

## Example

A Python requester:

```python
dealer.send_multipart([b"greeter", b"correlation-id: ask-1", b'{"who": "world"}'])
name, headers, payload = dealer.recv_multipart()
```

The greeter binds `tcp://0.0.0.0:5557` and answers each DEALER that asks.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_wire_envelope.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_wire_envelope.rs#app"
```

</TabItem>
</Tabs>
