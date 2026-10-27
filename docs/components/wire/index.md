---
title: Wire format
description: "In the default envelope every message is three frames, name, headers, payload, so a peer in any language composes one with send_multipart."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Wire format

In the default envelope every message is three frames, name, headers, payload, so a peer in any language composes one with `send_multipart`.

ZeroMQ carries messages made of frames and leaves their meaning to the application.
This crate gives every message on PUSH/PULL and PUB/SUB the same three frames: the name, the headers and the payload.
The layout is the [`Frames`](api:ruststream_zeromq::envelope::Frames) envelope, which every broker reads and writes unless it names another; [Envelopes](./envelopes.md) shows the others.

The name frame is the subscription's name on the fan-out, matched as a [prefix](../subscriptions/prefixes.md); a queue takes every message whatever its name says.
The [header frame](./headers.md) is text, and an empty frame means no headers.
A message of two frames, a name and a payload, reads as a message without headers, so a minimal peer can skip the middle frame.
A message of one frame is what a plain ZeroMQ peer sends: that frame is the payload, whole, the name is empty and there are no headers.
On the fan-out a SUB matches its prefix against that same frame, so a plain PUB's message reaches a subscription whose name prefixes it, and the handler reads the frame as it was sent, prefix included.
A message of more than three frames has no place for the frames past the payload, and it surfaces on the subscription as [`ZmqError::Wire`](../broker/errors.md), naming how many frames it had.
The payload frame is what the service's codec wrote, so a peer agrees on the codec only: with the default JSON codec it is the document the handler's input type deserializes from.
Bytes a service already holds encoded go out through a `#[derive(Outgoing, Serialized)]` newtype, and no codec runs on them.
A name frame that is not UTF-8 surfaces on the subscription as [`ZmqError::Wire`](../broker/errors.md).

On DEALER/ROUTER a request has the same three frames, and its answer travels behind the requester's identity, as [Identity frame](./identity.md) shows.

[Peers in other languages](../../guides/interop.md) feeds, watches and asks a service from Python peers.

## Example

```text
frame 0: name      UTF-8; the fan-out's subscribers filter on it as a prefix
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   the codec's output
```

A Python peer pushes a job into a worker with pyzmq:

```python
socket.send_multipart([b"jobs", b"content-type: application/json", b'{"id": 2}'])
```

The worker reads the jobs a Python PUSH socket sends, and pushes each result to a sink that a Python PULL socket binds.
The sink reads the three frames `results`, the header lines and the JSON document `{"id":2}`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_wire.rs#handler"
```

```rust reference="examples/zmq_wire.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_wire.rs#handler"
```

```rust reference="examples/manual/zmq_wire.rs#app"
```

</TabItem>
</Tabs>
