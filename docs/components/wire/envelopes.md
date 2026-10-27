---
sidebar_position: 2
title: Envelopes
description: "An envelope is the frame layout of a message: Frames by default, Framed for one codec document, NoOp for the payload alone, or a layout of your own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelopes

An envelope is the frame layout of a message: `Frames` by default, `Framed` for one codec document, `NoOp` for the payload alone, or a layout of your own.

A broker reads and writes through one envelope, named with `.envelope(..)` on the broker: `ZmqQueue::new(endpoint).envelope(NoOp)`.

A publish policy writes through its broker's envelope, or through its own when it names one: `Publish.envelope(NoOp)`.
The envelope is a type parameter of the broker and the publisher, so the choice costs nothing per message.

The built-in envelopes:

| Envelope | Frames | Headers |
|---|---|---|
| [`Frames`](api:ruststream_zeromq::envelope::Frames) | name, header lines, payload ([Wire format](./index.md)) | `name: value` lines |
| [`Framed<Codec>`](api:ruststream_zeromq::envelope::Framed) | name, then one document of the codec: `{"headers": {..}, "payload": ..}` | the document's `headers` map |
| [`NoOp`](api:ruststream_zeromq::envelope::NoOp) | the payload alone; on the fan-out, the name and the payload | none |

`Frames` is the default and the layout of earlier releases, so a service that names no envelope stays wire-compatible.
`Framed` suits a peer that reads every message with one parser.
It writes the payload as text when it is UTF-8, as the JSON codec's output always is, and as bytes otherwise.
`NoOp` suits a plain ZeroMQ peer, which sends and reads the payload and nothing else.

A message that is not in the envelope's layout is a peer's own payload.
A single frame reads as the payload, with an empty name and no headers, whichever envelope reads it.
A `Framed` second frame that is not the document, exactly its two fields, reads as the payload too.
A payload that looks like a document is safe: `Framed` always writes the payload inside a document of its own.

A delivery reads its headers on the first call to `headers()`, and keeps them.
A handler that reads no header never parses the envelope.
Headers that do not read are logged against the message, and the delivery has none.

`NoOp` carries no headers.
A publish through it that carries a header fails with [`ZmqError::Send`](../broker/errors.md), naming the header and the destination.
A retry copy carries the framework's retry counter in a header, so `retry_after` needs an envelope with headers.
On DEALER/ROUTER a request read through `NoOp` still carries the `reply-to` address of the peer that asked, and a requester takes the first answer on its DEALER, with no `correlation-id`.

An envelope of your own implements [`Envelope`](api:ruststream_zeromq::envelope::Envelope): `open` reads the frames a socket received, `headers` reads the headers out of what `open` kept, and `seal` writes a message.
The test harness runs a broker in process by its type, so a test registers a broker on your envelope once: `ruststream::register_testable_broker!(ZmqFanout<Bind, Spaced>);`.

The generated [AsyncAPI](../asyncapi/index.md) document names the envelope on each server and on each message a publish position writes, with the media type of the `Framed` document.

## Example

A feed's PUB socket sends each tick as one `name payload` frame.
The bridge reads it through an envelope of its own:

{/* single-form: an Envelope impl is plain trait code, the same under the macros and the manual API */}
```rust reference="examples/zmq_wire_envelopes.rs#envelope"
```

Each price leaves the bridge as one JSON document on a fan-out.
Beside it, orders arrive in the default three frames, and each audit record leaves for a sink that reads bare payloads.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_wire_envelopes.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_wire_envelopes.rs#app"
```

</TabItem>
</Tabs>
