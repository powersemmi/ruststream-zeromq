---
title: Path
sidebar_position: 4
description: "The way of one ZeroMQ message through a RustStream service: the socket task, the channel, the handler, the reply's frames and the send."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one ZeroMQ message that is answered, from the peer's frames to the reply's frames",
  "text": "The diagram as text",
  "peer": ["Peer", "sends three frames"],
  "driver": ["Socket task", "owns the socket, reads one message"],
  "read": ["ZmqMessage", "name, headers, and the payload frame"],
  "channel": ["Channel", "read_ahead deep"],
  "dispatch": ["Dispatch", "one at a time, or a batch"],
  "decode": ["Decode", "read in place from the payload frame"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into a buffer it hands over"],
  "frames": ["Frames", "the buffer becomes the payload frame"],
  "publisher": ["Publisher", "sends, retrying while the peer attaches"],
  "settle": ["Settlement", "ack answers Unsupported"]
};

# Path

The way of one ZeroMQ message through a RustStream service: the socket task, the channel, the
handler, the reply's frames and the send.

<Diagram name="path" words={words} />

The task that owns a subscription's socket receives one message at a time. It splits the frames
into a name, the headers and the payload. The payload is the received frame itself, not a copy,
and the handler's input is decoded from it in place.

The message goes into the subscription's channel, and the dispatch loop takes it from there. A
[batch handler](../components/subscriptions/batches.md) gets the messages already in the channel,
up to the size the mount site names, or what arrived within 20 milliseconds of the first.

The handler's reply is encoded into a buffer the publisher takes over. The buffer becomes the
payload frame, and the name and the headers go in frames of their own in front of it, so the
payload is not copied. The [publisher](../components/publishing/index.md) attaches its socket on its first
publish. A queue send that finds no peer connected yet retries every 50 milliseconds, for up to
five seconds, and then fails with an error naming the destination.

The delivery settles after the reply. ZeroMQ has nothing to settle, so the ack answers
`Unsupported` at once, as [Settlement](settlement.md) draws. What a message costs on this path,
in instructions and allocations, is on [Benchmarks](benchmarks/index.md#cost-of-the-code).
