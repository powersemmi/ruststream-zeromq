---
title: Backpressure
sidebar_position: 5
description: "A subscription reads its socket at most read_ahead messages ahead of the handler; past that it stops reading, and the wait reaches back to the peer's send."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A subscription's read-ahead: the message goes down the left column, and a full channel's wait climbs back up the right one to the peer",
  "text": "The diagram as text",
  "peer": ["Peer", "PUSH or PUB send"],
  "kernel": ["Socket buffers", "of the kernel"],
  "driver": ["Socket task", "reads one message"],
  "channel": ["Channel", "read_ahead deep"],
  "handler": ["Handler", "or workers(n) lanes"],
  "full": ["The handler is behind:", "the channel is full"],
  "stops": ["The task waits", "and stops reading"],
  "fills": ["The buffers fill, and", "the TCP window closes"],
  "waits": ["The peer's send", "waits for room"],
  "pub": ["On PUB/SUB the PUB waits for its slowest matching", "subscriber; a message no filter matches is dropped"]
};

# Backpressure

A subscription reads its socket at most `read_ahead` messages ahead of the handler; past that it
stops reading, and the wait reaches back to the peer's send.

<Diagram name="backpressure" words={words} />

A subscription's task reads one message at a time off its socket and puts it into a channel. The
channel holds [`read_ahead`](../components/subscriptions/read-ahead.md) messages, 1000 by
default, the receive high-water mark libzmq gives a socket. The broker carries the setting, so
every subscription of that broker reads that far ahead.

When the handler falls behind, the channel fills and the task waits for room. A task that waits
reads nothing, so the frames stay in the kernel's buffers until they fill. Then TCP stops the
sender, and the peer's send waits until the handler catches up. A slow handler slows the sender
down, and this process's memory stays bounded.

On PUB/SUB the waiting sender is the PUB socket. It writes to every matching subscriber in turn,
so one slow subscriber holds back the messages for all of them. A message no subscriber's filter
matches is still dropped, as [Queues](zeromq/queues.md) explains.

The handler takes one message at a time unless the mount site asks for more. `workers(n)` runs
`n` handlers side by side, and a free worker takes the next message from the channel. ZeroMQ
messages carry no key, so `workers(n, by_key)` has nothing to hash, and the messages rotate over
the lanes. A [batch](../components/subscriptions/batches.md) closes at the size the mount site
names, or 20 milliseconds after its first message.

A publish waits the same way when the peer reads slowly. A queue publish can be cancelled while
it waits, and the next publish completes its send. A fan-out publish, a reply and a request
cannot, so [give them up through a timeout](../components/publishing/cancellation.md).
