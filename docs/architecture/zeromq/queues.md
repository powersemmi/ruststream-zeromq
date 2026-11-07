---
title: Queues
sidebar_position: 2
description: "The zeromq client queues up to 100,000 messages per peer, and a send ends once its message is queued; the crate's publishers wait long before that."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Where a message waits between two zeromq sockets, for PUSH/PULL and for PUB/SUB",
  "text": "The diagram as text",
  "pushPull": "PUSH/PULL",
  "pubSub": "PUB/SUB",
  "pushsend": ["PUSH send", "takes the peer", "whose turn it is"],
  "pushwrite": ["Queues it for that", "peer; a task writes", "the queue out"],
  "pushkernel": ["Socket buffers", "of the kernel,", "at both ends"],
  "pushrecv": ["PULL recv", "reads its peers'", "connections in turn"],
  "pubsend": ["PUB send", "every subscriber", "whose filter matches"],
  "pubwrite": ["Queues a copy for", "each; a full queue", "drops its copy"],
  "pubkernel": ["Socket buffers", "of the kernel,", "at both ends"],
  "pubrecv": ["SUB recv", "reads what its", "filters let through"],
  "client": ["The zeromq client queues 100,000 messages per peer:", "a send ends when its message is queued"],
  "libzmq": ["libzmq queues 1000 messages per connection", "(SNDHWM, RCVHWM): at the mark PUB drops, PUSH blocks"],
  "inTurn": "in turn"
};

# Queues

The `zeromq` client queues up to 100,000 messages per peer, and a send ends once its message is
queued. A task per peer writes the queue into that peer's connection, and the kernel's socket
buffers hold what is in flight.

<Diagram name="queues" words={words} />

A PUSH send takes the next peer in turn and queues the message for it. Only a full queue makes
the send wait, so the client alone lets a sender run 100,000 messages ahead of a peer that reads
slowly. A queue publisher of this crate waits long before that, after a window of 256 messages or
1 MiB, as [Backpressure](../backpressure.md) explains.

A PULL socket reads a message from each connected peer in turn when it is asked for one. Until
then the frames stay in the kernel's buffers, on this side and on the sender's.

A PUB send queues a copy of the message for every subscriber whose filter matches, and it never
waits: a copy that a full queue cannot take is dropped. A fan-out publisher of this crate holds
itself back with the same window, so a subscriber that reads slowly holds it back instead of losing
messages. A message that matches no filter is dropped without an error.

libzmq, the C library most other languages use, keeps a queue per connection too, 1000 messages
by default (the high-water marks `SNDHWM` and `RCVHWM`). When a queue is full, PUB drops the
message and PUSH blocks the send. The `zeromq` client's queue holds 100,000 messages and has no
setting: a socket's options are a peer identity, a connect timeout and the receive buffer.

What bounds how far a subscription of this crate reads ahead is the crate's own channel,
[`read_ahead`](../../components/subscriptions/read-ahead.md) deep, and
[Backpressure](../backpressure.md) draws it.

The libzmq figures are the
[zguide's](https://zguide.zeromq.org/docs/chapter2/) ("High-Water Marks"). The client's behaviour
is the source of [`zeromq` 0.7](https://docs.rs/zeromq/0.7.0/zeromq/).
