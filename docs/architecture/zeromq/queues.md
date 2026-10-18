---
title: Queues
sidebar_position: 2
description: "The zeromq client keeps no queue of messages: a send writes its frames into a peer's connection, and the kernel's socket buffers hold what is in flight."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Where a message waits between two zeromq sockets, for PUSH/PULL and for PUB/SUB",
  "text": "The diagram as text",
  "pushPull": "PUSH/PULL",
  "pubSub": "PUB/SUB",
  "pushsend": ["PUSH send", "takes the peer", "whose turn it is"],
  "pushwrite": ["Writes the frames", "into that peer's", "connection"],
  "pushkernel": ["Socket buffers", "of the kernel,", "at both ends"],
  "pushrecv": ["PULL recv", "reads its peers'", "connections in turn"],
  "pubsend": ["PUB send", "every subscriber", "whose filter matches"],
  "pubwrite": ["Writes into each", "one's connection,", "one after another"],
  "pubkernel": ["Socket buffers", "of the kernel,", "at both ends"],
  "pubrecv": ["SUB recv", "reads what its", "filters let through"],
  "client": ["The zeromq client keeps no queue of messages:", "a send ends when its frames are written"],
  "libzmq": ["libzmq keeps 1000 messages per connection instead", "(SNDHWM, RCVHWM): at the mark PUB drops, PUSH blocks"],
  "inTurn": "in turn"
};

# Queues

The `zeromq` client keeps no queue of messages: a send writes its frames into a peer's
connection, and the kernel's socket buffers hold what is in flight.

<Diagram name="queues" words={words} />

A PUSH send takes the next peer in turn and writes the frames into that peer's connection. The
send ends when the frames are written, so a peer that reads slowly makes the send wait.

A PULL socket reads a message from each connected peer in turn when it is asked for one. Until
then the frames stay in the kernel's buffers, on this side and on the sender's.

A PUB send writes the message into the connection of every subscriber whose filter matches, one
after another. So the slowest matching subscriber holds the publisher back, and nothing is lost to
a slow one. A message that matches no filter is dropped without an error.

libzmq, the C library most other languages use, works differently. Each connection there has a
queue of its own, 1000 messages by default (the high-water marks `SNDHWM` and `RCVHWM`). When a
queue is full, PUB drops the message and PUSH blocks the send. The `zeromq` client has no such
queue and no such setting: a socket's options are a peer identity and a connect timeout.

What bounds how far a subscription of this crate reads ahead is the crate's own channel,
[`read_ahead`](../../components/subscriptions/read-ahead.md) deep, and
[Backpressure](../backpressure.md) draws it.

The libzmq figures are the
[zguide's](https://zguide.zeromq.org/docs/chapter2/) ("High-Water Marks"). The client's behaviour
is the source of [`zeromq` 0.6](https://docs.rs/zeromq/0.6.0/zeromq/).
