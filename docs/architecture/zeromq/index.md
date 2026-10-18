---
title: Sockets
sidebar_position: 0
description: "ZeroMQ is a library of sockets that talk to each other directly: no server, nothing stored, and a pattern that decides who receives each message."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The three socket patterns: a pipeline, a broadcast filtered by prefix, and requests told apart by connection, with no server between the peers",
  "text": "The diagram as text",
  "pipeline": "PUSH/PULL: a pipeline",
  "ventilator": ["Ventilator: PUSH", "binds"],
  "worker1": ["Worker 1", "PULL, dials", "gets 1 and 4"],
  "worker2": ["Worker 2", "PULL, dials", "gets 2"],
  "worker3": ["Worker 3", "PULL, dials", "gets 3"],
  "sink": ["Sink: PULL, binds", "reads them in turn"],
  "dealt": "in turn",
  "pubsub": "PUB/SUB: a broadcast",
  "pub": ["Publisher: PUB, binds", "matches the filters"],
  "jobs": ["SUB, dials", "filter jobs."],
  "prices": ["SUB, dials", "filter prices"],
  "unmatched": ["orders.new:", "no filter", "matches, dropped"],
  "jobsDone": "jobs.done",
  "pricesEur": "prices.eur",
  "reqrep": "DEALER/ROUTER: request and reply",
  "responder": ["Responder: ROUTER, binds", "an identity per connection"],
  "dealerA": ["Requester: DEALER", "dials"],
  "dealerB": ["Requester: DEALER", "dials"],
  "identityA": "identity A",
  "identityB": "identity B"
};

# Sockets

ZeroMQ is a library of sockets that talk to each other directly: no server, nothing stored, and a
pattern that decides who receives each message.

<Diagram name="sockets" words={words} />

Every box in the picture is a socket in some process. A message goes from one socket to another
over a connection between them. Nothing keeps it after that, so a socket that is not connected at
the time never sees it.

Which side listens and which side dials is independent of which way messages flow. The stable
part of a topology binds, and the parts that come and go dial it. In the pipeline the ventilator
and the sink bind, and the workers dial both. A [`ZmqEndpoint`](../../components/broker/endpoint/index.md)
states the side this service takes.

A PUSH socket deals its messages to its connected peers in turn, one message to one peer. A PULL
socket reads from all of its peers in turn, so no peer starves the others. This is
[`ZmqQueue`](../../components/broker/zmq-queue.md).

A SUB socket sends its filters to the PUB it is connected to. The PUB compares the first frame of
each message with every filter by prefix and sends the message only where one matches. A message
no filter matches is dropped, and the publisher is not told. This is
[`ZmqFanout`](../../components/broker/zmq-fanout.md), where a subscription's name is its
[filter](../../components/subscriptions/prefixes.md).

A ROUTER socket gives each connection an identity. It puts the identity in front of every message
it receives, and it sends a message to the connection whose identity is the first frame. A DEALER
sends and receives with no identity frame. This is [`ZmqRpc`](../../components/broker/zmq-rpc.md),
and [Frames](frames.md) draws the envelope.

A socket may have many peers, and order holds between one pair of sockets only. `ruststream-zeromq`
runs on `tcp://` and `ipc://`. Another scheme is refused when the broker connects, before any
socket opens.

The patterns are ZeroMQ's own
([pipeline](https://rfc.zeromq.org/spec/30/), [publish-subscribe](https://rfc.zeromq.org/spec/29/),
[request-reply](https://rfc.zeromq.org/spec/28/)), and the
[zguide](https://zguide.zeromq.org/docs/chapter2/) explains
which side binds.
