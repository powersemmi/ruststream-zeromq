---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on ZeroMQ, which settles nothing: an ack answers Unsupported, and a retry is a copy the service publishes."
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "Each outcome of a handler on ZeroMQ: what the delivery answers and what becomes of the message",
  "text": "The diagram as text",
  "handlerLane": "Handler",
  "wireLane": "Delivery",
  "deliveryLane": "Message",
  "ack": ["ack()"],
  "ackWire": ["AckError::", "Unsupported"],
  "ackEffect": ["done: there", "is nothing", "to settle"],
  "retry": ["retry()"],
  "retryWire": ["AckError::", "Unsupported"],
  "retryEffect": ["gone, unless", "a cap is declared:", "then a copy"],
  "retryAfter": ["retry_after(d)"],
  "retryAfterWire": ["a copy", "after d"],
  "retryAfterEffect": ["the copy is a", "new message,", "counted"],
  "drop": ["drop()"],
  "dropWire": ["AckError::", "Unsupported"],
  "dropEffect": ["gone"],
  "atMostOnce": ["Delivery is at most once: a message the handler", "does not finish is not delivered again"]
};

export const retries = {
  "title": "A retry on ZeroMQ: the attempt cap decides between a copy, the dead-letter destination and a drop",
  "text": "The diagram as text",
  "delivery": ["A delivery", "x-ruststream-retry-count says which attempt it is"],
  "outcome": ["The handler returns", "retry_after(delay), or retry() under a cap"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "own": ["Bound:", "a copy to", "its own", "listener"],
  "named": ["Dialing or", "responder:", "a copy to", "a named place"],
  "deadLetter": ["At the cap:", "a copy to", "the dead-", "letter name"],
  "dropped": ["At the cap,", "no dead", "letter:", "dropped"],
  "next": ["The copy is a new", "message, one more attempt"],
  "crash": ["A copy waits for its delay inside the process:", "a crash before the timer fires loses it"],
  "handled": "handled",
  "retries": "a retry",
  "below": "below",
  "atCap": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and [`retry_after`](../components/settlement/retries.md) become on
ZeroMQ, which settles nothing: an ack answers `Unsupported`, and a retry is a copy the service
publishes.

<Diagram name="settlement" words={table} />

A ZeroMQ socket hands a message over and forgets it. There is no acknowledgement in the protocol,
so [`ack` and `nack`](../components/settlement/index.md) on a `ZmqMessage` answer
`AckError::Unsupported` at once. The runtime logs that answer as a warning, and the message is
done either way.

A plain `retry()` delivers nothing again, and the handler is not called a second time. A
registration that declares [`max_attempts`](../components/settlement/retries.md) or a
[dead-letter destination](../components/settlement/dead-letters.md) turns `retry()` into a copy,
as for `retry_after`.

ZeroMQ counts no deliveries, so `redelivery_count` is empty and the runtime counts the attempts
itself. The count travels in the copy's `x-ruststream-retry-count` header, and the first delivery
counts as one.

## Retries

<Diagram name="retries" words={retries} />

A `retry_after` outcome waits for the delay inside the process, then publishes a copy through the
registration's [retry publisher](../components/settlement/out-retry.md). Where the copy goes
depends on the pattern and on the side of the endpoint, and the type states it.

A [`ZmqQueue`](../components/broker/zmq-queue.md) or
[`ZmqFanout`](../components/broker/zmq-fanout.md) subscription that binds its endpoint reports its
own name as the address. The retry publisher dials the subscription's own listener, and the copy
arrives there as the next attempt. The mount site names nothing.

A subscription that dials reads from a peer that only sends. A
[`ZmqRpc`](../components/broker/zmq-rpc.md) responder's name is not a destination: its publisher
sends only to the identities requests carry. Neither has an address for a copy, so the mount site
names where the copies go, usually a queue the service binds for itself. A router chain that names no destination does not compile, and a
registration that names none is refused at startup.

At the cap, the delivery goes to the dead-letter destination through the same publisher, so the
destination belongs to a publisher on another endpoint. On the endpoint a queue subscription binds,
a dead letter would come back to that subscription: the publisher refuses it, and the delivery is
dropped. On a bound fan-out, a dead-letter name under the subscription's prefix arrives back at
it. With no dead-letter destination the delivery is dropped, with a warning.
