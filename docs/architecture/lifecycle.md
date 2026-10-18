---
title: Lifecycle
sidebar_position: 3
description: "ZmqQueue::new records the endpoint, connect checks it, subscriptions and binding publishers attach at startup, dialing publishers on first publish."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on ZeroMQ",
  "text": "The diagram as text",
  "states": "Broker",
  "zeromq": "On ZeroMQ",
  "unconnected": ["ZmqQueue::new", "(endpoint)"],
  "connected": ["ConnectedZmqQueue"],
  "closed": ["Closed = ()"],
  "config": ["Endpoint recorded,", "no I/O"],
  "check": ["Address checked:", "tcp:// or ipc://"],
  "subscriptions": ["Each subscription", "binds or dials", "a socket of its own"],
  "publishers": ["Publishers attach:", "bind side at startup,", "dial side on first publish"],
  "flag": ["Marked closed,", "nothing to flush"],
  "afterShutdown": ["A publisher that outlives the connection answers", "ZmqError::NotConnected, never a silent success"],
  "dropped": ["A dropped subscription stops its task", "and frees the endpoint it bound"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`ZmqQueue::new`](../components/broker/zmq-queue.md) records the endpoint, connect checks it,
each subscription and each publisher on the bind side attach a socket at startup, a publisher that
dials on its first publish. [`ZmqFanout`](../components/broker/zmq-fanout.md) and
[`ZmqRpc`](../components/broker/zmq-rpc.md) go through the same steps.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. The runtime connects the broker
when the app starts. Connect checks that the address is `tcp://` or `ipc://` and opens no socket.
Another scheme fails the
[start](../components/broker/startup.md), with an error naming the address.

Each subscription attaches its socket as it opens. On an endpoint this service
[binds](../components/broker/endpoint/bind.md), the subscription listens there, and a bind the
system refuses fails the start. One subscription holds a bound endpoint: a second one on the same
broker is refused with an error naming both. On an endpoint this service
[connects](../components/broker/endpoint/connect.md) to, the subscription dials it. Its peer is
the sending end of the pattern, so from then on a publish through the same broker is refused
before it dials.

A service that only consumes attaches no publisher socket. A publisher on the bind side attaches
its own at startup, one that dials on its first publish, and a send retries for up to five seconds while the connection to its peer
finishes the handshake.

[Shutdown](../components/broker/shutdown.md) marks the connection closed and returns. Nothing is
buffered in this crate, so nothing is flushed, and the closed form carries no report. A publisher
handed out earlier answers [`ZmqError::NotConnected`](../components/broker/errors.md) from then
on.

A subscription stops when the runtime drops it. Dropping it stops the task that reads its socket,
since a receive on a socket with no peer never returns by itself. A subscription that bound its
endpoint frees it at that moment, so a new one can bind the same address.
