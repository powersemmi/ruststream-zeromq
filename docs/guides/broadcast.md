---
sidebar_position: 3
description: "Announce events on PUB/SUB: a worker that publishes on a fan-out it binds, watchers that subscribe by prefix, late joiners and slow watchers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Broadcast with PUB/SUB

This guide announces events on PUB/SUB: one service publishes, and every watcher that subscribed receives a copy.

The example has two services.
The worker takes jobs and announces each finished job on a fan-out.
The watcher subscribes to every job announcement.

## The announcer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/broadcast.rs#announcer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/broadcast.rs#announcer"
```

</TabItem>
</Tabs>

The worker reads jobs on a [`ZmqQueue`](../components/broker/zmq-queue.md) and announces on a [`ZmqFanout`](../components/broker/zmq-fanout.md), the PUB/SUB pattern.
A file that mounts two patterns imports the crate's [prelude](../components/broker/prelude.md), which names each policy after its pattern: `ZmqFanoutPublish` here.
The announcer is the stable side, so it binds, and the watchers dial it.
`.out_reply(announce)` publishes every `JobDone` under its name, `jobs.done`.

## The watcher

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/broadcast_watcher.rs#watcher"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/broadcast_watcher.rs#watcher"
```

</TabItem>
</Tabs>

On a fan-out the subscription name is a [prefix](../components/subscriptions/prefixes.md) of the message name, compared byte by byte.
`jobs.` receives `jobs.done`, `jobs.failed` and any other `jobs.` name, and nothing else.
`ctx.name()` is the name the message arrived under.
Every watcher that dials the announcer receives its own copy of each announcement.

The watcher dials, so its mount names where retry copies go, even though `watch` never asks for a retry.
The copies go to a queue the watcher binds for itself, and the same handler reads it.
[Retries and dead letters](retries.md) covers this rule.

## Late watchers

A fan-out stores nothing.
A watcher receives what is published after its subscription has reached the publisher, and nothing from before.
A watcher that starts late, or reconnects, misses the announcements in between.
An announcement that matches no watcher is dropped without an error.

A service that announces to its own subscription loses nothing to this: its publisher reads that subscription's filter before the first send.

## Slow watchers

A watcher reads at most [`read_ahead`](../components/subscriptions/read-ahead.md) deliveries ahead of its handler, 1000 by default.
Past that it stops reading, and the socket buffers fill.
The publisher then waits for the slowest watcher whose prefix matches.
So a slow watcher slows the announcer down instead of losing announcements.

## Retries on a fan-out

A retry copy on a fan-out is a publish like any other: it reaches every subscription whose prefix matches.
On a fan-out the service binds, a copy goes back to the whole audience the original had.
A [dead-letter](../components/settlement/dead-letters.md) name on the same endpoint matches the prefix too: `jobs.dead` arrives on `jobs.` and is handled again.
A fan-out's dead letters go to an endpoint of their own.

## Testing

The announcer's test pushes a job and reads the announcement published on `jobs.done`.
The watcher's test publishes `jobs.done`, `jobs.failed` and `metrics.cpu` as the announcer would, and checks that `watch` received the first two.
The in-process mode applies the prefix rule; late joiners and back-pressure exist only on real sockets.
[Testing a ZeroMQ service](testing.md) covers both modes.
