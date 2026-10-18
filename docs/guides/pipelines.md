---
sidebar_position: 2
description: "Build a ventilator, workers and sink pipeline on PUSH/PULL: who binds, workers that compete, results to a sink, and batches."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Pipelines with PUSH/PULL

This guide builds the pipeline of the [ZeroMQ guide](https://zguide.zeromq.org/docs/chapter1/#Divide-and-Conquer): a ventilator pushes jobs, workers compete for them, and a sink collects the results.

The example has two services.
The worker takes jobs from the ventilator and pushes a result for each to the sink.
The sink collects the results in batches.
The ventilator is any peer that pushes jobs, such as the Python peer of [Getting started](getting-started.md).

## Who binds

The stable side of a connection binds, and the side that comes and goes dials.
In a pipeline the ventilator and the sink are stable, so they bind.
The workers dial both, and one more worker process is one more dial.

[`ZmqEndpoint::bind`](../components/broker/endpoint/bind.md) and [`ZmqEndpoint::connect`](../components/broker/endpoint/connect.md) state the side.
The side is part of the broker's type: a broker built on `connect` is a `ZmqQueue<Connect>`.
It decides what the broker may publish and where a retry copy can go, so a mistake is caught at compile time or at startup, not in production.

## The worker

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipelines.rs#work"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipelines.rs#work"
```

</TabItem>
</Tabs>

`work` takes a job and returns a `Done`.
`Done` names its destination itself, `results`, so the subscriber clause carries a bare `reply`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipelines.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipelines.rs#app"
```

</TabItem>
</Tabs>

The `ventilator` broker dials the ventilator, and its subscription reads the jobs.
Workers that dial one ventilator compete: PUSH hands each job to one worker, in turn.

The results leave through a second broker, `sink`, which dials the sink.
`bindable()` turns a broker into one that publish policies can be bound to, and `sink.bind(Publish)` is a policy that publishes through it.
`.out_reply(results)` sends every `Done` there.
The worker cannot reply through the `ventilator` broker: the ventilator only sends, so the broker refuses every publish once its subscription has dialed.
[Replies](../components/publishing/replies.md) covers the refusals.

A subscription that dials also names where its [retry copies](../components/settlement/out-retry.md) go, even when its handler never asks for a retry.
Here they go to `retries`, a queue the worker binds for itself, and the same handler reads it.
A copy carries the name `jobs`, the name of the subscription that takes it back.
[Retries and dead letters](retries.md) covers retries in full.

## The sink

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipeline_sink.rs#sink"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipeline_sink.rs#sink"
```

</TabItem>
</Tabs>

The sink binds the address the workers dial.
A bound PULL socket takes whatever any worker pushes to it, so one subscription reads the results of every worker.
`collect` takes a slice, so it receives [batches](../components/subscriptions/batches.md).
A batch closes once it holds 32 results, or 20 ms after its first result, whichever comes first.
The frames on the wire do not change: the batch is gathered on the sink's side.

## One kind of work per endpoint

PUSH and PULL do not look at the name frame.
A bound queue hands its subscription every message pushed to it, whatever the name says.
So a second kind of work, such as reports next to jobs, gets an endpoint of its own and a broker of its own.
A second subscription on a bound endpoint is refused at startup, and the error names both subscriptions.

## Order and keys

PUSH hands messages to the attached workers in turn and does not read a key.
Two jobs of one customer may run on two workers at the same time.
A handler that must see one customer's jobs in order runs in one worker process.
Inside one process, [`workers`](api:ruststream::runtime::Workers) at the mount site runs several deliveries of a handler at once.
A ZeroMQ message carries no key, so lanes keyed with `by_key` take its deliveries in turn.

## Testing the pipeline

Each example file holds a test that starts its service with `TestApp::start`.
The worker's test pushes a job as the ventilator would and reads the result published to `results` on the `sink` broker.
The sink's test pushes three results at once and checks that `collect` received them as one batch.
[Testing a ZeroMQ service](testing.md) covers the harness.
