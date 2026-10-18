---
sidebar_position: 5
description: "Retry a ZeroMQ message later, cap the attempts and park what fails, on a transport that delivers at most once and stores nothing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a message later, caps the attempts and parks what fails on a [dead-letter](../components/settlement/dead-letters.md) queue.

ZeroMQ delivers a message at most once and stores nothing.
A message that a handler fails on is gone unless the service publishes it again.
The framework does that: the mount site declares the cap and where the copies go, and each copy carries a retry count in a header.

The example is a billing service with two workers.
The job worker retries a failed job into its own queue.
The invoice worker parks a failed invoice on a queue of its own, for a person to look at.

## At most once

[`ack`](../components/settlement/index.md) and `nack` have nothing to tell the socket: ZeroMQ has no acknowledgement.
Each of them reports `AckError::Unsupported`, and the runtime logs a warning.
A plain `retry` with no cap therefore brings nothing back, and the handler is not called again.
A retry that must happen goes through a copy, as below.

## The job worker

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#jobs"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#jobs"
```

</TabItem>
</Tabs>

A job that fails asks for another try in 30 seconds with [`retry_after`](../components/settlement/retries.md).
The runtime waits out the delay and publishes a copy of the delivery, its payload and headers as they arrived.
The copy waits in the memory of the service, so a process that stops before the delay is over loses it.

## The invoice worker

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#invoices"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#invoices"
```

</TabItem>
</Tabs>

An invoice without an amount cannot be sent, and sending it again changes nothing.
`send_invoice` returns `retry`, and the mount site turns that into a dead letter.
`park` reads the dead letters and logs each one.

## The mount sites

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) counts every delivery of one job, the first included.
ZMTP has no delivery counter, so the framework counts through its `x-ruststream-retry-count` header, and the cap holds wherever the copies travel.
The fifth delivery of a failing job is not retried; with no dead-letter destination it is dropped with a warning that names the subscription.

[`out_retry`](../components/settlement/out-retry.md) picks the publisher the copies leave through.
The `jobs` queue is bound by this service, so `.out_retry(Publish)` sends each copy back into it, and whichever job worker is free takes it.

The invoice mount declares `dead_letter("invoices.dead")` and no cap, so every failed invoice goes straight there.
A dead letter is a publish like any other, and a bound queue takes only its own subscription's name.
So the dead letters leave through `parked`, a policy on the `dead` broker, and `park` reads them there.
The retry copies of a mount leave through the same publisher, so a mount trades its retries for its dead letters.
A mount that wants both keeps its retries on its own queue and leaves dead-lettering out.

## A worker that dials

A worker that dials a ventilator cannot send a copy back: the ventilator only sends.
Its mount names where the copies go, with `.out_retry(policy).to("jobs")` over a queue the worker binds for itself, and mounts the same handler on that queue.
A mount that names no destination is refused at startup, and a [`Router`](api:ruststream::runtime::Router) chain that names none does not compile.
[`out_retry`](../components/settlement/out-retry.md) shows this mount, and [Pipelines with PUSH/PULL](pipelines.md) uses it.

## Testing the delays

The tests run the app in process.
The job test starts on a paused clock, so `tb.advance(BACKOFF)` lets 30 seconds pass at once.
It checks five deliveries and a last copy that carries the retry count 4.
The invoice test checks that the invoice reached `park` after one delivery.
[Testing a ZeroMQ service](testing.md) covers the harness.
