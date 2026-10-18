---
sidebar_position: 1
title: Retries
description: "retry_after publishes a copy after the delay, and max_attempts caps the copies through the framework's x-ruststream-retry-count header."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) publishes a copy after the delay, and [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) caps the copies through the framework's `x-ruststream-retry-count` header.

The runtime waits out the delay and publishes a copy of the delivery, its payload and headers as they arrived.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) incremented, and a handler reads it like any other header.
The copy waits in the memory of the service, so a process that stops before the delay is over loses it.

`max_attempts(n)` caps the deliveries of one message at the mount site, the first one included.
ZMTP has no delivery counter, so the framework counts through that header, and the cap holds wherever the copies travel.
Under a cap a plain [`retry`](api:ruststream::runtime::HandlerOutcome::retry) travels as a copy too.
The delivery that reaches the cap goes to the [dead-letter destination](./dead-letters.md), and with none it is dropped with a warning naming the subscription.

The copies leave through the [`Retry`](api:ruststream::runtime::Retry) slot, which [`out_retry`](./out-retry.md) sets.
On a queue the service binds, a copy goes back into the same queue.
The copy carries the bytes of the delivery, so a codec named at the retry position encodes nothing.
A `.transform(..)` there reads the delivery it retries, which is where a service marks a redelivery.

## Example

A job that fails gets five deliveries, 30 seconds apart, and each copy returns to the queue the worker binds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_retries.rs#retries"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_retries.rs#retries"
```

</TabItem>
</Tabs>
