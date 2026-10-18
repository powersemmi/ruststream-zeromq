---
sidebar_position: 3
title: Dead letters
description: "dead_letter names where a spent delivery goes; on ZeroMQ that is a publisher on another endpoint, because a name on the own one stays home."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`dead_letter(name)`](api:ruststream::runtime::Mounting::dead_letter) names where a spent delivery goes; on ZeroMQ that is a publisher on another endpoint, because a name on the own one stays home.

When a [retry](./retries.md) would go past the cap of [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts), the runtime publishes the delivery under the dead-letter name instead, payload and headers as they arrived.
A mount with a dead-letter name and no cap sends every failed delivery there at once.
A cap with no dead-letter name drops the spent delivery with a warning naming the subscription.

A name is frame 0, not an address, and the dead letter leaves through the [retry publisher](./out-retry.md).
On the subscription's own bound queue that publisher refuses `jobs.dead` with [`ZmqError::Send`](../broker/errors.md), and the delivery is dropped with a warning.
On the subscription's own bound fan-out `jobs.dead` matches the prefix `jobs` and comes back to the subscription that gave up on it.

So a dead letter leaves through a publisher on another endpoint, set with `.out_retry(..)`.
The retry copies leave through that publisher as well, so such a mount sends every failed delivery to the dead letters, with no retry first.
A handler that should see its retries again keeps `.out_retry(..)` on its own endpoint and declares no dead-letter name.

[Retries and dead letters](../../guides/retries.md) parks failed invoices on a queue of their own.

## Example

A job that fails leaves for a second queue the service binds, where `park` reads it under `jobs.dead`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_settlement_dead_letters.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_settlement_dead_letters.rs#app"
```

</TabItem>
</Tabs>
