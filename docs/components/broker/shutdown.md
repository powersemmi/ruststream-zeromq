---
sidebar_position: 6
title: Shutdown
description: "shutdown closes the connected form at once; a publisher handed out earlier reports ZmqError::NotConnected and never sends."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

`shutdown` closes the connected form at once; a publisher handed out earlier reports [`ZmqError::NotConnected`](./errors.md) and never sends.

The crate keeps no queue of its own, so the broker has nothing to flush and its shutdown returns at once.
The runtime first stops the subscriptions and lets the handlers in flight finish.
Then it shuts each broker down, and a subscription's socket closes as the runtime drops it.

A publisher can outlive the broker: one a startup hook kept, one a sibling task took from [`RunningApp::publisher`](api:ruststream::runtime::RunningApp::publisher), or a clone of either.
After shutdown, a publish through it returns `ZmqError::NotConnected` and sends nothing.
An answer and a request after shutdown fail the same way.

## Example

The service announces on a fan-out that it has started.
The hook's publisher lives as long as the hook; a task that kept it past shutdown would get `ZmqError::NotConnected` from its next publish.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
