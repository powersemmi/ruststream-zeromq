---
sidebar_position: 5
title: Startup
description: "connect checks the endpoint without I/O; each subscription attaches its socket as it opens, and a refused bind fails the start naming it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

`connect` checks the endpoint without I/O; each subscription attaches its socket as it opens, and a refused bind fails the start naming it.

When the runtime connects a broker, the connect checks the endpoint's scheme and opens nothing.
An address that is neither `tcp://` nor `ipc://` fails startup with [`ZmqError::Invalid`](./errors.md), naming the address.

Each subscription opens its own socket as the runtime opens the subscription.
A subscription on a [bound](./endpoint/bind.md) endpoint binds it, and a port another process holds fails startup with [`ZmqError::Endpoint`](./errors.md), naming the address.
A subscription that [dials](./endpoint/connect.md) waits for its peer to listen.
The ZeroMQ client retries the connection for up to 30 seconds, then the start fails with `ZmqError::Endpoint`, naming the address.
A [publisher](../publishing/index.md) attaches its socket on its first publish, the same way.

The sockets attach, and their drivers run, on the runtime the broker connected on.
So a handler that runs on threads of its own, mounted with `threads(n)`, publishes through the same sockets as every other handler.

## Example

The worker binds the address `service` takes, and opens it when the runtime starts the subscription.
A test starts the same service on an address that fails, to see the error the service would stop with.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_startup.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_startup.rs#app"
```

</TabItem>
</Tabs>
