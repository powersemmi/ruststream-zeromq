---
sidebar_position: 8
description: "Run a ZeroMQ service in production: endpoints from the environment, startup order, read-ahead and back-pressure, security and shutdown."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

ZeroMQ has no server between services, so the deployment decides what a broker would otherwise decide: who listens, in which order services start, and how far a slow consumer buffers.

## Endpoints from the environment

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#settings"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#settings"
```

</TabItem>
</Tabs>

The endpoints and the read-ahead bound are the service's whole ZeroMQ configuration.
`Settings` reads them from the environment the deployment sets, with a default for each.
`from_vars` takes the lookup as a function, so a test passes its own values without touching the process environment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

The worker binds `JOBS_BIND` and dials the sink at `SINK_ADDR`.
`tcp://0.0.0.0:5555` listens on every interface; `tcp://10.0.0.5:5555` listens on one.
`ipc:///run/jobs.sock` is a Unix socket for services on one host.
The side is part of the broker's type, so the deployment chooses the address and the code chooses the side.

## Startup order

A subscription that binds opens its port at startup, and a port another process holds fails the start with [`ZmqError::Endpoint`](../components/broker/startup.md).
A subscription that dials waits for its peer to listen.
The client retries the connection for up to 30 seconds, then the start fails with `ZmqError::Endpoint`, naming the address.
So the services may start in any order, as long as each peer is up within that window.

A publisher attaches on its first publish.
A send with no peer attached retries for five seconds, then fails with [`ZmqError::Send`](../components/broker/errors.md), naming the destination.

## Read-ahead and back-pressure

A subscription reads at most [`read_ahead`](../components/subscriptions/read-ahead.md) deliveries ahead of its handler, 1000 by default.
Past that bound it stops reading, the socket buffers fill, and the sender waits.
So a slow handler slows the sender down instead of growing the memory of the service.
On PUB/SUB the waiting sender is the publisher, held back by its slowest matching subscriber.
The bound is set per broker, here from `ZMQ_READ_AHEAD`.

## Security

The client negotiates the NULL mechanism: peers connect without credentials, and traffic travels as plain bytes.
A service therefore runs inside a network it trusts, or inside a tunnel: a VPN, WireGuard, or an SSH tunnel.
The generated AsyncAPI document reports each endpoint's host and port without credentials ([Server](../components/asyncapi/server.md)).

## Shutdown

On shutdown the service stops its subscriptions and closes its sockets.
A publisher handed out earlier then fails with [`ZmqError::NotConnected`](../components/broker/shutdown.md) instead of sending into nothing.
A queue publish is safe to cancel.
A fan-out publish, a reply and a request are not, so give up through a timeout, not a `select!` arm ([Cancellation](../components/publishing/cancellation.md)).

## Testing the configuration

The tests build `Settings` with `from_vars`.
One checks the defaults of an empty environment.
The other starts the configured service under `TestApp::start` and checks that a result reaches the sink.
The in-process mode opens no socket, so the addresses need not exist.
