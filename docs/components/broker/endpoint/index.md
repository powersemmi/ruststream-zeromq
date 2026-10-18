---
title: ZmqEndpoint
description: "ZmqEndpoint is an address on tcp:// or ipc:// plus the side this process takes; another scheme is refused at connect."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ZmqEndpoint

[`ZmqEndpoint`](api:ruststream_zeromq::ZmqEndpoint) is an address on `tcp://` or `ipc://` plus the side this process takes; another scheme is refused at connect.

Two constructors fix the side.
[`ZmqEndpoint::bind`](./bind.md) makes this process listen, and [`ZmqEndpoint::connect`](./connect.md) makes it dial.
The side is part of the type, and the broker built on the endpoint carries it, as [`EndpointRole`](./endpoint-role.md) describes.

`tcp://host:port` reaches another host.
`ipc:///path` is a socket file on the same host, with no network stack in between.
When the broker connects, an address with any other scheme fails startup with [`ZmqError::Invalid`](../errors.md), naming the address, before any socket opens.
[`address`](api:ruststream_zeromq::ZmqEndpoint::address) returns the address as written.

The endpoint and its side are the whole configuration of a broker.
The ZeroMQ implementation carries no CURVE or ZAP layer, so traffic is neither authenticated nor encrypted: a service runs on a trusted network or inside a tunnel.

## Example

A worker listens on a socket file for the producers on its host, and pushes its results to a collector it dials over the network.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_endpoint.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_endpoint.rs#app"
```

</TabItem>
</Tabs>
