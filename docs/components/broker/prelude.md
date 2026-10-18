---
sidebar_position: 7
title: Prelude
description: "Each pattern's prelude offers its policy as Publish; ruststream_zeromq::prelude mounts several patterns under prefixed names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

Each pattern's prelude offers its policy as `Publish`; [`ruststream_zeromq::prelude`](api:ruststream_zeromq::prelude) mounts several patterns under prefixed names.

A routes file on one pattern imports that pattern's prelude: [`queue::prelude`](api:ruststream_zeromq::queue::prelude), [`fanout::prelude`](api:ruststream_zeromq::fanout::prelude) or [`rpc::prelude`](api:ruststream_zeromq::rpc::prelude).
Each carries the core's prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
Each adds [`ZmqEndpoint`](./endpoint/index.md), the pattern's broker and its publish policy under the name every broker crate uses, `Publish`.
`rpc::prelude` also brings `RequestReply`, the capability only [`ZmqRpc`](./zmq-rpc.md) implements.
So moving a service to another pattern changes the import line, and the mount site stays as it is.

A file that mounts more than one pattern imports `ruststream_zeromq::prelude`.
One glob cannot carry three policies under one name, so this prelude names them `ZmqQueuePublish`, `ZmqFanoutPublish` and `ZmqRpcPublish`.
It also carries the three brokers, `RequestReply` and the modules `queue`, `fanout` and `rpc`.

A handler body bounds its injected publisher with a core capability, such as `Out<impl Publisher, Slot>`, and imports `ruststream::prelude::*` alone.
A ZeroMQ send takes the frames alone, so a handler body stays on the core's prelude, whichever pattern it runs on.

[Broadcast with PUB/SUB](../../guides/broadcast.md) mounts a queue and a fan-out in one file with the crate's prelude.

## Example

A worker takes jobs from a queue and announces each finished job on a fan-out, so the routes file imports the crate's prelude.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
