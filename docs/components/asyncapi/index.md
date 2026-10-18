---
title: AsyncAPI
description: "With asyncapi the document says ZeroMQ: protocol zeromq, ZMTP 3.0, and the crate's facts under the x-ruststream-zeromq extension."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AsyncAPI

With the `asyncapi` feature the document says ZeroMQ: protocol `zeromq`, ZMTP `3.0`, and the crate's facts under the `x-ruststream-zeromq` extension.

The core builds the AsyncAPI document from the app, before anything connects.
The specification has no ZeroMQ binding, and its binding keys are a closed list.
So this crate reports everything it knows in one extension, `x-ruststream-zeromq`, at two levels.
A [server](./server.md) says how to attach to the endpoint: the transport, the coordinate and the side this service takes.
A [channel](./channels.md) says which socket pair its messages travel over and what their name frame holds.
A responder's reply channel has no address, and the operation points a client at the [reply address](./reply-address.md) instead.

Everything comes from the service's own declarations: the brokers, their endpoints and the destinations the mount sites resolved.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

[Peers in other languages](../../guides/interop.md) reads from the document how a peer joins each channel.

## Example

The dependency line:

```toml
ruststream-zeromq = { version = "0.7", features = ["asyncapi"] }
```

The service registers each broker under a label, the key of its server in the document.
`cargo run --example zmq_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_asyncapi.rs#app"
```

</TabItem>
</Tabs>

The server of the worker's queue and the channel its results go to:

```json reference="tests/documents/queue.json"
```
