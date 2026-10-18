---
sidebar_position: 1
title: Server
description: "The server reports the coordinate without scheme or credentials, the transport, the side this service takes and ZMTP 3.0."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the coordinate without scheme or credentials, the transport, the side this service takes and ZMTP `3.0`.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the endpoint's address without the scheme, such as `0.0.0.0:5557`; an `ipc://` endpoint reports its socket path whole.
User information in the address stays out of it, because the document is published and shared.
The `protocol` is `zeromq`, and `protocolVersion` is `3.0`, the ZMTP version the client greets every peer with.

The `x-ruststream-zeromq` extension of the server holds three fields.
`transport` is `tcp` or `ipc`.
`endpoint` is the same coordinate as `host`.
`role` is `bind` when this service listens on the endpoint and `connect` when it dials it, so a peer reads which side it takes.

## Example

The worker dials the dispatcher at `worker:5555`, and the greeter listens on every interface at port 5557.
The document reports the first as `role: connect` and the second as `role: bind`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_asyncapi.rs#server"
```

</TabItem>
</Tabs>
