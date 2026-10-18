---
sidebar_position: 1
title: Headers
description: "The header frame is UTF-8 name: value lines; a value that is not text is refused on publish, and a malformed frame surfaces as ZmqError::Wire."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

The header frame is UTF-8 `name: value` lines; a value that is not text is refused on publish, and a malformed frame surfaces as `ZmqError::Wire`.

Frame 1 holds one header per line, the name and the value separated by `:`, and lines separated by `\n`.
A service reads and writes headers through the core's typed contract: a serde struct whose fields are the headers, parsed by `Headers<T>` in the handler and declared on an outgoing type with `#[outgoing(headers = T)]`.

Nothing is guessed in either direction.
A header value that is not UTF-8 has no form in a text frame, so its publish fails with [`ZmqError::Send`](../broker/errors.md), naming the header and the destination.
A peer's header frame that is not UTF-8, or a line without `:`, fails the message as `ZmqError::Wire` on the subscription, and the message never reaches the handler.
Blank lines are skipped, so a peer that ends the frame with a newline interoperates.
A name and a value are trimmed of surrounding whitespace on receipt.

## Example

A Python peer writes the frame by hand:

```python
socket.send_multipart([b"jobs", b"x-tenant: acme\ncontent-type: application/json", payload])
```

The worker reads the tenant a peer names in `x-tenant` and sends it on with the result.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_wire_headers.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_wire_headers.rs#handler"
```

</TabItem>
</Tabs>
