---
sidebar_position: 2
title: Channels
description: "A publish channel reports its socket pair and, on PUSH/PULL and PUB/SUB, nameFrame: the frame 0 a peer sends or subscribes with."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A publish channel reports its socket pair and, on PUSH/PULL and PUB/SUB, `nameFrame`: the frame 0 a peer sends or subscribes with.

Each destination a mount site publishes to is a channel of the document.
Its `x-ruststream-zeromq` extension comes from the publish policy, so it says how messages leave this service.
`socketPair` is `PUSH/PULL`, `PUB/SUB` or `DEALER/ROUTER`.
`nameFrame` is the value frame 0 holds on the [wire](../wire/index.md), which is the destination itself.
A peer reads from it what to send to reach the channel and, on PUB/SUB, which prefix to subscribe with.

A responder's reply channel reports no `nameFrame`.
An answer travels to the identity the ROUTER supplies, and its frame 0 always holds `reply`, so the channel's own name reaches no peer.

## Example

The worker's results go to `results` over PUSH/PULL, so the channel reports `nameFrame: results`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
