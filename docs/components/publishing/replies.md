---
sidebar_position: 1
title: Replies
description: "A queue worker replies through a queue on an endpoint of its own, since the one its subscription binds would hand the reply back to it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies

A queue worker replies through a queue on an endpoint of its own, since the one its subscription binds would hand the reply back to it.

A reply type that owns its destination names it with `#[outgoing(name = "..")]`, and the subscriber clause stays a bare `reply`.
A type that names nothing takes the name the clause gives, `#[subscriber("jobs", reply("results"))]`.
The [generated document](../asyncapi/channels.md) reports the destination either form resolves to.

A queue that a subscription of this service binds takes every message pushed into it, whatever its name frame says.
A reply published there would come back to the same handler as its next job.
So the publisher refuses it with [`ZmqError::Send`](../broker/errors.md), naming the destination, and the in-process test broker refuses it the same way.
The worker sends its replies through a second broker instead: an endpoint the sink binds, which the worker dials.
`bindable()` turns that broker into a token, `.bind(Publish)` makes the reply policy publish through it, and `register_broker` adds it to the service.

On `ZmqFanout` a reply on the fan-out the subscription reads reaches every subscription of the service whose name is a prefix of the reply's name, its own included when it matches.
On `ZmqRpc` a reply goes back to the peer that asked, as [Responders](../request-reply/responders.md) shows.

## Example

The worker binds `tcp://0.0.0.0:5555` for its jobs and dials the sink at `tcp://sink:5556` for its results.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/zmq_publishing_replies.rs#reply"
```

```rust reference="examples/zmq_publishing_replies.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/zmq_publishing_replies.rs#reply"
```

```rust reference="examples/manual/zmq_publishing_replies.rs#app"
```

</TabItem>
</Tabs>
