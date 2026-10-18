---
sidebar_position: 4
description: "Answer requests on DEALER/ROUTER with an ordinary reply handler, and ask another service from a handler, with a timeout and a later retry."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request and reply

This guide answers requests on DEALER/ROUTER and asks another service from inside a handler.

The example has two services.
The scoring service answers whether an order amount is approved for a customer.
The checkout service asks it about each order it takes.

## The responder

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

[`ZmqRpc`](../components/broker/zmq-rpc.md) is the DEALER/ROUTER pattern.
The responder is the stable side, so it binds, and every requester dials it.
`score` is an ordinary reply handler: it returns the answer, or drops a request without an amount.

A ROUTER socket knows which peer sent each request.
It stamps that peer on the request as a `reply-to` header, and the answer must go there.
So `Score` declares no destination of its own, and `reply("reply")` names only a fallback.
The transform below replaces it for each request.

A responder names where its retry copies go: `.out_retry(Publish).to("score.retry")`.
A copy of a request has no address of its own, so a mount without it is refused at startup.
A responder also takes no batches: `.batch(..)` on it does not compile, because each answer goes to its own peer.

## The transform

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#transform"
```

</TabItem>
</Tabs>

`ReplyToRequester` is a [publish transform](../components/request-reply/responders.md) on the reply.
It reads the request's `reply-to` header and sends the answer there.
It also copies the request's `correlation-id`, which the requester matches the answer by.
It declares `Destination = Names`, because it chooses the destination.
A transform may do that only where the reply type declares none, so a named reply type and this transform do not compile together.

An answer to a peer that has gone is refused with an error naming its address.
The responder learns that its answer arrived nowhere; on a fan-out, an unmatched message is dropped without a word.

## The requester

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply_client.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply_client.rs#requester"
```

</TabItem>
</Tabs>

`check` takes an `Out<impl RequestReply>` parameter, and the mount site binds it to a `ZmqRpc` policy with `.out(DefaultSlot, ask)`.
The `scoring` broker dials the scoring service and has no subscription; it only asks.
[`request`](../components/request-reply/index.md) sends the request over a DEALER socket of its own and returns the answer that carries the same `correlation-id`.
`check` encodes the request and decodes the answer itself, with the JSON the scoring service reads and writes.

## Timeouts

A request that gets no answer in `ASK_TIMEOUT` fails with [`ZmqError::RequestTimeout`](../components/broker/errors.md).
`check` then returns `retry_after(ASK_AGAIN)`, and the order comes back ten seconds later.
`max_attempts` caps the tries at three.
This is the Lazy Pirate pattern of the [ZeroMQ guide](https://zguide.zeromq.org/docs/chapter4/#Client-Side-Reliability-Lazy-Pirate-Pattern), with the framework keeping the count.
A request that timed out may still have reached the responder, so a request must be safe to send twice.

Neither `request` nor a reply publish is safe to cancel.
A handler gives up through the request's timeout, not through a `select!` arm ([Cancellation](../components/publishing/cancellation.md)).

## Testing

The scoring service's tests send requests as a requester's DEALER would and check how `score` settled them.
The checkout test runs with no scoring service in process, so its request times out.
It checks that `check` asked for a retry, and that the copy came back after `ASK_AGAIN` with a retry count of one.
The clock is paused, so the timeout and the delay pass at once.
[Testing a ZeroMQ service](testing.md) covers the harness.
