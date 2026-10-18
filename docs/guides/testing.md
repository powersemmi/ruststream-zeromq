---
sidebar_position: 7
description: "Test a ZeroMQ service with TestApp: the production app in process, delays on a paused clock, and the same test over loopback sockets."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a ZeroMQ service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), in process or over real sockets on the loopback.

The service under test is a worker.
It takes jobs on a queue it binds and announces each finished job as `jobs.done` on a fan-out it binds.
A job whose input has not been uploaded yet is held: the handler asks for it again in a minute with [`retry_after`](../components/settlement/retries.md).
After three deliveries a held job is dropped.
The app is built by `service(jobs, events)`, so a test chooses the addresses and `main` uses the production ones.
The tests need the `testing` feature of `ruststream-zeromq` in `[dev-dependencies]`.

## A job and its announcement

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](../components/testing/index.md) connects each broker of the app in process.
No socket is opened, and the addresses are never bound.
The test addresses a broker by its production type: `tb.broker::<ZmqQueue>()` is the queue this service binds, and `tb.broker::<ZmqFanout>()` the fan-out.
A broker this service dials is `tb.broker::<ZmqQueue<Connect>>()`, and two brokers of one type are told apart by the labels of `with_broker_labeled`, through `tb.broker_named(..)`.

The test's job arrives the way a peer in another process pushes it.
`publish` returns once the handler and its announcement have settled.
So the next lines assert on finished state: how the subscriber settled, and what was published on `jobs.done`.
The body takes the started app as an argument, so the test over sockets below runs it too.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
ZeroMQ stores nothing, so `retry_after` publishes a copy of the job to its queue when the delay ends.
The copy carries the retry count in a header, and `published` shows it on `jobs` next to the test's own job.
The third delivery is the last one the mount site allows, so nothing is published after it.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process mode models

The in-process mode carries the frames a socket carries, and routes them by the pattern's rule:

- A queue that binds takes whatever a peer pushes to it; subscriptions that dial take a peer's messages in turn.
- A fan-out delivers to every subscription whose name is a prefix of the message's name.
- A request reaches the responder, and its answer reaches the peer that asked.
- A publish to an endpoint the service dials, or binds with no subscription, leaves for another process: the test reads it with `published`.

The socket's refusals hold in process too, in the socket's words.
A publish a bound queue would hand back to its own subscription is refused, and so is a second subscription on a bound endpoint.
`ack` and `nack` report `AckError::Unsupported`, as on a socket, so a handler that settles by a plain retry fails its test.

Delivery channels have no bound in process.
Back-pressure, the [`read_ahead`](../components/subscriptions/read-ahead.md) bound, late fan-out subscribers, the five-second wait for a peer and reconnection exist only on real sockets.

## The same test over sockets

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) starts the same app over real ZMTP sockets.
ZeroMQ needs no server, so the test binds loopback ports and runs in every `cargo test`.
Port zero lets the system pick a free port, so tests that run in parallel never collide.
The test's job leaves through the broker's own publisher and crosses a real socket.
The assertions are the in-process ones, unchanged.

```bash
cargo test
```
