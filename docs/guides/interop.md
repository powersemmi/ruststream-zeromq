---
sidebar_position: 6
description: "Join a ZeroMQ topology that Python, C or C++ peers already speak: the three frames, a PUSH, a SUB and a DEALER peer, codecs and versions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Peers in other languages

This guide connects a RustStream service to peers written in other languages: a PUSH that feeds it, a SUB that reads its announcements, and a DEALER that asks it.

The peers here use [pyzmq](https://pyzmq.readthedocs.io/), the Python binding of libzmq.
A peer in C, C++ or another language composes the same frames with its own binding.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/interop.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#service"
```

</TabItem>
</Tabs>

The service binds three endpoints.
`jobs` on port 5555 is a [`ZmqQueue`](../components/broker/zmq-queue.md) that takes the jobs a PUSH sends.
Each worked job is announced as `jobs.done` on the [`ZmqFanout`](../components/broker/zmq-fanout.md) on port 5556.
`score` on port 5557 answers requests on a [`ZmqRpc`](../components/broker/zmq-rpc.md); `ReplyToRequester` is the transform of [Request and reply](request-reply.md).

## The three frames

Every message is three frames ([Wire format](../components/wire/index.md)):

```text
frame 0: name      UTF-8; on PUB/SUB also what a subscriber's prefix is matched against
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   what the codec wrote; JSON by default
```

A peer that sends two frames, the name and the payload, sends a message with no headers.
A peer that sends one frame, as a plain ZeroMQ application does, sends that frame as the payload, with an empty name; on PUB/SUB the prefix is matched against it.
A message of more than three frames fails the delivery with `ZmqError::Wire`.
A blank line in the header frame is skipped, so a line that ends with a newline is fine.
A header frame or a name that is not UTF-8, or a header line with no `:`, fails the delivery with [`ZmqError::Wire`](../components/wire/headers.md) on the subscription.
The service checks the headers it publishes, so that each reads back unchanged: a header that is not text, holds a line break, has a `:` in its name, or starts or ends with whitespace fails the publish with `ZmqError::Send`, naming the header and the destination.

## A PUSH that feeds the service

```python title="push.py"
import json
import zmq

push = zmq.Context().socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send_multipart([
    b"jobs",
    b"content-type: application/json",
    json.dumps({"id": 1, "items": 3}).encode(),
])
push.close(linger=1000)
```

The name frame is `jobs`, but a bound PULL takes every message pushed to it, whatever its name.

## A SUB that reads the announcements

```python title="watch.py"
import json
import zmq

sub = zmq.Context().socket(zmq.SUB)
sub.connect("tcp://127.0.0.1:5556")
sub.setsockopt(zmq.SUBSCRIBE, b"jobs.")
name, headers, payload = sub.recv_multipart()
print(name.decode(), json.loads(payload))
```

The subscription is a prefix of the name frame, as it is for a RustStream watcher ([Broadcast with PUB/SUB](broadcast.md)).
A SUB receives the announcements published after it has joined the fan-out, and none from before.
Start `watch.py`, run `push.py`, and run it again a second later, so that an announcement is published after the SUB has joined.
`watch.py` prints the first announcement it receives and exits: `jobs.done {'id': 1, 'items': 3}`.

## A DEALER that asks the service

```python title="ask.py"
import json
import zmq

dealer = zmq.Context().socket(zmq.DEALER)
dealer.connect("tcp://127.0.0.1:5557")
dealer.send_multipart([
    b"score",
    b"correlation-id: 42",
    json.dumps({"customer": 3, "amount_cents": 1250}).encode(),
])
name, headers, payload = dealer.recv_multipart()
print(name, headers, json.loads(payload))
```

A requester on the other side of `ZmqRpc` is a DEALER, and it sends the three frames.
The ROUTER adds the DEALER's identity in front of them and the `reply-to` header to the request.
The answer comes back as three frames, and frame 0 is the literal `reply` ([Envelope](../components/wire/envelope.md)).
The headers carry the request's `correlation-id`, so a DEALER with several requests in flight matches each answer to its request:

```text
b'reply' b'correlation-id: 42' {'customer': 3, 'approved': True}
```

## Codecs

The payload frame is whatever the codec wrote, so a peer agrees with the service on the codec, not on Rust types.
JSON is the default; `msgpack` and `cbor` are features of `ruststream`.
Bytes the peer framed itself pass through untouched in a `#[derive(Outgoing, Serialized)]` newtype ([Wire format](../components/wire/index.md)).

## Versions and security

The client greets every peer with ZMTP 3.0.
libzmq 4 speaks ZMTP 3.1 and accepts a 3.0 peer, and the `zeromq` client is tested against libzmq.
The handshake uses the NULL mechanism, so a peer joins without credentials.
Traffic is plain: [Running in production](production.md) covers where to run it.

## The contract in the generated document

With the `asyncapi` feature, the generated document tells a peer how to join each channel ([AsyncAPI](../components/asyncapi/index.md)).
The server reports the transport, the address and the side this service takes.
A channel reports its socket pair and, on PUSH/PULL and PUB/SUB, `nameFrame`: the frame 0 a peer sends or subscribes with.
A responder's reply channel points at the `reply-to` header instead of an address.

## Testing the frames

The in-process test pushes a job as a peer would and checks the announcement published on `jobs.done`.
The second test runs the service over real sockets with `TestApp::start_live`.
A raw `zeromq` DEALER plays the foreign peer: it composes the request frames by hand and checks the three frames of the answer.
The responder binds a socket path of the test's own, so parallel runs never meet.
