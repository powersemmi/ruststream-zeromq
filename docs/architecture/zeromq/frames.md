---
title: Frames
sidebar_position: 1
description: "A ZMTP connection opens with a greeting and a READY command, then carries messages of frames; this crate's message is three frames: name, headers, payload."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A ZMTP connection opening, then one message of this crate as its three frames",
  "text": "The diagram as text",
  "connection": "ZMTP connection, both peers",
  "greeting": ["Greeting", "signature, version 3.0, NULL mechanism"],
  "ready": ["READY command", "Socket-Type, and Identity when the socket has one"],
  "messages": ["Messages", "frames, each but the last flagged MORE"],
  "message": "One message of this crate",
  "name": ["Frame 0", "name"],
  "headers": ["Frame 1", "headers"],
  "payload": ["Frame 2", "payload"],
  "rules": ["The name is UTF-8 text, the headers are name: value lines,", "and the payload is the bytes the codec wrote"],
  "bothWays": "both ways",
  "oneMessage": "one message"
};

export const envelope = {
  "title": "A request at the responder's ROUTER and the reply to it: the identity frame becomes reply-to, and the reply travels behind it",
  "text": "The diagram as text",
  "request": "A request, as the ROUTER reads it",
  "requestIdentity": ["Identity", "of the peer"],
  "requestName": ["Name"],
  "requestHeaders": ["Headers"],
  "requestPayload": ["Payload"],
  "stamp": ["The handler sees the header", "reply-to: zmq-reply:<identity in hex>"],
  "reply": "The reply, as the ROUTER sends it",
  "replyIdentity": ["Identity", "from reply-to"],
  "replyName": ["reply"],
  "replyHeaders": ["Headers"],
  "replyPayload": ["Payload"],
  "dealer": ["The ROUTER takes the identity off, and the", "requester's DEALER receives the other three frames"],
  "becomes": "becomes",
  "addresses": "addresses the reply"
};

# Frames

A ZMTP connection opens with a greeting and a READY command, then carries messages of frames;
this crate's message is three frames: name, headers, payload.

<Diagram name="frames" words={words} />

ZMTP is the protocol under every ZeroMQ socket. Both peers send a greeting with the protocol
version and the security mechanism. The `zeromq` client greets with version 3.0 and the NULL
mechanism, which neither authenticates nor encrypts. Then each peer sends a READY command with
its socket type, and a pair of types that do not belong together ends the connection there.

A message is a list of frames. Every frame but the last carries the MORE flag, so the receiver
knows where one message ends. A socket hands over a whole message or nothing.

This crate puts three frames in every message. Frame 0 is the name, frame 1 the
[headers](../../components/wire/headers.md), frame 2 the payload. A peer in any language
composes one with a single multipart send, as the [wire format](../../components/wire/index.md)
shows. A message of two frames is read as a name and a payload with no headers.

The payload frame is the buffer the codec wrote, not a copy of it. Each publisher of this crate
takes the bytes the runtime encoded, and a frame owns its bytes, so the buffer becomes the frame.

## Requests and replies

<Diagram name="envelope" words={envelope} />

The responder's ROUTER reads a request with the requester's identity in front of the three
frames. The crate turns that identity into the `reply-to` header, `zmq-reply:` followed by the
identity in hexadecimal, and the handler's reply is addressed there. A
[responder](../../components/request-reply/responders.md) mounts a transform that names that
destination per delivery.

On the way back the frames are the identity, the literal name `reply`, the headers and the
payload. The ROUTER sends them to the connection the identity names and takes the identity off.

A request goes out on a DEALER of its own, with a random identity of its own, so an answer can
reach only the request that asked. The request carries a `correlation-id` header, kept when the
caller set one, and the reply that carries the same id resolves it. The
[envelope](../../components/wire/envelope.md) page lists the headers.

The protocol is [ZMTP 3.0](https://rfc.zeromq.org/spec/23/), and the identity envelope is the
[request-reply pattern](https://rfc.zeromq.org/spec/28/)'s.
