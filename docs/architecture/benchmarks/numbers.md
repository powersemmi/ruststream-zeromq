---
title: Numbers
description: "What the benchmark numbers of ruststream-zeromq measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-zeromq` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, over one transport. A row is not comparable with a row another broker crate
publishes: there a server sits in the path, and the transports do different work per message.

The peer is a socket in the same process. With no network and no server, the same cost per message
is a larger share of the time than it would be between two hosts. Read the percentage as an upper
bound on what a deployed service pays.

An overhead marked `indistinguishable` is smaller than the spread of the rounds of the two loops it
compares. A figure below that spread would claim a precision nobody measured, so none is printed.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the `zeromq` client's
framing on the service's thread. The same count costs more time where it misses the cache, so a
count is not a time.

Every figure is the steady state, the difference between a run of 1000 messages and one of 2000.
Starting the service, accepting the peer's connection and the first allocations behind them are the
cold start, paid once.

## What they do not mean

A row is one consumer, one queue, a small body and a peer in the same process. It measures what a
message costs inside this crate, not what ZeroMQ can carry.

Every loop puts the same three frames on the wire, because that is the layout this crate
documents and a foreign peer writes. The header frame goes out empty. A service that sends headers
pays this crate's header parse on every message, on top of the figure here.

Nothing settles a message on this transport. The two loops that go through this crate still call
`ack`, which answers `Unsupported`, and the raw loop has nothing to call. A row another broker
publishes carries a real acknowledgement inside its window, and this one carries none.

A pool of worker lanes, several peers and fan-out to many subscribers each answer a different
question, and the comparison measures none of them. The code chart counts a
[batch handler](../../components/subscriptions/batches.md) and a reply on DEALER/ROUTER as well.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They
are measured again by hand, on a machine given to the run alone: the differences they show are
smaller than the noise of a shared one.
