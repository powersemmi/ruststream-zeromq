---
title: Methodology
description: "How ruststream-zeromq measures itself: the three loops, a peer in the same process instead of a broker, the transport-bound arithmetic and the code count."
---

# Methodology

How `ruststream-zeromq` measures itself: the three loops, a peer in the same process instead of a
broker, the transport-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on ZeroMQ.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `zeromq` sockets, driven directly: receive, decode.
- **Broker crate.** This crate and nothing above it: the broker connects, a subscription opens,
  and a loop pulls messages off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the socket pattern, the socket options, the three
frames on the wire, the decode into the same struct, the payload bytes, the tokio runtime and the
binary.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## There is no broker in this row

Every other broker crate of the framework measures itself against a server: a container starts,
the client connects to it, and the row is what the crate adds on top of that client. ZeroMQ has no
server. The other end of every loop is a PUSH socket in the same process, with the default socket
options, and there is nothing to start or stop.

A transport with no server in the path is cheap per message. This crate's own work is then a
larger share of the total than it is anywhere else, so a percentage here reads larger than the same
cost would read against a broker.

## The run

- **A fresh address per run**, a new port on the loopback or a new `ipc://` path, so a run never
  meets a connection an earlier one left.
- **The window opens at the first message and closes when the last one is decoded**, in all three
  loops. Binding and the handshake are outside it.
- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `transport-bound` when a message spent most of its time waiting on the transport, so the
work above it happened inside a wait already paid for. That is arithmetic: the round trip of one
exchange on the same transport, times the round trips a message costs, against the measured time
per message. The round trip is measured with a REQ/REP pair outside every loop and published with
the machine. A PUSH/PULL consumer sends nothing back, so a message costs zero round trips, and no
row is transport-bound.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on the
production broker bound to a TCP port on the loopback, built without the `testing` feature. The
peer is a raw `zeromq` socket on a thread of its own that writes this crate's three frames.

The peer writes every message before the count starts, while the service is not reading, and the
messages wait in the kernel's buffers. PUSH/PULL keeps them for a consumer that is not reading, so
the consume and batch scenarios use it. The reply scenario is DEALER/ROUTER, where an answer goes
back to the peer that asked, and its count runs until the peer has read every answer.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one message, then
1000, then 2000. The difference between the last two is the steady cost of a message, and the
single message is the cold start.

Across three runs of one binary, consume gave the same instruction count, and the other two
agreed within a third of a percent: how the peer reads answers and when a batch closes depend on
timing. The allocation counts agreed within one block. A run against a saved baseline fails on
two percent more instructions in a scenario. Allocations are held to a floor each scenario
declares, on every run.

## Running it yourself

```bash
just bench
```

The recipe runs both comparison scenarios and rewrites `docs/benchmarks/results.json`. It starts
no stand, since the peer is a socket in the same process. It takes minutes and wants the machine
to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on. Both
recipes build the benchmarks the way a release is built, with fat link-time optimization across
crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes seconds and needs valgrind. The recipe installs the benchmark runner itself,
at the release `Cargo.lock` pins. `just bench-code --save-baseline=main` on `main` records a
baseline, and `just bench-code --baseline=main` on a branch compares with it. `just bench-code
2000` counts every scenario over two thousand messages instead of a thousand.
