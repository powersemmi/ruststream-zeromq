# Benchmarks

This crate sits between the `zeromq` client and your service: a subscription that yields messages,
a documented frame layout, a publisher that writes it. The framework's runtime sits above that
again. This page says what each of the two costs, measured against the same work written by hand on
the client.

Every scenario runs three times over in one process, as three loops that differ in what carries the
messages.

- **Raw client** - the `zeromq` sockets, driven directly: bind, receive, decode.
- **ruststream-zeromq** - this crate's subscription, the message it yields, its `ack` and its
  publisher, driven by a loop in the benchmark: no handler, no app, no dispatch.
- **RustStream service** - the whole service a user writes, `#[subscriber]` and the app, over the
  same crate.

Crate overhead is the second column against the first: what this crate costs over the sockets it
wraps, and the figure this repository answers for. Total overhead is the third against the first,
so the difference between the two is the runtime. The runtime belongs to the core crate, which
measures it there in instructions and allocations per message; what these columns add is what it
costs over this transport in particular.

Everything else is held equal - the socket pattern, the socket options, the three frames on the
wire, the decode into the same type, the payload bytes, the tokio runtime and the build. The
procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## There is no broker in this row

Every other crate in the framework measures itself against a server: a container starts, the client
connects to it, and the row is what the crate adds on top of that client. ZeroMQ has no server. The
peers talk to each other, so the other side of every loop is a socket in the same process, and
there is nothing to start or stop.

So this row answers a different question rather than answering the old one faster or slower. A
transport with no server in the path is cheap per message, which leaves this crate's own work a
larger share of the total than it is anywhere else. Lining these percentages up against another
crate's is only meaningful with that in mind.

## The numbers

The best of three interleaved rounds, with the median round in parentheses. Higher is better in the
rate columns.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "ruststream-zeromq", "framework": "RustStream service", "adapterOverhead": "Crate overhead", "overhead": "Total overhead", "indistinguishable": "indistinguishable", "brokerBound": "transport-bound", "machine": "Machine", "os": "OS", "broker": "Transport", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

Both scenarios are the PUSH/PULL pattern over the two transports this crate admits: `tcp://` on the
loopback and `ipc://`. They differ in the kernel path a message takes and in nothing else, so the
pair of them says how much of the cost is the socket.

An overhead published as `indistinguishable` is one smaller than the run-to-run spread of the two
columns it compares. A figure below that spread would read as precision nobody measured, so none is
published.

No row here carries the `transport-bound` mark, and that is a property of the pattern rather than
of the machine. The mark says a delivery spent most of its time waiting on the transport, so the
work above it happened inside a wait that was already being paid. PUSH/PULL gives a delivery
nothing to wait for: the consumer reads frames off the stream and sends nothing back - no poll, no
fetch, no acknowledgement - so a delivery costs zero round trips and the mark cannot apply.

The round trip published with the machine below is what one exchange between two peers costs on the
same transport, measured with a REQ/REP pair outside every loop. Multiply it by the zero above and
compare it with the time per message: that is the arithmetic the mark comes from, and you can check
it.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-zeromq/latest/benchmarks/results.json).

## The crate's own code

<div id="benchmark-code"></div>

The second table is this crate's own cost per message, counted rather than timed: instructions
under callgrind and allocations under DHAT. Each scenario is the service a user writes, on the
production broker, bound to a TCP port on the loopback. The other end is a peer on a thread of its
own: a raw `zeromq` socket that writes the crate's three-frame layout.

What is counted is everything the service's thread does: the framework's dispatch and codec, this
crate's subscription, frame layout and publisher, and the `zeromq` client's framing and socket
handling on that thread. The peer's thread is not counted, and neither is the kernel.

The peer writes every message before the count starts, while the service is not reading, and the
messages wait in the kernel's socket buffers. PUSH/PULL keeps them for a consumer that is not
reading, so the two consume rows use it. A PUSH/PULL endpoint carries one stream of work, so the
reply row is DEALER/ROUTER, where an answer goes back to the requesting peer over the connection its
request came in on. The count on that row runs until the peer has read every answer.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what starting the service, accepting the peer's
connection and taking the first delivery cost once. The numbers are absolute, the framework's own
cost included; the core publishes that cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

Three runs of one binary gave the same instruction count on the consume row, and totals within a
third of a percent on the other two, where the peer reading answers and the batch deadline depend
on timing. The allocation counts agreed within one block. `just bench-code` fails on an allocation
above the floor a scenario declares, the highest count of those runs plus 0.1 percent, and with
`--baseline=main` on more than two percent more instructions. A pull request that changes the cost
cites its numbers.

## The machine

<div id="benchmark-environment"></div>

The `zeromq` crate exposes no high-water mark and no buffer setting - a socket carries a peer
identity and a connect timeout and nothing else - so every loop opens its sockets with the defaults
and what bounds the queue is the transport's own buffer. Nothing else here is tunable, so there is
no socket configuration to publish beyond the transport itself.

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one queue, a small body and a peer in the same process. It measures what a
delivery costs in this crate, not what ZeroMQ can carry, and a row here is not comparable with a
row published for another broker: the transports do different work per message.

Every loop puts the same three frames on the wire - name, headers, payload - because that is the
layout this crate documents and a foreign peer writes by hand. This crate reads all three, the
hand-written loop takes the payload out of the third. So the figure is the cost of this crate's
subscription, frame layout and publisher, not the cost of putting the extra frames there.

The header frame goes out empty. A service that sends headers pays this crate's header parse on top
of the figure here, on every delivery.

Nothing settles a delivery on this transport. The two loops that go through this crate still call
`ack`, because that is where a settlement would go and the answer - unsupported - is one a caller
waits for; the raw loop has nothing to call. A row published for a broker that does settle carries
an acknowledgement inside the window, and this one carries none.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

There is no stand to start. The recipe runs both scenarios and rewrites
`docs/benchmarks/results.json` with what it measured. It takes minutes and wants the machine to
itself. The message count is not fixed: a probe run sets it so that every measured run lasts at
least five seconds on whatever machine it is taken on.

```bash
just bench-code
```

The recipe counts the code table under valgrind and rewrites the `code` section of the same
document. There is no stand to start here either: the service binds the loopback, and the peer is
a socket in the same process. It takes seconds and needs valgrind and the benchmark runner:
`cargo install --locked gungraun-runner --version =0.19.4`.
