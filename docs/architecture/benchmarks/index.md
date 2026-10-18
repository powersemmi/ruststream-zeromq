---
title: Benchmarks
description: "What ruststream-zeromq costs on every message: throughput against the raw zeromq client, and instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {zeromq} from '@site/src/components/Benchmarks/zeromq';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "transport-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The transport", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Transport", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-zeromq` costs on every message: throughput against the raw `zeromq` client, and
instructions and allocations of its code.

## Against a raw client

The first chart runs each scenario three ways over real sockets. `Raw client` drives `zeromq`
sockets directly. `Broker crate` drives this crate's subscription with a loop of its own and no
runtime. `RustStream` is the service a user writes, a `#[subscriber]` handler in an app. The first
gap is what this crate costs over the client it wraps, and the second is what the runtime adds on
top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

Both scenarios are PUSH/PULL, over the two transports this crate runs on: `tcp://` on the loopback
and `ipc://`. They differ in the path a message takes through the kernel and in nothing else, so
the pair shows how much of the cost is the socket.

There is no broker in either row. ZeroMQ has no server, so the other end of every loop is a
socket in the same process, as [Methodology](methodology.md#there-is-no-broker-in-this-row)
explains.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on the
production broker, bound to a port on the loopback, with a peer on a thread of its own. The count
includes the framework's dispatch, this crate's code and the `zeromq` client's framing on that
thread. The peer's thread and the kernel are not counted. `Cold start` is what starting the
service, accepting the peer's connection and taking the first message cost once.

## Results

<Results entry={zeromq} labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={zeromq} labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
