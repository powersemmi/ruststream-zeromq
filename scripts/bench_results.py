#!/usr/bin/env python3
"""Turn a benchmark run into the published results document.

`benches/paired.rs` reports the scenarios it measured and nothing else, because the machine and
the build are not its to describe. This script reads that summary, adds the environment the run
was taken in and the versions it was taken against, and writes the document the documentation
site serves at `benchmarks/results.json`.

The schema is the core's, declared at
https://powersemmi.github.io/ruststream/latest/benchmarks/#publishing-results. This crate
publishes the throughput table alone, each loop as its best and worst round, so the document
declares schema 3.

The schema's `broker` field names what carried the messages. ZeroMQ has no broker, so it names
the socket pair and its options instead: they, not a server's configuration, are what decides
the result. `round_trip` is the benchmark's own measurement, carried through from the summary:
it is what the `broker_bound` mark on a row is decided against, published so a reader can redo
the arithmetic.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
"""

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "Cargo.toml"
LOCK = REPO / "Cargo.lock"

# What `just bench` builds the benchmark with. All of these are recipe decisions rather than
# machine facts, so they are stated here next to the recipe rather than sniffed.
PROFILE = "bench, inheriting release (opt-level = 3, lto = false, codegen-units = 16)"
FEATURES = "ruststream-zeromq default (none), ruststream macros,json"
RUSTFLAGS = "none (the recipe clears RUSTFLAGS, so the numbers are not tied to this CPU)"
# The `zeromq` crate has no high-water mark and no send or receive buffer setting: `SocketOptions`
# carries a peer identity and a connect timeout and nothing else. Both halves therefore open their
# sockets with the defaults, and what bounds the queue is the transport's own buffer.
TRANSPORT = (
    "none: ZeroMQ has no broker, so the peer is a PUSH socket in the same process, "
    "over tcp:// on the loopback and over ipc://, with default socket options "
    "(the zeromq crate exposes no high-water mark)"
)


def run(*args: str) -> str:
    try:
        return subprocess.run(args, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return ""


def proc_field(path: str, key: str) -> str:
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        name, _, value = line.partition(":")
        if name.strip() == key:
            return value.strip()
    return ""


def lscpu() -> dict[str, str]:
    fields = {}
    for line in run("lscpu").splitlines():
        name, _, value = line.partition(":")
        fields[name.strip()] = value.strip()
    return fields


def cores(cpu: dict[str, str]) -> str:
    physical = cpu.get("Core(s) per socket", "")
    sockets = cpu.get("Socket(s)", "1")
    logical = cpu.get("CPU(s)", "")
    if not physical or not logical:
        return "unknown"
    return f"{int(physical) * int(sockets)} physical, {logical} logical"


def frequency(cpu: dict[str, str]) -> str:
    low, high = cpu.get("CPU min MHz", ""), cpu.get("CPU max MHz", "")
    if not low or not high:
        return "unknown"
    return f"{float(low.replace(',', '.')):.0f}-{float(high.replace(',', '.')):.0f} MHz"


def memory() -> str:
    total = proc_field("/proc/meminfo", "MemTotal")
    if not total.endswith(" kB"):
        return "unknown"
    return f"{int(total[:-3]) / (1024 * 1024):.1f} GiB"


def crate_version() -> str:
    match = re.search(r'^version = "([^"]+)"', MANIFEST.read_text(encoding="utf-8"), re.M)
    return match.group(1) if match else "unknown"


def locked_version(name: str) -> str:
    match = re.search(
        rf'^name = "{re.escape(name)}"\nversion = "([^"]+)"',
        LOCK.read_text(encoding="utf-8"),
        re.M,
    )
    return match.group(1) if match else "unknown"


def environment(round_trip: str) -> dict[str, str]:
    cpu = lscpu()
    return {
        "cpu": proc_field("/proc/cpuinfo", "model name") or cpu.get("Model name", "unknown"),
        "architecture": cpu.get("Architecture", "unknown"),
        "cpu_frequency": frequency(cpu),
        "cores": cores(cpu),
        "memory": memory(),
        "memory_speed": "unknown",
        "os": f"Linux {run('uname', '-r').strip()}",
        "broker": TRANSPORT,
        "round_trip": round_trip,
        "rustc": run("rustc", "--version").replace("rustc", "").strip().split()[0],
        "profile": PROFILE,
        "features": FEATURES,
        "rustflags": RUSTFLAGS,
    }


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    document = {
        "schema": 3,
        "crate": "ruststream-zeromq",
        "crate_version": crate_version(),
        "core_version": locked_version("ruststream"),
        "measured_at": date.today().isoformat(),
        "environment": environment(summary.get("round_trip", "unknown")),
        "scenarios": summary["scenarios"],
    }
    out = Path(sys.argv[2])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
