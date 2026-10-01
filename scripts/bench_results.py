#!/usr/bin/env python3
"""Turn a benchmark run into the published results document.

`benches/paired.rs` reports the scenarios it measured and nothing else, because the machine and
the build are not its to describe. This script reads that summary, adds the environment the run
was taken in and the versions it was taken against, and writes the document the documentation
site serves at `benchmarks/results.json`.

The schema is the core's, declared at
https://powersemmi.github.io/ruststream/latest/benchmarks/#publishing-results: schema 3, each
loop of the comparison as its best, median and worst round, and the `code` section.

The schema's `broker` field names what carried the messages. ZeroMQ has no broker, so it names
the socket pair and its options instead: they, not a server's configuration, are what decides
the result. `round_trip` is the benchmark's own measurement, carried through from the summary:
it is what the `broker_bound` mark on a row is decided against, published so a reader can redo
the arithmetic.

`--code` reads the other run instead: the summary `cargo bench -- --output-format=json` writes for
the code-cost benches under `crates/ruststream-zeromq-bench/benches`, one JSON object per
benchmark. It writes the `code` section, one entry per scenario with instructions and allocations
per message plus what starting the service cost once, by the core's method: every scenario is
measured over one delivery, over MESSAGES and over twice MESSAGES, the slope between the last two
is the steady state, and the one-delivery run is the cold start. Either run keeps the section the
other one wrote.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json
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


# Deliveries per measured run of the code-cost benches, the default of their `MESSAGES`.
CODE_MESSAGES = 1000

# An instruction count below this on a code run means the measured region stopped matching its
# frame and the run reported the process exit, not that the code got faster. The cold run handles
# one delivery, so it is held to a lower floor.
CODE_FLOOR = 100_000
CODE_COLD_FLOOR = 1_000

# The code table, in reading order: the published name, the benchmark as `file/function`, and
# whether the benchmark's hard limit holds its allocation floor.
CODE_SCENARIOS = [
    (
        "PUSH/PULL on the loopback, JSON decode into a small struct, ack each",
        "consume/service",
        True,
    ),
    ("DEALER/ROUTER on the loopback, answered to the requesting peer", "reply/service", True),
    ("PUSH/PULL on the loopback, in batches of 64 assembled on the client", "batch/service", True),
]


def code_metric(summary: dict, tool: str, name: str) -> int | None:
    """The new value of one metric, out of the nested summary the runner emits."""
    for profile in summary["profiles"]:
        metrics = profile["summaries"]["parts"][0]["metrics_summary"].get(tool)
        if not metrics or name not in metrics:
            continue
        values = metrics[name]["metrics"]
        entry = values["Both"][0] if "Both" in values else next(iter(values.values()))
        return int(entry["Int"])
    return None


def code_runs(path: Path) -> dict[str, dict]:
    """Every benchmark in the run, keyed by `file/function/id`."""
    found = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        summary = json.loads(line)
        key = f"{Path(summary['benchmark_file']).stem}/{summary['function_name']}/{summary['id']}"
        found[key] = {
            "instructions": code_metric(summary, "Callgrind", "Ir"),
            "allocations": code_metric(summary, "Dhat", "TotalBlocks"),
        }
    return found


def code_total(found: dict, key: str, floor: int) -> dict:
    """One run's totals, checked for the two ways this measurement fails silently."""
    if key not in found:
        sys.exit(f"benchmark {key} is not in the run: rename it here or in benches/")
    measured = found[key]
    if measured["instructions"] is None or measured["instructions"] < floor:
        sys.exit(
            f"benchmark {key} reports {measured['instructions']} instructions, below the floor of "
            f"{floor}: collection did not cover the measured region"
        )
    # Every run starts a service, which allocates: none at all means DHAT attributed nothing to
    # the measured frame, and a slope read off that would publish a cost of zero.
    if measured["allocations"] is None or measured["allocations"] < 1:
        sys.exit(
            f"benchmark {key} reports {measured['allocations']} allocations: DHAT did not attribute "
            "the measured region"
        )
    return measured


def per_message(figure: float) -> float:
    """Three places below one, so one allocation for the whole run does not read as zero."""
    return round(figure, 3) if abs(figure) < 1 else round(figure, 1)


def code_section(path: Path) -> list[dict]:
    found = code_runs(path)
    rows = []
    for name, key, gated in CODE_SCENARIOS:
        base = code_total(found, f"{key}/base", CODE_FLOOR)
        twice = code_total(found, f"{key}/twice", CODE_FLOOR)
        if twice["instructions"] <= base["instructions"]:
            sys.exit(f"benchmark {key} does not grow with the message count: no slope to read")
        first = code_total(found, f"{key}/first", CODE_COLD_FLOOR)
        rows.append(
            {
                "name": name,
                "messages": CODE_MESSAGES,
                "framework": {
                    metric: per_message((twice[metric] - base[metric]) / CODE_MESSAGES)
                    for metric in ("instructions", "allocations")
                },
                "cold": {metric: first[metric] for metric in ("instructions", "allocations")},
                "gated": gated,
            }
        )
    return rows


def valgrind() -> str:
    return run("valgrind", "--version").strip().removeprefix("valgrind-") or "unknown"


def main() -> int:
    args = sys.argv[1:]
    code = bool(args) and args[0] == "--code"
    if code:
        args = args[1:]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    source, out = Path(args[0]), Path(args[1])
    previous = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    if code:
        # The code costs join a paired document: the page shows them beside its scenarios and
        # renders nothing without them.
        if "scenarios" not in previous:
            sys.exit(
                f"{out} holds no paired results to add the code costs to: run `just bench` first"
            )
        document = previous
        document["schema"] = 3
        document["code"] = code_section(source)
        # The code costs carry their own provenance: the paired numbers beside them may come
        # from another run, on another version, on another day.
        document["code_measured"] = {
            "crate_version": crate_version(),
            "core_version": locked_version("ruststream"),
            "measured_at": date.today().isoformat(),
        }
        document.setdefault("environment", {})["valgrind"] = valgrind()
    else:
        summary = json.loads(source.read_text(encoding="utf-8"))
        document = {
            "schema": 3,
            "crate": "ruststream-zeromq",
            "crate_version": crate_version(),
            "core_version": locked_version("ruststream"),
            "measured_at": date.today().isoformat(),
            "environment": environment(summary.get("round_trip", "unknown")),
            "scenarios": summary["scenarios"],
        }
        if "code" in previous:
            document["code"] = previous["code"]
            if "code_measured" in previous:
                document["code_measured"] = previous["code_measured"]
            if "valgrind" in previous.get("environment", {}):
                document["environment"]["valgrind"] = previous["environment"]["valgrind"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    if code:
        for row in document["code"]:
            print(
                f"  {row['name']}: {row['framework']['instructions']} instructions, "
                f"{row['framework']['allocations']} allocations per message; cold "
                f"{row['cold']['instructions']} instructions, {row['cold']['allocations']} "
                "allocations"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
