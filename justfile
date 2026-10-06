set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

# The scenarios that count instructions and allocations, one benchmark file each. Every run holds
# them to the allocation floor each declares, and a run against a baseline to the instruction
# limit too.
code_benches := "--bench consume --bench reply --bench batch"

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows.
    cargo clippy --workspace --exclude ruststream-zeromq-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-zeromq-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-zeromq-bench --all-targets --all-features
    cargo check --workspace --no-default-features

# The compile-fail snapshots run here too: the toolchain file pins stable, the one they record.
test:
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --workspace --all-features

# What this crate costs over the zeromq client it wraps: two scenarios, each run as a RustStream
# service and as a hand-written socket loop. There is no stand to start - ZeroMQ has no broker, so
# the other peer is a second socket in the same process. On demand only: it takes minutes and it
# wants the machine to itself. The page it feeds is docs/benchmarks.md.
bench *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-zeromq-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

# What a message costs in this crate's code, counted under valgrind: instructions through
# callgrind and allocations through DHAT. Each scenario is a service on the production broker,
# bound on the loopback, fed by a raw `zeromq` peer on a thread of its own; there is no stand to
# start. The page it feeds is the code table of docs/benchmarks.md. RUSTFLAGS is cleared because
# valgrind aborts on the instructions a recent CPU advertises. Needs valgrind.
#
# The benchmarks hand the measurement to gungraun's runner, which has to be the release of the
# library the lock file pins. The recipe installs that release into `target/gungraun-runner` on
# the first run and after the library moves, and puts it first on PATH, where the benchmarks look
# the runner up. A `GUNGRAUN_RUNNER` in the environment would win over PATH when the benchmarks
# build, so the recipe clears it.
#
# A leading number is the deliveries per measured run: the default of 1000 is what the published
# document is measured at, a larger count buys a steadier number for a longer run
# (`just bench-code 2000`). The benches read it at build time, so a new count rebuilds them. The
# other arguments reach the runner: `just bench-code --save-baseline=main` records a baseline,
# `just bench-code --baseline=main` measures against it. Totals over another count are not
# comparable, so each count keeps its runs and baselines in a directory of its own,
# `target/gungraun/<count>`.
#
# A run against a baseline, named with `--baseline` or in `GUNGRAUN_BASELINE`, fails on two
# percent more instructions than the baseline in a scenario. The limit is relative, so it applies
# only there: a plain run would be held to whichever run came before it. The allocation limits
# are absolute, and every run is held to them.
#
# A benchmark that breaches a limit fails the run, and the run still goes to the end: the table
# prints, every breach under it with the value it was compared against beside the new one, and
# the recipe fails after that. A build error stops it before anything runs.
[positional-arguments]
bench-code *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target
    messages=1000
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        messages="$1"
        shift
    fi
    version="$(cargo pkgid gungraun)"
    version="${version##*@}"
    runner="$PWD/target/gungraun-runner"
    installed="$("$runner/bin/gungraun-runner" --version 2> /dev/null || true)"
    if [ "$installed" != "gungraun-runner $version" ]; then
        cargo install --locked --root "$runner" gungraun-runner --version "=$version"
    fi
    unset GUNGRAUN_RUNNER
    export PATH="$runner/bin:$PATH" RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES="$messages" \
        GUNGRAUN_HOME="$PWD/target/gungraun/$messages"
    # A baseline named on the command line or in the environment brings the instruction limit.
    baseline="${GUNGRAUN_BASELINE:-}"
    for arg in "$@"; do
        case "$arg" in --baseline | --baseline=*) baseline="$arg" ;; esac
    done
    limits=()
    if [ -n "$baseline" ]; then
        limits=(--callgrind-limits='ir=2.0%')
    fi
    cargo bench -p ruststream-zeromq-bench {{ code_benches }} --no-run
    status=0
    cargo bench -p ruststream-zeromq-bench {{ code_benches }} --no-fail-fast \
        -- --output-format=json "${limits[@]}" "$@" > target/bench-code.json || status=$?
    python3 scripts/bench_results.py --code --messages "$messages" target/bench-code.json \
        docs/benchmarks/results.json
    exit "$status"

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean

ci: check test typo security
