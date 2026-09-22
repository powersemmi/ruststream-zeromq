set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

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

test:
    cargo test --workspace --all-features

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
