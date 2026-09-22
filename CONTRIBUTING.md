# Contributing to ruststream-zeromq

`ruststream-zeromq` is the ZeroMQ transport of RustStream. The framework's core crate lives in
[`powersemmi/ruststream`](https://github.com/powersemmi/ruststream), and each of the other broker
crates in a repository of its own. This page covers the environment, the checks a change passes
before review, and how a change is tested against a local checkout of the core.

## Repositories

This crate depends on the core through crates.io and builds on its own. To work on both, clone
them side by side in one directory:

```text
RustStream/
  ruststream/
  ruststream-zeromq/
  ...           the other broker crates, when a change reaches them too
```

```bash
mkdir RustStream && cd RustStream
git clone https://github.com/powersemmi/ruststream.git
git clone https://github.com/powersemmi/ruststream-zeromq.git
```

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.88, the `rust-version` in `Cargo.toml`:
  `rustup toolchain install 1.88` builds against it with
  `cargo +1.88 check --workspace --all-features`.
- **just**, which runs every recipe below.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| `just bench` | Python 3 | the system package manager |
| the documentation site | Python 3.12 | `pip install -r docs/requirements.txt`, then `properdocs serve` |

## Checking a change

```bash
just check   # rustfmt, clippy, cargo check with all features and with none
just test    # the test suite
just ci      # both, plus codespell, cargo deny and zizmor
```

The whole suite, conformance and lifecycle included, runs on loopback sockets: ZeroMQ has no
server, so `just test` needs no stand.

`just bench` measures what this crate and the framework's runtime cost over the raw `zeromq`
sockets and rewrites `docs/benchmarks/results.json`. It takes minutes and wants the machine to
itself.

## Testing against a local core

A change in the core is tested here before it is released, with the `ruststream` dependency
patched to the checkout next to this one:

```bash
cargo test --workspace --all-features --config "patch.crates-io.ruststream.path='../ruststream'"
```

From the core, `just brokers zeromq` runs the same command against the core's working tree.

## Pull requests

- One logical change per pull request.
- Open it as a draft. CI runs when it is marked ready for review.
- A pull request merges as one squashed commit, after the `CI result` check and one approving
  review. Commits are signed.
- Documentation changes with the code. An item's rustdoc says what it is and does, and the crate's
  module overview is its guide. The site holds the entry pages in English, Russian and Chinese,
  and an edit reaches all three.
