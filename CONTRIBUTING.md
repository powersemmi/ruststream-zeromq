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
  minimum supported version is 1.95, the `rust-version` in `Cargo.toml`:
  `rustup toolchain install 1.95` builds against it with
  `cargo +1.95 check --workspace --all-features`.
- **just**, which runs every recipe below.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| `just bench` | Python 3 | the system package manager |
| `just bench-code` | valgrind and Python 3 | the system package manager; the recipe installs the benchmark runner itself, at the release `Cargo.lock` pins |
| the documentation site | Node.js 22 and Python 3 | `scripts/site_rustdoc.sh` (the rustdoc the API links resolve against), then `npm ci` in `website/` and `npm start` (one language) or `npm run build` (all three, strict); `npm run test` and `npm run typecheck` check the site's own code |

## Checking a change

```bash
just check   # rustfmt, clippy, cargo check with all features and with none
just test    # the test suite
just ci      # both, plus codespell, cargo deny and zizmor
```

The whole suite, the core's contract suites included, runs inside `just test`, in process and on
loopback sockets: the two peers of a ZeroMQ exchange talk to each other directly, so no server is
started first. The loopback sockets are the live side of every contract check that compares a
live transport with the in-process one.

`just bench` measures what this crate and the framework's runtime cost over the raw `zeromq`
sockets and rewrites `docs/benchmarks/results.json`. It takes minutes and wants the machine to
itself. `just bench-code` counts what a message costs in this crate's code, in instructions and
allocations, on the production broker over the loopback, and rewrites the code table of the same
document.

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
  module overview is its guide. The site in `docs/` holds the guides, one page per component and
  the architecture with the benchmarks, in English, Russian and Chinese (`website/i18n/`), and an
  edit reaches all three. Its code blocks come from the compiled examples in
  `crates/ruststream-zeromq/examples/`. `scripts/lint_doc_fences.py`,
  `scripts/check_doc_parity.py` and `scripts/check_doc_i18n.py` check the fences and the
  translations.
