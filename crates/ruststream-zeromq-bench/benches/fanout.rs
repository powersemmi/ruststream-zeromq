// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Publishing a small JSON body to a SUB peer over PUB: the publisher frames it behind the crate's
//! layout and hands it to the socket, and the region ends when the peer has read every message.
//! The publisher holds itself back from a subscriber that does not keep up, so the peer's reading
//! is inside the region.

mod common;

use common::{FanningOut, MESSAGES};
use gungraun::{library_benchmark, library_benchmark_group, main};

// Three runs counted 9002 blocks over 1000 publishes and 18002 over 2000: nine allocations a
// publish, and two once, one of them for the window's first mark. Three are the publish path's
// own (the payload buffer, the frames and the name frame) and six the client's: its boxed send,
// the copy its fan-out task makes for the subscriber and queues, and the first sharing of two
// frames with that copy. A window allocates nothing, so one allocation per window would add 3 over
// 1000 publishes and 7 over 2000. The floor is stated as 9.000 blocks per publish with 3 cold.
#[library_benchmark(config = common::config_every(9_000, 1_000, 3))]
#[bench::first(common::fanning_out(1))]
#[bench::base(common::fanning_out(MESSAGES))]
#[bench::twice(common::fanning_out(2 * MESSAGES))]
fn publish(run: FanningOut) {
    common::fan_out_and_drain(run);
}

library_benchmark_group!(name = fanout_group; benchmarks = publish);
main!(library_benchmark_groups = fanout_group);
