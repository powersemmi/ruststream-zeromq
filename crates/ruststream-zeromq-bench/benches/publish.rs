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
//! Publishing a small JSON body to a PULL peer over PUSH: the publisher frames it behind the
//! crate's layout and hands it to the socket, and the region ends when the peer has read every
//! message. The publisher holds itself back from a peer that does not keep up, so the peer's
//! reading is inside the region.

mod common;

use common::{MESSAGES, Publishing};
use gungraun::{library_benchmark, library_benchmark_group, main};

// Four runs counted 5002 blocks over 1000 publishes and 10002 over 2000: five allocations a
// publish, all of them the publish path's own (the payload buffer, the frames, the name frame and
// the client's), and two once, for the window's first mark. A window allocates nothing, and it
// closes every 256 messages, so one allocation per window would add 3 over 1000 publishes and 7
// over 2000. The floor is stated as 5.000 blocks per publish with 3 cold.
#[library_benchmark(config = common::config_every(5_000, 1_000, 3))]
#[bench::first(common::publishing(1))]
#[bench::base(common::publishing(MESSAGES))]
#[bench::twice(common::publishing(2 * MESSAGES))]
fn publish(run: Publishing) {
    common::publish_and_drain(run);
}

library_benchmark_group!(name = publish_group; benchmarks = publish);
main!(library_benchmark_groups = publish_group);
