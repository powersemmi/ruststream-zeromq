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
//! Consuming a small JSON body over PUSH/PULL: the subscription reads the frames off the socket
//! and parses the crate's layout, the dispatcher decodes the body into a struct, the handler reads
//! a field, and the runtime acks it. The ack answers `Unsupported`: `ZeroMQ` settles nothing.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::queue(messages, |b| b.include(consume))
}

// Three runs counted 10483 blocks over 2000 deliveries and 5473 over 1000 every time: four of the
// five allocations a delivery makes are the `zeromq` client's, one is the crate's name frame. The
// floor is the highest count plus 0.1 percent, 10494, stated as 5.010 blocks per delivery and 474
// cold; one more allocation per delivery would exceed it by 2000.
#[library_benchmark(config = common::config_every(5_010, 1_000, 474))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
