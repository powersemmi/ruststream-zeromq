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
//! Consuming in batches of 64 over PUSH/PULL: the subscription assembles the batch on the client,
//! in the framework's buffer with this crate's deadline, hands the handler a slice, and the runtime
//! settles every delivery in it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;

#[subscriber("orders")]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::queue(messages, |b| b.include(consume.batch(nonzero!(64))))
}

// Three runs counted 10488 blocks over 2000 deliveries and 5478 over 1000 every time: five
// allocations a delivery, and a few a batch. The floor is the highest count plus 0.1 percent,
// 10499, stated as 5.010 blocks per delivery and 479 cold; one more allocation per delivery would
// exceed it by 2000.
#[library_benchmark(config = common::config_every(5_010, 1_000, 479))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
