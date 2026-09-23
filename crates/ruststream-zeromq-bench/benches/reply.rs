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
//! Answering a request over DEALER/ROUTER: the responder reads the request behind the ROUTER's
//! peer identity and stamps the reply address on it, the handler returns a value, the runtime
//! encodes it, and this crate's publisher frames the answer behind that identity and hands it to
//! the ROUTER. The region ends when the requesting peer has read every answer.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::prelude::*;
use ruststream::runtime::{ForReply, Names, Outgoing, PublishContext, PublishTransform};
use ruststream::{Outgoing, Str};
use ruststream_zeromq::ZmqRpcPublish;
use serde::Serialize;

/// An answer has no destination of its own: the ROUTER addresses it per request.
#[derive(Debug, Serialize, Outgoing)]
struct Confirmation {
    id: u64,
}

/// Addresses an answer to the peer that asked, as the crate's request-reply example does: the
/// responder stamps a `reply-to` address on every request, and the transform names it as the
/// destination.
struct ReplyToRequester;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(reply_to) = cx.headers().get_shared("reply-to")
            && let Ok(reply_to) = Str::try_from(reply_to)
        {
            out.set_name(reply_to);
        }
    }
}

// The literal destination is a placeholder: `ReplyToRequester` replaces it per delivery.
#[subscriber("orders", publish("reply"))]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    // A responder addresses no retry copies of its own, so the mount names where they go, as the
    // crate's request-reply example does; nothing here retries.
    common::rpc(messages, |b| {
        b.include(confirm)
            .out_reply(ZmqRpcPublish)
            .transform(ReplyToRequester)
            .out_retry(ZmqRpcPublish)
            .to("orders.retry");
    })
}

// Three runs counted 36694 blocks over 2000 requests every time. The floor is that count plus 0.1
// percent, 36731, stated as 18.043 blocks per request; one more allocation per request would
// exceed it by 2000.
#[library_benchmark(config = common::config_every(18_043, 1_000, 645))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
