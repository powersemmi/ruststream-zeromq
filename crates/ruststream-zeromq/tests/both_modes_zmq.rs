//! One test body run twice: in process, and live over real sockets on the loopback. Only the start
//! call differs; the app is the one a service builds, on `ZmqQueue`.
//!
//! `ZeroMQ` settles nothing, so a `retry_after` is served by the runtime's own fallback: it
//! publishes a copy to the address the subscription reported, which on a queue this service binds
//! is its own listener. The live leg shows the copy leaving one socket and arriving on the other;
//! the in-process leg shows the same copy on the transport the harness connects instead.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::prelude::*;
// The two `Outgoing` names live in different namespaces: the prelude's is the derive on a message
// type, and the value a publish transform rewrites is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::TestApp;
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue, ZmqQueuePublish};
use serde::{Deserialize, Serialize};

/// Long enough to be a delay the runtime actually waits out live, short enough to keep the live
/// run quick. In process the clock is paused, so nothing waits for it there.
const RETRY_DELAY: Duration = Duration::from_millis(300);

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Job {
    id: u64,
}

/// Defers the first delivery and acknowledges the copy that comes back.
#[subscriber("deferred")]
async fn defer_once(job: &Job, ctx: &mut Context) -> HandlerOutcome {
    let _ = job.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(RETRY_DELAY)
    }
}

/// Stamps a retry copy with the subscription the delivery came from, so the copy says where it
/// was made.
#[derive(Debug, Clone, Copy)]
struct StampRetry;

impl<C, Options> PublishTransform<ForReply<C>, Options> for StampRetry {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

/// The app `main` would run: a worker binding its queue on an ephemeral loopback port, whose
/// retry copies go back to its own subscription.
fn deferring_app() -> RustStream {
    RustStream::new(AppInfo::new("zmq-both-modes", "0.0.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
        |b| {
            b.include(defer_once)
                .out_retry(ZmqQueuePublish)
                .transform(StampRetry);
        },
    )
}

async fn a_deferred_copy_comes_back_stamped_and_counted(tb: TestApp<()>) {
    let job = Job { id: 11 };
    tb.broker::<ZmqQueue>()
        .message(&job)
        .to("deferred")
        .publish()
        .await
        .expect("the job reaches the worker");
    tb.broker::<ZmqQueue>()
        .subscriber("deferred")
        .assert_called_once()
        .with(&job)
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY)
        .await
        .expect("the copy comes back and is handled");

    tb.broker::<ZmqQueue>()
        .subscriber("deferred")
        .assert_called(2)
        .with(&job)
        .settled(HandlerOutcome::ack());
    tb.broker::<ZmqQueue>()
        .published::<Job>("deferred")
        .assert_called(2)
        .with(&job)
        .with_header(RETRY_COUNT_HEADER, "1")
        .with_header("x-retried-from", "deferred");

    tb.shutdown().await.expect("the service stops");
}

#[tokio::test(start_paused = true)]
async fn a_deferred_copy_comes_back_stamped_and_counted_in_process() {
    let tb = TestApp::start(deferring_app())
        .await
        .expect("the harness starts");
    a_deferred_copy_comes_back_stamped_and_counted(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deferred_copy_comes_back_stamped_and_counted_live() {
    let tb = TestApp::start_live(deferring_app())
        .await
        .expect("the harness starts over loopback sockets");
    a_deferred_copy_comes_back_stamped_and_counted(tb).await;
}
