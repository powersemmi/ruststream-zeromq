//! A PUSH/PULL worker that settles a whole page of jobs at a time.
//!
//! Nothing about the wire changes: the producer still pushes one message at a time, in the same
//! frame layout, and the pages are assembled on this side.
//!
//! ```text
//! cargo run --example zmq_pages -- run
//! ```

// --8<-- [start:handler]
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn drain(jobs: &[Job]) -> HandlerOutcome {
    let ids: Vec<u64> = jobs.iter().map(|job| job.id).collect();
    println!("working on a page of {}: {ids:?}", jobs.len());
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(drain.batch(nonzero!(32)));
        },
    )
}
// --8<-- [end:app]
