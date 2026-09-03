//! A worker stage of a PUSH/PULL pipeline: upstream pushes work, this service pulls it.
//!
//! The producer runs separately: any peer that speaks the crate's frame layout (name,
//! headers, payload) can push into this socket, Rust or not.
//!
//! ```text
//! cargo run --example zmq_pipeline -- run
//! ```

// --8<-- [start:handler]
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    println!("working on job {}", job.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
