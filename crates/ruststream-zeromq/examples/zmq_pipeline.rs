//! A worker stage of a PUSH/PULL pipeline: upstream pushes work, this service pulls it.
//!
//! Run the producer side separately (any libzmq peer works), or use `zmq_bridge_peer.py`
//! from the repository as the non-Rust side.

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerResult {
    println!("working on job {}", job.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://*:5555")),
        |b| {
            b.include(handle);
        },
    )
}
