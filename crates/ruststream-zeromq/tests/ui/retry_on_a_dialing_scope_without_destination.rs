use ruststream::runtime::{AppInfo, RustStream};
use ruststream_zeromq::queue::prelude::*;
use ruststream_zeromq::{Connect, ZmqEndpoint};
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::ack()
}

// A worker that dials a ventilator reads from a PUSH peer that takes nothing, so its queue
// addresses no retry copy. The same mount on a scope of a production app is refused where the
// scope is written, not when the app starts.
fn main() {
    let _ = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::<Connect>::new(ZmqEndpoint::connect("tcp://ventilator:5555")),
        |b| b.include(work).out_retry(Publish),
    );
}
