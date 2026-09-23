use ruststream_zeromq::Connect;
use ruststream_zeromq::queue::prelude::*;
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
// addresses no retry copy. The copies used to be published back to that peer and dropped with a
// warning; a router chain that names no destination for them now fails at `.build()`.
fn main() {
    let _ = Router::<ZmqQueue<Connect>>::new()
        .include(work)
        .out_retry(Publish)
        .build();
}
