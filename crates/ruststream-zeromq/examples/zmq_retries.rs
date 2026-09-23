//! A worker that asks for a retry, on a transport that settles nothing.
//!
//! `ZeroMQ` has no delivery counter and no dead-letter topology, so the copies are published by
//! this service and counted by the framework. The mount site declares how many attempts a job
//! gets, and the copies come back through the queue the worker binds.
//!
//! ```text
//! cargo run --example zmq_retries -- run
//! ```

use std::io;
use std::time::Duration;

use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

/// Stands in for the work: whatever the job actually does, it can fail and be worth another try.
fn run(job: &Job) -> io::Result<()> {
    if job.id == 0 {
        return Err(io::Error::other("a job carries no id"));
    }
    println!("working on job {}", job.id);
    Ok(())
}

// --8<-- [start:handler]
#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    match run(job) {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            eprintln!("job {} failed, retrying: {err}", job.id);
            HandlerOutcome::retry_after(Duration::from_secs(30))
        }
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            // --8<-- [start:declaration]
            b.include(handle)
                .max_attempts(nonzero!(5u32))
                .out_retry(Publish);
            // --8<-- [end:declaration]
        },
    )
}
