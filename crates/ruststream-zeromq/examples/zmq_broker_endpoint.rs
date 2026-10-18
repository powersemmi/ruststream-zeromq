//! Endpoints: a worker listens on a local socket for the producers on its host and pushes its
//! results to a collector it dials over the network.
//!
//! ```text
//! cargo test --example zmq_broker_endpoint --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::queue::prelude::*;

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    tracing::info!(job.id, "working on the job");
    Done { id: job.id }
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // This process dials the collector, which listens on another host.
    let collector = ZmqQueue::new(ZmqEndpoint::connect("tcp://collector:5560")).bindable();
    let results = collector.bind(Publish);
    // This process listens, on a socket file the producers of this host dial.
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind("ipc:///tmp/jobs")), |b| {
            b.include(work).out_reply(results);
        })
        .register_broker(collector)
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;

    use super::{Done, Job, app, work};

    #[tokio::test]
    async fn a_job_from_the_socket_file_reaches_the_collector() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Job { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue<Connect>>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn another_scheme_is_refused_naming_the_address() {
        let app = RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("udp://0.0.0.0:5555")),
            |b| {
                b.include(work);
            },
        );

        let Err(refused) = TestApp::start(app).await else {
            panic!("ZeroMQ runs on tcp:// and ipc:// only");
        };
        let refused = refused.to_string();
        assert!(
            refused.contains("udp://0.0.0.0:5555"),
            "the refusal names the address, got: {refused}",
        );
    }
}
