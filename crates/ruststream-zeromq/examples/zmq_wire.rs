//! The wire a peer in another language speaks: a worker reads jobs a Python peer pushes as three
//! frames, and pushes its results to a sink the same way.
//!
//! ```text
//! cargo test --example zmq_wire --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_zeromq::Connect;
use ruststream_zeromq::queue::prelude::*;

// --8<-- [start:handler]
/// What the peer's payload frame holds: the JSON document this type deserializes from.
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

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    tracing::info!(job.id, "working");
    Done { id: job.id }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn service(jobs: ZmqEndpoint, sink: ZmqEndpoint<Connect>) -> impl App {
    let results = ZmqQueue::new(sink).bindable();
    let to_results = results.bind(Publish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(jobs), |b| {
            b.include(work).out_reply(to_results);
        })
        .register_broker(results)
}

#[ruststream::app]
fn app() -> impl App {
    service(
        ZmqEndpoint::bind("tcp://0.0.0.0:5555"),
        ZmqEndpoint::connect("tcp://sink:5556"),
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use std::{env, process, str};

    use bytes::Bytes;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::queue::prelude::*;
    use tokio::time::timeout;
    use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend, ZmqMessage as Frames};

    use super::{Done, Job, app, service};

    /// Long enough for a handshake and a round trip on a loaded machine.
    const ROUND_TRIP: Duration = Duration::from_secs(10);

    /// A socket path of this test alone, so parallel tests and other processes never meet.
    fn ipc(label: &str) -> (String, PathBuf) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "zmq-wire-{label}-{}-{}.sock",
            process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        (format!("ipc://{}", path.display()), path)
    }

    /// Runs the worker over real sockets between a raw PUSH peer and a raw PULL sink, sends
    /// `frames` from the peer, and returns the frames the sink reads.
    async fn round_trip(frames: Frames) -> Result<Vec<Bytes>, Box<dyn Error>> {
        let (jobs, jobs_path) = ipc("jobs");
        let (sink, sink_path) = ipc("sink");
        let mut results = PullSocket::new();
        results.bind(&sink).await?;
        let tb = TestApp::start_live(service(
            ZmqEndpoint::bind(jobs.clone()),
            ZmqEndpoint::connect(sink),
        ))
        .await?;

        let mut peer = PushSocket::new();
        peer.connect(&jobs).await?;
        peer.send(frames).await?;
        let result = timeout(ROUND_TRIP, results.recv()).await??;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 2 });
        tb.shutdown().await?;
        for path in [jobs_path, sink_path] {
            let _ = fs::remove_file(path);
        }
        Ok(result.into_vec())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_composes_three_frames_and_reads_three_back() -> Result<(), Box<dyn Error>> {
        let mut frames = Frames::from("jobs");
        frames.push_back(Bytes::from_static(b"content-type: application/json"));
        frames.push_back(Bytes::from_static(br#"{"id":2}"#));

        let result = round_trip(frames).await?;

        assert_eq!(result.len(), 3, "name, headers, payload: {result:?}");
        assert_eq!(result[0].as_ref(), b"results");
        assert!(str::from_utf8(&result[1]).is_ok(), "{result:?}");
        assert_eq!(serde_json::from_slice::<Done>(&result[2])?, Done { id: 2 });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_two_frame_message_reads_as_headerless() -> Result<(), Box<dyn Error>> {
        let mut frames = Frames::from("jobs");
        frames.push_back(Bytes::from_static(br#"{"id":2}"#));

        let result = round_trip(frames).await?;

        assert_eq!(serde_json::from_slice::<Done>(&result[2])?, Done { id: 2 });
        Ok(())
    }

    #[tokio::test]
    async fn a_job_is_answered_on_the_sink_in_process() -> Result<(), Box<dyn Error>> {
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
}
