//! The service of the Testing guide and its tests: a worker that announces each job it finishes,
//! holds a job whose input has not arrived yet, and runs the same test in process and over
//! loopback sockets.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_zeromq::prelude::*;
use serde::{Deserialize, Serialize};

/// How long a job whose input has not arrived waits before it is tried again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    uploaded: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs.done")]
struct JobDone {
    id: u64,
}

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Result<JobDone, HandlerOutcome> {
    if !job.uploaded {
        tracing::info!(job.id, "input not uploaded yet, holding the job");
        return Err(HandlerOutcome::retry_after(HOLD));
    }
    tracing::info!(job.id, "job done");
    Ok(JobDone { id: job.id })
}

/// The worker on the endpoints it binds: jobs in, announcements out.
fn service(jobs: &str, events: &str) -> RustStream {
    let events = ZmqFanout::new(ZmqEndpoint::bind(events)).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind(jobs)), |b| {
            b.include(work)
                .max_attempts(nonzero!(3u32))
                .out_reply(announce);
        })
        .with_broker(events, |_b| {})
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service("tcp://0.0.0.0:5555", "tcp://0.0.0.0:5556")
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_zeromq::prelude::*;

    use super::{HOLD, Job, JobDone, app, service};

    // --8<-- [start:in_process]
    /// The body both modes run: a job arrives, is worked, and its announcement leaves.
    async fn a_job_is_worked_and_announced(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let job = Job {
            id: 7,
            uploaded: true,
        };
        tb.broker::<ZmqQueue>().message(&job).publish().await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&job)
            .settled(HandlerOutcome::ack());
        tb.broker::<ZmqFanout>()
            .published::<JobDone>("jobs.done")
            .assert_called_once()
            .with(&JobDone { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        a_job_is_worked_and_announced(TestApp::start(app()).await?).await
    }
    // --8<-- [end:in_process]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_job_without_its_input_is_held() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let job = Job {
            id: 8,
            uploaded: false,
        };
        tb.broker::<ZmqQueue>().message(&job).publish().await?;
        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The clock is paused: a minute of HOLD passes at once.
        tb.advance(HOLD).await?;
        tb.advance(HOLD).await?;

        let queue = tb.broker::<ZmqQueue>();
        assert_eq!(
            queue.subscriber("jobs").outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack]
        );
        queue
            .published::<Job>("jobs")
            .assert_called(3)
            .with_header(RETRY_COUNT_HEADER, "2");
        tb.broker::<ZmqFanout>()
            .published::<JobDone>("jobs.done")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn over_loopback_sockets() -> Result<(), Box<dyn Error>> {
        // Port zero: the system picks a free port, so parallel tests never collide.
        let tb = TestApp::start_live(service("tcp://127.0.0.1:0", "tcp://127.0.0.1:0")).await?;
        a_job_is_worked_and_announced(tb).await
    }
    // --8<-- [end:live]
}
