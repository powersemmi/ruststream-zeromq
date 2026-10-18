//! The service of the Peers in other languages guide, written without the macros: the handlers are
//! types with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written
//! by hand.
//!
//! ```text
//! cargo run --example manual_guide_interop --features testing -- run
//! cargo test --example manual_guide_interop --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;

// --8<-- [start:service]
use ruststream_zeromq::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
    items: u32,
}

/// Announced on the fan-out, under a name a `jobs.` subscriber matches.
#[derive(Debug, PartialEq, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct JobDone {
    id: u64,
    items: u32,
}

// `#[outgoing(name = "jobs.done")]` by hand.
impl OutgoingDestination for JobDone {
    type Form = FixedName;
    const DESTINATION: &'static str = "jobs.done";
}

impl MessageHeaders for JobDone {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ScoreRequest {
    customer: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Score {
    customer: u64,
    approved: bool,
}

// `#[derive(Outgoing)]` with no name by hand: the ROUTER addresses each answer per request.
impl OutgoingDestination for Score {
    type Form = CallerName;
}

impl MessageHeaders for Score {
    type Contract = NoHeaders;
}

struct Work;

impl Handle<Job, JobDone> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<JobDone, HandlerOutcome>> {
        tracing::info!(job.id, job.items, "job worked");
        ready(Ok(JobDone {
            id: job.id,
            items: job.items,
        }))
    }
}

struct ScoreIt;

impl Handle<ScoreRequest, Score> for ScoreIt {
    fn handle(
        &self,
        request: &ScoreRequest,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Score, HandlerOutcome>> {
        tracing::info!(request.customer, request.amount_cents, "scoring");
        ready(Ok(Score {
            customer: request.customer,
            approved: request.amount_cents <= 50_000,
        }))
    }
}

/// The service on the three endpoints it binds: jobs in, announcements out, requests answered.
fn service(jobs: &str, events: &str, scoring: &str) -> RustStream {
    let events = ZmqFanout::new(ZmqEndpoint::bind(events)).bindable();
    let announce = events.bind(ZmqFanoutPublish);
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind(jobs)), |b| {
            b.include(subscriber("jobs", Work).reply().build())
                .out_reply(announce);
        })
        .with_broker(events, |_b| {})
        .with_broker(ZmqRpc::new(ZmqEndpoint::bind(scoring)), |b| {
            b.include(subscriber("score", ScoreIt).reply().to("reply").build())
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester)
                .out_retry(ZmqRpcPublish)
                .to("score.retry");
        })
}

fn app() -> RustStream {
    service(
        "tcp://0.0.0.0:5555",
        "tcp://0.0.0.0:5556",
        "tcp://0.0.0.0:5557",
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

/// Answers at the `reply-to` address the ROUTER stamped on the request, under the request's
/// `correlation-id`, as the Request and reply guide shows.
struct ReplyToRequester;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(reply_to) = cx.headers().get_shared("reply-to")
            && let Ok(reply_to) = Str::try_from(reply_to)
        {
            out.set_name(reply_to);
        }
        if let Some(correlation) = cx.headers().get_shared("correlation-id") {
            out.headers_mut()
                .insert(Str::from_static("correlation-id"), correlation);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;
    use std::{env, fs, process};

    use bytes::Bytes;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::prelude::*;
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;
    use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

    use super::{Job, app, service};

    /// A job as a foreign PUSH frames it.
    #[derive(Serialize)]
    struct Pushed {
        id: u64,
        items: u32,
    }

    impl OutgoingDestination for Pushed {
        type Form = FixedName;
        const DESTINATION: &'static str = "jobs";
    }

    impl MessageHeaders for Pushed {
        type Contract = NoHeaders;
    }

    /// The announcement as a foreign SUB reads it.
    #[derive(Debug, PartialEq, Deserialize)]
    struct Announced {
        id: u64,
        items: u32,
    }

    #[tokio::test]
    async fn a_pushed_job_is_announced_on_the_fan_out() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqQueue>()
            .message(&Pushed { id: 1, items: 3 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 1, items: 3 });
        tb.broker::<ZmqFanout>()
            .published::<Announced>("jobs.done")
            .assert_called_once()
            .with(&Announced { id: 1, items: 3 });
        tb.shutdown().await?;
        Ok(())
    }

    /// Over real sockets: a raw DEALER composes the request frames by hand, as a peer in another
    /// language does, and reads the answer's frames.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_foreign_dealer_reads_the_answer_frames() -> Result<(), Box<dyn Error>> {
        // A socket path of this test alone, so parallel runs never meet.
        let path = env::temp_dir().join(format!(
            "ruststream-zeromq-guide-interop-manual-{}.sock",
            process::id()
        ));
        let scoring = format!("ipc://{}", path.display());
        let tb = TestApp::start_live(service("tcp://127.0.0.1:0", "tcp://127.0.0.1:0", &scoring))
            .await?;

        let mut dealer = DealerSocket::new();
        dealer.connect(&scoring).await?;
        let mut request = ZmqMessage::from("score");
        request.push_back(Bytes::from_static(b"correlation-id: 42"));
        request.push_back(Bytes::from_static(br#"{"customer":3,"amount_cents":1250}"#));
        dealer.send(request).await?;
        let answer = timeout(Duration::from_secs(10), dealer.recv()).await??;

        let frames: Vec<&[u8]> = answer.iter().map(Bytes::as_ref).collect();
        assert_eq!(frames.len(), 3, "the answer is three frames: {frames:?}");
        assert_eq!(frames[0], b"reply");
        assert!(
            String::from_utf8_lossy(frames[1]).contains("correlation-id: 42"),
            "the answer carries the request's correlation-id: {frames:?}",
        );
        assert_eq!(frames[2], br#"{"customer":3,"approved":true}"#);

        dealer.close().await;
        tb.shutdown().await?;
        let _ = fs::remove_file(path);
        Ok(())
    }
}
