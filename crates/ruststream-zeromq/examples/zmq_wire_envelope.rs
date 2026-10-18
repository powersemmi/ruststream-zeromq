//! The request-reply envelope: a responder a Python DEALER asks. The request is the usual three
//! frames with a `correlation-id` header line; the answer comes back behind the asker's identity,
//! with `reply` in its name frame.
//!
//! ```text
//! cargo test --example zmq_wire_envelope --features testing
//! ```

// The prelude's `Outgoing` is the derive; the publish pipeline's message of the same name is the
// type a transform rewrites.
use ruststream::runtime::{Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

use ruststream_zeromq::rpc::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "greeter")]
struct Greeting {
    who: String,
}

// No destination of its own: the ROUTER addresses each answer per request.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Answer {
    text: String,
}

#[subscriber("greeter", reply("reply"))]
async fn greet(request: &Greeting) -> Answer {
    tracing::info!(who = %request.who, "greeting");
    Answer {
        text: format!("hello {}", request.who),
    }
}

// --8<-- [start:app]
fn service(endpoint: ZmqEndpoint) -> impl App {
    RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(ZmqRpc::new(endpoint), |b| {
        // The answer goes to the identity the ROUTER read off the request, and it carries the
        // request's `correlation-id` back.
        b.include(greet)
            .out_reply(Publish)
            .transform(ReplyToRequester)
            .out_retry(Publish)
            .to("greeter.retry");
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(ZmqEndpoint::bind("tcp://0.0.0.0:5557"))
}
// --8<-- [end:app]

/// Answers at the `reply-to` address the ROUTER stamped on the request, under the request's
/// `correlation-id`.
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
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use std::{env, process, str};

    use bytes::Bytes;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::rpc::prelude::*;
    use tokio::time::timeout;
    use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage as Frames};

    use super::{Answer, Greeting, app, service};

    /// Long enough for a handshake and a round trip on a loaded machine.
    const ROUND_TRIP: Duration = Duration::from_secs(10);

    /// A socket path of this test alone, so parallel tests and other processes never meet.
    fn ipc(label: &str) -> (String, PathBuf) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "zmq-envelope-{label}-{}-{}.sock",
            process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        (format!("ipc://{}", path.display()), path)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dealer_peer_reads_its_answer_behind_reply() -> Result<(), Box<dyn Error>> {
        let (address, path) = ipc("greeter");
        let tb = TestApp::start_live(service(ZmqEndpoint::bind(address.clone()))).await?;

        // The foreign peer: a raw DEALER composing the request by hand.
        let mut peer = DealerSocket::new();
        peer.connect(&address).await?;
        let mut request = Frames::from("greeter");
        request.push_back(Bytes::from_static(
            b"content-type: application/json\ncorrelation-id: ask-1",
        ));
        request.push_back(Bytes::from_static(br#"{"who":"world"}"#));
        peer.send(request).await?;
        let answer = timeout(ROUND_TRIP, peer.recv()).await??.into_vec();

        // The ROUTER took the identity frame off: the DEALER reads the three frames behind it.
        assert_eq!(answer.len(), 3, "name, headers, payload: {answer:?}");
        assert_eq!(answer[0].as_ref(), b"reply");
        let headers = str::from_utf8(&answer[1])?;
        assert!(
            headers.lines().any(|line| line == "correlation-id: ask-1"),
            "the answer carries the request's correlation id: {headers}",
        );
        assert_eq!(
            serde_json::from_slice::<Answer>(&answer[2])?,
            Answer {
                text: "hello world".to_owned()
            },
        );

        tb.broker::<ZmqRpc>()
            .subscriber("greeter")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        let _ = fs::remove_file(path);
        Ok(())
    }

    #[tokio::test]
    async fn a_request_is_answered_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqRpc>()
            .message(&Greeting {
                who: "world".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<ZmqRpc>()
            .subscriber("greeter")
            .assert_called_once()
            .with(&Greeting {
                who: "world".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
