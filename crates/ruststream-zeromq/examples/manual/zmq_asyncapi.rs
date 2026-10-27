//! The service of `examples/zmq_asyncapi.rs` without the macros: the handlers are named types and
//! `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_zmq_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_zmq_asyncapi --features asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::Connect;
use ruststream_zeromq::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Greeting {
    who: String,
}

// --8<-- [start:channels]
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
}

// No destination of its own: the mount names `results`, the name frame of every result.
impl OutgoingDestination for Done {
    type Form = CallerName;
}

impl MessageHeaders for Done {
    type Contract = NoHeaders;
}

struct Work;

impl Handle<Job, Done> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Done, HandlerOutcome>> {
        tracing::info!(job.id, "working");
        ready(Ok(Done { id: job.id }))
    }
}
// --8<-- [end:channels]

// --8<-- [start:replies]
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Answer {
    text: String,
}

// The ROUTER addresses each answer per request, so `reply` is only the fallback name.
impl OutgoingDestination for Answer {
    type Form = CallerName;
}

impl MessageHeaders for Answer {
    type Contract = NoHeaders;
}

struct Greet;

impl Handle<Greeting, Answer> for Greet {
    fn handle(
        &self,
        request: &Greeting,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Answer, HandlerOutcome>> {
        ready(Ok(Answer {
            text: format!("hello {}", request.who),
        }))
    }
}

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
// --8<-- [end:replies]

// --8<-- [start:server]
/// The worker dials the dispatcher that hands out the jobs.
fn jobs() -> ZmqQueue<Connect> {
    ZmqQueue::new(ZmqEndpoint::connect("tcp://worker:5555"))
}

/// The greeter listens on every interface.
fn greeter() -> ZmqRpc {
    ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557"))
}
// --8<-- [end:server]

// --8<-- [start:app]
fn app() -> impl App {
    // Each label is the key of the broker's server in the document.
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker_labeled("jobs", jobs(), |b| {
            b.include(subscriber("jobs", Work).reply().to("results").build())
                .out_reply(ZmqQueuePublish);
        })
        .with_broker_labeled("rpc", greeter(), |b| {
            b.include(subscriber("greeter", Greet).reply().to("reply").build())
                .out_reply(ZmqRpcPublish)
                .transform(ReplyToRequester)
                .out_retry(ZmqRpcPublish)
                .to("greeter.retry");
        })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::app;

    /// The excerpts the pages show; the crate's own document tests pin them too.
    const QUEUE: &str = include_str!("../../tests/documents/queue.json");
    const REPLY_ADDRESS: &str = include_str!("../../tests/documents/reply-address.json");

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&build_spec(&app()).to_json()?)?)
    }

    /// True when every field of `expected` appears in `actual` with the same value.
    fn contains(actual: &Value, expected: &Value) -> bool {
        match (actual, expected) {
            (Value::Object(actual), Value::Object(expected)) => expected
                .iter()
                .all(|(key, value)| actual.get(key).is_some_and(|found| contains(found, value))),
            (actual, expected) => actual == expected,
        }
    }

    #[test]
    fn the_document_carries_the_excerpts_the_pages_show() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        for excerpt in [QUEUE, REPLY_ADDRESS] {
            let expected: Value = serde_json::from_str(excerpt)?;
            assert!(contains(&spec, &expected), "{expected:#}\nnot in\n{spec:#}");
        }
        Ok(())
    }

    #[test]
    fn each_server_says_how_to_attach() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["rpc"],
            json!({
                "host": "0.0.0.0:5557",
                "protocol": "zeromq",
                "protocolVersion": "3.0",
                "bindings": {
                    "x-ruststream-zeromq": {
                        "transport": "tcp",
                        "endpoint": "0.0.0.0:5557",
                        "role": "bind",
                        "envelope": { "name": "frames" },
                    },
                },
            })
        );
        assert_eq!(
            spec["servers"]["jobs"]["bindings"]["x-ruststream-zeromq"]["role"],
            "connect"
        );
        Ok(())
    }

    #[test]
    fn a_channel_reports_its_socket_pair_and_name_frame() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(
            channels["results"]["bindings"]["x-ruststream-zeromq"],
            json!({ "socketPair": "PUSH/PULL", "nameFrame": "results" })
        );
        // A reply travels to the identity the ROUTER supplies, so no name frame reaches it.
        assert_eq!(
            channels["reply"]["bindings"]["x-ruststream-zeromq"],
            json!({ "socketPair": "DEALER/ROUTER" })
        );
        Ok(())
    }

    #[test]
    fn a_responder_points_at_the_reply_to_header() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(spec["channels"]["reply"]["address"], Value::Null);
        assert_eq!(
            spec["operations"]["receive_greeter"]["reply"]["address"]["location"],
            "$message.header#/reply-to"
        );
        Ok(())
    }
}
