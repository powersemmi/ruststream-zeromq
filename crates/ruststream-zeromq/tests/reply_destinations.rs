//! Where a reply lands on this transport: at the name its own type declares, or at the name the
//! mount site supplies to a type that declares none. Both resolutions run through the `TestApp`
//! harness on the in-process transport.

#![cfg(feature = "testing")]

use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_zeromq::testing::{ZmqTestBroker, ZmqTestPublish};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Job {
    id: u64,
}

// The result queue is a property of the message, so the type names it and the clause stays bare.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish)]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Greeting {
    who: String,
}

// An answer has no destination of its own on the request/reply form, so the type declares none
// and the mount site supplies the name.
#[derive(Debug, Deserialize, PartialEq, Serialize, Outgoing)]
struct Answer {
    text: String,
}

#[subscriber("greeter", publish("answers"))]
async fn greet(request: &Greeting) -> Answer {
    Answer {
        text: format!("hello {}", request.who),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_names_its_queue_publishes_there() {
    let app = RustStream::new(AppInfo::new("zmq-declared-reply", "0.0.0")).with_broker(
        ZmqTestBroker::new(),
        |b| {
            b.include(work).out(Reply, ZmqTestPublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker>()
        .message(&Job { id: 7 })
        .to("jobs")
        .publish()
        .await
        .expect("the job is published");

    tb.broker::<ZmqTestBroker>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Job { id: 7 });
    // No name appears at the mount site, so this one can only come from the reply type.
    tb.broker::<ZmqTestBroker>()
        .published::<Done>("results")
        .assert_called_once()
        .with(&Done { id: 7 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_without_a_name_publishes_where_the_mount_site_says() {
    let app = RustStream::new(AppInfo::new("zmq-mounted-reply", "0.0.0")).with_broker(
        ZmqTestBroker::new(),
        |b| {
            b.include(greet).out(Reply, ZmqTestPublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<ZmqTestBroker>()
        .message(&Greeting {
            who: "world".to_owned(),
        })
        .to("greeter")
        .publish()
        .await
        .expect("the request is published");

    tb.broker::<ZmqTestBroker>()
        .subscriber("greeter")
        .assert_called_once()
        .with(&Greeting {
            who: "world".to_owned(),
        });
    // `Answer` declares nothing, so this name is the one the `publish("answers")` clause supplied.
    tb.broker::<ZmqTestBroker>()
        .published::<Answer>("answers")
        .assert_called_once()
        .with(&Answer {
            text: "hello world".to_owned(),
        });
}
