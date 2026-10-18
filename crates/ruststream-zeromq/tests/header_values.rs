//! The header frame holds `name: value` lines, so a header whose text would change the lines it is
//! written into has no representation in it. A handler publishing one gets an error, in both
//! modes, rather than a publish reported as sent whose message the receiver then refuses or reads
//! with different headers.

#![cfg(feature = "testing")]

use ruststream::HeaderMap;
use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue, ZmqQueuePublish};
use serde::{Deserialize, Serialize};

/// A note to pass on, carrying the header it is passed on with.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, Outgoing)]
struct Draft {
    header: String,
    value: String,
}

/// The note as it is passed on.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize, Outgoing)]
#[outgoing(name = "notes")]
struct Note {
    header: String,
    value: String,
}

#[derive(OutSlot)]
#[publishes(Note)]
struct Notes;

/// Passes a draft on to `notes` with the header it names; a draft whose publish is refused is
/// dropped.
#[subscriber("drafts")]
async fn pass_on(draft: &Draft, Out(notes): Out<impl Publisher, Notes>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert(draft.header.clone(), draft.value.clone());
    let note = Note {
        header: draft.header.clone(),
        value: draft.value.clone(),
    };
    match notes.message(&note).with_headers(headers).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::drop(),
    }
}

/// Takes a note whose header arrived as it was sent, and drops one whose header changed on the way.
#[subscriber("notes")]
async fn take_note(note: &Note, ctx: &mut Context) -> HandlerOutcome {
    if ctx.headers().get_str(&note.header) == Some(note.value.as_str()) {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// The relay and the note taker each bind a queue of their own; the relay's slot publishes to the
/// note taker's.
fn notes_app() -> RustStream {
    let notes = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")).bindable();
    let to_notes = notes.bind(ZmqQueuePublish);
    RustStream::new(AppInfo::new("zmq-header-values", "0.0.0"))
        .with_broker_labeled("notes", notes, |b| {
            b.include(take_note);
        })
        .with_broker_labeled(
            "relay",
            ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
            |b| {
                b.include(pass_on).out(Notes, to_notes).build();
            },
        )
}

fn draft(header: &str, value: &str) -> Draft {
    Draft {
        header: header.to_owned(),
        value: value.to_owned(),
    }
}

/// Each header the text frame cannot carry as written.
fn unwritable_drafts() -> Vec<Draft> {
    vec![
        draft("x-note", "first\nsecond"),
        draft("x-tenant", "acme\nx-ruststream-retry-count: 9"),
        draft("x-note", "first\rsecond"),
        draft("x-note", " padded"),
        draft("x-note", "padded "),
        draft("x-a:b", "value"),
        draft("x-a\nb", "value"),
        draft(" x-note", "value"),
    ]
}

async fn every_unwritable_header_is_refused(tb: TestApp<()>) {
    let drafts = unwritable_drafts();
    for draft in &drafts {
        tb.broker_named("relay")
            .message(draft)
            .to("drafts")
            .publish()
            .await
            .expect("the draft reaches the relay");
    }
    tb.settle().await.expect("every draft is handled");

    let outcomes = tb.broker_named("relay").subscriber("drafts").outcomes();
    assert_eq!(
        outcomes.len(),
        drafts.len(),
        "every draft reaches the relay"
    );
    assert!(
        outcomes.iter().all(|outcome| *outcome == Outcome::Drop),
        "the publish of a header the frame cannot carry must fail, got {outcomes:?}",
    );
    tb.broker_named("notes")
        .subscriber("notes")
        .assert_not_called();

    tb.shutdown().await.expect("the service stops");
}

/// A value the frame does carry arrives as it was written, inner spaces and separators included.
async fn a_writable_value_arrives_unchanged(tb: TestApp<()>) {
    let draft = draft("x-note", "a: b  c");
    tb.broker_named("relay")
        .message(&draft)
        .to("drafts")
        .publish()
        .await
        .expect("the draft reaches the relay");
    tb.settle()
        .await
        .expect("the note is passed on and handled");

    tb.broker_named("relay")
        .subscriber("drafts")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.broker_named("notes")
        .subscriber("notes")
        .assert_called_once()
        .with(&Note {
            header: draft.header,
            value: draft.value,
        })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the service stops");
}

#[tokio::test(start_paused = true)]
async fn every_unwritable_header_is_refused_in_process() {
    let tb = TestApp::start(notes_app())
        .await
        .expect("the harness starts");
    every_unwritable_header_is_refused(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_unwritable_header_is_refused_live() {
    let tb = TestApp::start_live(notes_app())
        .await
        .expect("the harness starts over loopback sockets");
    every_unwritable_header_is_refused(tb).await;
}

#[tokio::test(start_paused = true)]
async fn a_writable_value_arrives_unchanged_in_process() {
    let tb = TestApp::start(notes_app())
        .await
        .expect("the harness starts");
    a_writable_value_arrives_unchanged(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writable_value_arrives_unchanged_live() {
    let tb = TestApp::start_live(notes_app())
        .await
        .expect("the harness starts over loopback sockets");
    a_writable_value_arrives_unchanged(tb).await;
}
