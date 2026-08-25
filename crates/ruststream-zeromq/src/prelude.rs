//! The imports a mixed-form service on `ZeroMQ` writes, in one glob.
//!
//! A service that runs on one form wants that form's own prelude
//! ([`queue`](crate::queue::prelude), [`fanout`](crate::fanout::prelude),
//! [`rpc`](crate::rpc::prelude)), where the policy is the bare name `Publish`. This one is for a
//! file that names more than one form: it carries the framework's prelude, the shared
//! [`ZmqEndpoint`], the union capability manifest, and the three form modules themselves, so the
//! file qualifies through them - `queue::Publish`, `rpc::Publish`, `queue::ZmqQueue`.
//!
//! There is deliberately no bare `Publish` here. Three forms each have one, and at crate level the
//! name would have to mean one of them; qualifying says which. Globbing two *form* preludes into
//! one file instead of this one is the same question asked the other way, and the compiler answers
//! it: the first use of `Publish` is `E0659`, pointing at both globs, which is the signal to switch
//! to this prelude.
//!
//! `Publish` is a publish *policy*, the value an include site hands to a publisher. It is not the
//! framework's `runtime::Publish`, which is the builder a publish call returns and which services
//! never name.
//!
//! The framework's prelude keeps brokers out of itself, because which broker a service runs on is
//! the one thing every service states for itself. Importing a prelude from *this* crate is that
//! statement: the broker-specificity has moved into the crate path, so the framework glob rides
//! along instead of being written a second line down.
//!
//! # The capability manifest
//!
//! The glob carries exactly the framework capability traits this transport implements, which
//! across all three forms is [`RequestReply`] and nothing else. A handler that bounds on one it did
//! not get is a compile error naming the trait, rather than a method that is missing for reasons
//! the reader has to go and look up.
//!
//! This is the union, so it is the weaker statement of the two: `RequestReply` is here because
//! *some* form has it. Only [`rpc::prelude`] carries it, and the queue and
//! fan-out preludes carry an empty manifest, which is why a single-form service is better served by
//! its form's prelude - the manifest is then a statement about the form it actually runs on.
//!
//! Because these are the framework's own items, a service that globs two broker preludes at once
//! unifies on the same traits instead of colliding, and the compiler checks that rather than the
//! reader.
//!
//! # Examples
//!
//! ```
//! use ruststream_zeromq::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Job {
//!     id: u64,
//! }
//!
//! #[derive(Serialize)]
//! struct Done {
//!     id: u64,
//! }
//!
//! #[subscriber("jobs", publish("results"))]
//! async fn work(job: &Job) -> Done {
//!     Done { id: job.id }
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
//!         queue::ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
//!         |b| {
//!             b.include(work)
//!                 .publisher(TypedPublisher::new(queue::Publish));
//!         },
//!     )
//! }
//! ```

pub use ruststream::prelude::*;

// The union capability manifest: the framework capability traits some form of this transport
// implements. `ZmqRpcPublisher` implements `RequestReply` over DEALER/ROUTER; the transport has no
// transactions, no batch receive, no broker-side partitioning and no history, so
// `TransactionalPublisher`, `OwnedTransactions`, `BatchSubscriber`, `Partitioned`, `Seekable` and
// `Positioned` have no impl here and are absent by that fact, not by oversight.
pub use ruststream::RequestReply;

/// The endpoint is shared by all three forms, so it is named directly rather than qualified.
pub use crate::endpoint::ZmqEndpoint;

// The forms themselves, for qualified access. Everything form-specific is reached through one of
// these - the descriptor (`queue::ZmqQueue`) and the policy (`queue::Publish`) alike - so there is
// one rule in a mixed-form file rather than a rule and an exception.
pub use crate::{fanout, queue, rpc};

// Three things this crate exports are deliberately not here.
//
// The `testing` module: it is feature-gated broker-author tooling, not user API, and a service
// that reaches for it is writing a test, where the extra import line is the point.
//
// The connected forms, the live publishers and the delivered message (`ConnectedZmq*`,
// `Zmq*Publisher`, `ZmqSubscriber`, `ZmqMessage`): the runtime holds these, and a service reaches
// them through the framework rather than by name. Whatever does need one - a publish transform, a
// middleware, a lifecycle hook - names it explicitly, and says by that import which layer it is
// working at.
//
// `ZmqError`: a service names the error where it handles it, which is a handful of places, not
// every file.
