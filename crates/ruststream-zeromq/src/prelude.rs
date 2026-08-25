//! The imports a service on `ZeroMQ` writes every time, in one glob.
//!
//! `use ruststream_zeromq::prelude::*;` brings in the framework's own prelude and this
//! transport's user-facing surface: the three socket patterns, the endpoint that says which side
//! listens, and the publish policy each pattern pairs with. One import serves a service file.
//!
//! The framework's prelude keeps brokers out of itself, because which broker a service runs on is
//! the one thing every service states for itself. Importing *this* prelude is that statement: the
//! broker-specificity has moved into the crate path, so the framework glob rides along instead of
//! being written a second line down.
//!
//! It is also a capability manifest: the glob carries exactly the framework capability traits this
//! transport implements, which here is [`RequestReply`] and nothing else. A handler that bounds on
//! one it did not get is a compile error naming the trait, rather than a method that is missing for
//! reasons the reader has to go and look up. Because these are the framework's own items, a service
//! that globs two broker preludes at once unifies on the same traits instead of colliding, and the
//! compiler checks that rather than the reader.
//!
//! # Examples
//!
//! ```
//! use ruststream_zeromq::prelude::*;
//!
//! #[subscriber("jobs")]
//! async fn handle(job: &[u8]) -> HandlerResult {
//!     let _ = job.len();
//!     HandlerResult::Ack
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
//!         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
//!         |b| {
//!             b.include(handle);
//!         },
//!     )
//! }
//! ```

pub use ruststream::prelude::*;

// The capability manifest: the framework capability traits this transport implements, and only
// those. `ZmqRpcPublisher` implements `RequestReply` over DEALER/ROUTER; the transport has no
// transactions, no batch receive, no broker-side partitioning and no history, so
// `TransactionalPublisher`, `OwnedTransactions`, `BatchSubscriber`, `Partitioned`, `Seekable` and
// `Positioned` have no impl here and are absent by that fact, not by oversight.
pub use ruststream::RequestReply;

pub use crate::endpoint::ZmqEndpoint;
pub use crate::fanout::{ZmqFanout, ZmqFanoutPublish};
pub use crate::queue::{ZmqQueue, ZmqQueuePublish};
pub use crate::rpc::{ZmqRpc, ZmqRpcPublish};

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
