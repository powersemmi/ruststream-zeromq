//! The imports a service that names more than one `ZeroMQ` form writes, in one glob: the
//! framework's prelude, the shared [`ZmqEndpoint`], the [`RequestReply`] capability, and the three
//! form modules.
//!
//! Reach a form's descriptor and policy through its module - `queue::ZmqQueue`, `queue::Publish`,
//! `rpc::Publish`. A service on a single form imports that form's prelude
//! ([`queue::prelude`], [`fanout::prelude`], [`rpc::prelude`]) and writes a bare `Publish` instead.
//!
//! `Publish` is a publish policy, not the framework's `runtime::Publish` builder.
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

pub use ruststream::RequestReply;
pub use ruststream::prelude::*;

pub use crate::endpoint::ZmqEndpoint;

// No bare `Publish` here: each of the three forms has one, so the name would be ambiguous.
pub use crate::{fanout, queue, rpc};
