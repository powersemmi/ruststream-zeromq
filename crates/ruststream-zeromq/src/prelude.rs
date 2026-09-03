//! The imports a service that names more than one `ZeroMQ` form writes, in one glob.
//!
//! The framework's prelude, the shared [`ZmqEndpoint`], the [`RequestReply`] capability, the
//! three descriptors with their publish policies, and the three form modules (for the connected
//! forms and live publishers, which a service rarely names).
//!
//! A service on a single form imports that form's prelude instead: [`queue::prelude`],
//! [`fanout::prelude`], [`rpc::prelude`].
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
//!         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
//!         |b| {
//!             b.include(work)
//!                 .publisher(TypedPublisher::new(ZmqQueuePublish));
//!         },
//!     )
//! }
//! ```

pub use ruststream::RequestReply;
pub use ruststream::prelude::*;

pub use crate::endpoint::ZmqEndpoint;

// The descriptors and policies are already distinct per form, so they come in flat; the
// framework's prelude owns `Publish` (the out-slot capability trait), and a per-form alias of
// that name would shadow it.
pub use crate::{
    ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish, fanout, queue,
    rpc,
};

#[cfg(test)]
mod tests {
    /// Every prelude this crate ships must leave `Publish` resolving to the framework's
    /// out-slot capability trait. A name of our own would shadow it under a glob import, and a
    /// body that bounds its slot on it would fail with "expected trait, found struct" at the
    /// signature rather than at the import. The bounds below are the whole assertion.
    #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
    mod publish_is_the_framework_trait {
        fn root<P: crate::prelude::Publish>(_: &P) {}
        fn queue<P: crate::queue::prelude::Publish>(_: &P) {}
        fn fanout<P: crate::fanout::prelude::Publish>(_: &P) {}
        fn rpc<P: crate::rpc::prelude::Publish>(_: &P) {}
    }
}
