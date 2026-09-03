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

// The descriptors and policies are already distinct per form, so they come in flat. The bare
// names the framework's prelude owns for its slot capability traits - `Publish` today,
// `TransactionalPublish`, `OwnedTransactionalPublish` and `RequestReplyPublish` as they land -
// are not ours to take: an explicit re-export beats the glob above, so an alias under one of
// them would replace a trait with a struct for every service that globs this module. The probes
// below hold the line for the name that exists; extend them as the others arrive.
pub use crate::{
    ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish, fanout, queue,
    rpc,
};

#[cfg(test)]
mod tests {
    //! Every prelude this crate ships must leave `Publish` resolving to the framework's slot
    //! capability trait. Each probe globs one prelude exactly as a service does and then asks
    //! for the trait as a bound, so a shadowing alias fails here - at the import that caused
    //! it - rather than in some downstream handler signature with "expected trait, found
    //! struct".

    #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
    mod root {
        use crate::prelude::*;

        fn probe<T: Publish>() {}
    }

    #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
    mod queue {
        use crate::queue::prelude::*;

        fn probe<T: Publish>() {}
    }

    #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
    mod fanout {
        use crate::fanout::prelude::*;

        fn probe<T: Publish>() {}
    }

    #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
    mod rpc {
        use crate::rpc::prelude::*;

        fn probe<T: Publish>() {}
    }
}
