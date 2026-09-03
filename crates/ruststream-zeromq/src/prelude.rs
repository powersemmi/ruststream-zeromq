//! The imports a routes file that mounts more than one `ZeroMQ` form writes, in one glob.
//!
//! The framework's prelude, the shared [`ZmqEndpoint`], the [`RequestReply`] capability, the
//! three descriptors with their publish policies, and the three form modules (for the connected
//! forms and live publishers, which a service rarely names).
//!
//! Policies keep their prefixed names here, because all three forms call theirs `Publish` and one
//! glob cannot carry three. A routes file on a single form imports that form's prelude instead -
//! [`queue::prelude`], [`fanout::prelude`], [`rpc::prelude`] - and writes the bare `Publish`.
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

// Prefixed here, aliased to the bare `Publish` in each form prelude: three forms cannot share
// one bare name in a single glob, and a routes file that mounts two of them is exactly the file
// that needs to tell them apart.
pub use crate::{
    ZmqFanout, ZmqFanoutPublish, ZmqQueue, ZmqQueuePublish, ZmqRpc, ZmqRpcPublish, fanout, queue,
    rpc,
};

#[cfg(test)]
mod tests {
    //! What each prelude must carry, checked by globbing it exactly as a file would.
    //!
    //! A handler bound names a broker capability trait ([`Publisher`] and its siblings) and a
    //! mount site names a policy, so both vocabularies have to survive the glob: the capability
    //! trait as a bound, the policy as a value. A form prelude offers its policy under the bare
    //! `Publish`; the crate prelude cannot, because three forms would collide, so it offers the
    //! prefixed names and this pins that difference.

    mod queue {
        use crate::queue::prelude::*;

        #[expect(dead_code, reason = "the bound is the assertion; nothing calls it")]
        fn handler_bound<T: Publisher>() {}

        #[test]
        fn the_mount_site_vocabulary_is_the_bare_name() {
            let _: Publish = Publish;
        }
    }

    mod fanout {
        use crate::fanout::prelude::*;

        #[expect(dead_code, reason = "the bound is the assertion; nothing calls it")]
        fn handler_bound<T: Publisher>() {}

        #[test]
        fn the_mount_site_vocabulary_is_the_bare_name() {
            let _: Publish = Publish;
        }
    }

    mod rpc {
        use crate::rpc::prelude::*;

        #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
        fn handler_bound<T: Publisher>() {}

        // The request side rides the same policy, so `RequestReply` is a bound like any other.
        #[expect(dead_code, reason = "the bounds are the assertion; nothing calls them")]
        fn request_bound<T: RequestReply>() {}

        #[test]
        fn the_mount_site_vocabulary_is_the_bare_name() {
            let _: Publish = Publish;
        }
    }

    mod root {
        use crate::prelude::*;

        #[expect(dead_code, reason = "the bound is the assertion; nothing calls it")]
        fn handler_bound<T: Publisher>() {}

        #[test]
        fn the_three_policies_arrive_under_their_prefixed_names() {
            let _: ZmqQueuePublish = ZmqQueuePublish;
            let _: ZmqFanoutPublish = ZmqFanoutPublish;
            let _: ZmqRpcPublish = ZmqRpcPublish;
        }
    }
}
