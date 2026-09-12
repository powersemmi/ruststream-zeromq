//! In-process test support, behind the `testing` feature.
//!
//! [`ZmqTestBroker`] is a stand-in transport that reproduces the crate's routing in memory - no
//! server, no sockets - and implements [`TestableBroker`](ruststream::testing::TestableBroker) on
//! its connected form, so application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness and the framework's conformance suite runs
//! against it.
//!
//! The three production policies pair here, so a routes file keeps the policy the service ships:
//! [`ZmqQueuePublish`](crate::ZmqQueuePublish), [`ZmqFanoutPublish`](crate::ZmqFanoutPublish) and
//! [`ZmqRpcPublish`](crate::ZmqRpcPublish). Each pattern keeps the part of its behaviour a channel
//! can hold honestly:
//!
//! * the queue hands each message to one of the consumers on the destination, so two workers
//!   mounted on one name do not both run;
//! * the fan-out filters by name prefix, the protocol's own rule, and drops what nothing matches;
//! * the request-reply exchange correlates an answer to its request and routes it back to the
//!   caller that asked, and only to that caller.
//!
//! The ladder holds too, aliasing included: `shutdown` closes the transport before dropping what
//! it carried, so a publisher paired earlier - or a clone of the broker - reports
//! [`ZmqError::NotConnected`](crate::ZmqError::NotConnected) instead of succeeding against a
//! broker that is gone.
//!
//! What has no counterpart in a channel is not imitated. There is no peer to connect, so a
//! publish that a real PUSH socket would fail after its retry window is recorded and dropped here,
//! and a request timeout covers the wait for an answer alone. Delivery guarantees, high-water
//! marks and the slow joiner are transport behaviour: exercise them on the loopback suite, which
//! needs no external service.
//!
//! Settlement answers what the transport answers. `ZeroMQ` settles nothing, so a delivery from
//! this stand-in returns [`AckError::Unsupported`](ruststream::AckError::Unsupported) from `ack`
//! and from `nack`, and never comes back, exactly as a delivery over a socket does. A handler
//! that settles by retrying fails its test here instead of losing its message after deployment.
//! Cover redelivery on a broker that has it.
//!
//! One divergence is left for the test author to keep out of assertions, and it is one-way: the
//! stand-in offers more than the transport, never less. The capability split of a responder's
//! subscriber cannot be reproduced while the crate mounts by name. The real
//! [`ZmqRpcSubscriber`](crate::ZmqRpcSubscriber) is deliberately no
//! [`BatchSubscriber`](ruststream::BatchSubscriber), but a mount site names only a string here, so
//! nothing tells the stand-in which pattern a subscription belongs to, and `.batch(..)` on a
//! request-reply mount compiles in a test where production rejects it. The publish side has no
//! such gap, because there the policy at the mount site names the pattern.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedZmqTestBroker, ZmqTestBroker};
pub use publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
pub use subscriber::{ZmqTestMessage, ZmqTestSubscriber};
