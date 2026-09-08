//! In-process test support, behind the `testing` feature.
//!
//! [`ZmqTestBroker`] is a stand-in transport that reproduces the crate's routing in memory - no
//! server, no sockets - and implements [`TestableBroker`](ruststream::testing::TestableBroker) on
//! its connected form, so application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness.
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
//! Settlement is reproduced by refusing it: `ZeroMQ` acknowledges nothing, so a delivery here
//! reports [`AckError::Unsupported`](ruststream::AckError::Unsupported) for `ack` and `nack` just
//! as [`ZmqMessage`](crate::ZmqMessage) does, and a handler that settles by retrying loses its
//! message under the harness the same way it loses it on the wire. That is also why the
//! framework's routing suite (`conformance::harness::run_suite`) is not run against this stand-in:
//! every one of its scenarios settles the delivery it received, so passing it would mean settling
//! what the transport cannot. The routing it checks is covered directly in the crate's
//! `testing_core` tests.
//!
//! What has no counterpart in a channel is not imitated. There is no peer to connect, so a
//! publish that a real PUSH socket would fail after its retry window is recorded and dropped here,
//! and a request timeout covers the wait for an answer alone. Delivery guarantees, high-water
//! marks and the slow joiner are transport behaviour: exercise them on the loopback suite, which
//! needs no external service.
//!
//! One split does not survive, and cannot while the crate mounts by name. A responder's real
//! subscriber is deliberately no [`BatchSubscriber`](ruststream::BatchSubscriber), but a mount
//! site names only a string here, so nothing tells the stand-in which pattern a subscription
//! belongs to, and `.batch(..)` on a request-reply mount compiles in a test where production
//! rejects it. The publish side has no such gap, because there the policy at the mount site names
//! the pattern.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedZmqTestBroker, ZmqTestBroker};
pub use publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
pub use subscriber::{ZmqTestMessage, ZmqTestSubscriber};
