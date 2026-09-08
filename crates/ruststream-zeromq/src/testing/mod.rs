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
//! What has no counterpart in a channel is not imitated. There is no peer to connect, so a
//! publish that a real PUSH socket would fail after its retry window is recorded and dropped here,
//! and a request timeout covers the wait for an answer alone. Delivery guarantees, high-water
//! marks, the slow joiner and settlement (`ZeroMQ` acknowledges nothing) are transport behaviour:
//! exercise them on the loopback suite, which needs no external service.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedZmqTestBroker, ZmqTestBroker};
pub use publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
pub use subscriber::{ZmqTestMessage, ZmqTestSubscriber};
