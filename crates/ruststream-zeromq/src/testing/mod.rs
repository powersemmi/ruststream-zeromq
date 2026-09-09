//! In-process test support, behind the `testing` feature.
//!
//! [`ZmqTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and does
//! not simulate broker-specific semantics (dead-letter policies, credit, redelivery timing);
//! those are verified end to end against a real broker.
//!
//! Settlement answers what the transport answers. `ZeroMQ` settles nothing, so a delivery from
//! this stand-in returns [`AckError::Unsupported`](ruststream::AckError::Unsupported) from `ack`
//! and from `nack`, and never comes back, exactly as a delivery over a socket does. A handler
//! that settles by retrying fails its test here instead of losing its message after deployment.
//! Cover redelivery on a broker that has it.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedZmqTestBroker, ZmqTestBroker, ZmqTestPublish, ZmqTestPublisher};
pub use subscriber::{ZmqTestMessage, ZmqTestSubscriber};
