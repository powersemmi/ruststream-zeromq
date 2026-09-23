//! In-process test support, behind the `testing` feature.
//!
//! [`ZmqTestBroker`] is a stand-in transport that reproduces the crate's routing in memory - no
//! server, no sockets - and implements [`TestableBroker`](ruststream::testing::TestableBroker) on
//! its connected form, so application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness and the framework's conformance suite runs
//! against it.
//!
//! There is one stand per pattern, chosen by the constructor: `ZmqTestBroker::queue()`,
//! `ZmqTestBroker::fanout()`, `ZmqTestBroker::rpc()`. Each answers what its own broker answers,
//! so what compiles and starts under the harness compiles and starts against the socket:
//!
//! * a stand takes the side of the endpoint its broker takes: a constructor gives the bind side,
//!   and `.dialing()` the side that dials;
//! * a stand of the bind side takes one subscription, as the endpoint it stands for is one socket,
//!   and refuses a second in the socket's words. On the queue the stand is then that
//!   subscription's queue: its publisher sends under the subscription's name and refuses any other
//!   in the socket's words;
//! * a stand of the side that dials takes as many subscriptions as the peer serves, and a message
//!   the harness injects arrives the way the peer sends it: on the queue to one of the workers,
//!   taken in turn, so two workers dialing one ventilator do not both run. Its one-way
//!   subscriptions address no retry copy, and once a subscription has opened its publisher
//!   refuses every publish in the socket's words, because the peer takes nothing;
//! * the fan-out filters by name prefix, the protocol's own rule, and drops what nothing matches;
//! * the request-reply exchange correlates an answer to its request and routes it back to the
//!   caller that asked, and only to that caller. Its subscriber is no
//!   [`BatchSubscriber`](ruststream::BatchSubscriber) and its subscriptions address no retry
//!   copies, both of which the real responder also withholds.
//!
//! Each production policy pairs against its own stand and no other - [`ZmqQueuePublish`](crate::ZmqQueuePublish) against
//! the queue, [`ZmqFanoutPublish`](crate::ZmqFanoutPublish) against the fan-out, [`ZmqRpcPublish`](crate::ZmqRpcPublish) against the responder -
//! so a mount site keeps the policy the service ships, and mounting the wrong one is the compile
//! error it is in production. A mount that names no reply policy takes the default its own
//! pattern names.
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
//! Settlement answers what the transport answers. `ZeroMQ` settles nothing, so a delivery from a
//! stand returns [`AckError::Unsupported`](ruststream::AckError::Unsupported) from `ack` and from
//! `nack`, and never comes back, exactly as a delivery over a socket does. A handler that settles
//! by retrying fails its test here instead of losing its message after deployment. Cover
//! redelivery on a broker that has it.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedZmqTestBroker, Fanout, Queue, Rpc, TestPattern, ZmqTestBroker};
pub use publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
pub use subscriber::{ZmqTestMessage, ZmqTestRpcSubscriber, ZmqTestSubscriber};
