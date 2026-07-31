//! `ZeroMQ` transport implementation of the `RustStream` broker contract, for bridging to
//! non-Rust peers.
//!
//! Unlike every other broker crate, this one has no server in the middle: which side listens
//! is a deployment decision, stated explicitly on the [`ZmqEndpoint`]. Three socket patterns
//! cover three messaging shapes, over the pure-Rust
//! [`zeromq`](https://docs.rs/zeromq) implementation (TCP and IPC transports):
//!
//! - [`ZmqQueue`] - PUSH/PULL: competing consumers, round-robin.
//! - [`ZmqFanout`] - PUB/SUB: broadcast, prefix filtering by name.
//! - [`ZmqRpc`] - DEALER/ROUTER: request and reply.
//!
//! The frame layout is part of the public contract, because the peer on the other side
//! composes messages by hand: frame 0 is the name (also the subscription prefix for the
//! fan-out pattern), frame 1 the headers (`"name: value"` lines; may be empty), frame 2 the
//! payload. A Python peer sends
//! `socket.send_multipart([b"orders", b"", payload])`.
//!
//! Honest scope, stated here rather than discovered: delivery is at most once and there is
//! no durability, so acknowledgement is reported as unsupported rather than emulated; a
//! subscriber that connects after a publisher has started misses what was sent before it
//! arrived; the implementation has no encryption layer, so it is for trusted networks or for
//! use inside an existing tunnel; and it exposes no high-water-mark configuration - a slow
//! reader exerts raw TCP back-pressure on senders, except the fan-out pattern, which drops
//! unmatched messages by design.

#![forbid(unsafe_code)]

mod common;
mod endpoint;
mod error;
mod fanout;
mod message;
mod queue;
mod rpc;
#[cfg(feature = "testing")]
pub mod testing;
mod wire;

pub use endpoint::ZmqEndpoint;
pub use error::ZmqError;
pub use fanout::{ConnectedZmqFanout, ZmqFanout, ZmqFanoutPublish, ZmqFanoutPublisher};
pub use message::ZmqMessage;
pub use queue::{ConnectedZmqQueue, ZmqQueue, ZmqQueuePublish, ZmqQueuePublisher, ZmqSubscriber};
pub use rpc::{ConnectedZmqRpc, ZmqRpc, ZmqRpcPublish, ZmqRpcPublisher};
