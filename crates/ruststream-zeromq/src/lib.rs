//! `ZeroMQ` transport implementation of the `RustStream` broker contract, for bridging to
//! non-Rust peers.
//!
//! Unlike the other `RustStream` broker crates, this one has no server in the middle: which
//! side listens is a deployment decision, stated explicitly on the [`ZmqEndpoint`]. Three
//! socket patterns cover three messaging shapes, over the pure-Rust
//! [`zeromq`](https://docs.rs/zeromq) implementation (TCP and IPC transports):
//!
//! - [`ZmqQueue`] - PUSH/PULL: competing consumers, round-robin.
//! - [`ZmqFanout`] - PUB/SUB: broadcast, prefix filtering by name.
//! - [`ZmqRpc`] - DEALER/ROUTER: request and reply.
//!
//! A receive yields one multipart message, so the two one-way patterns serve a `&[T]` handler by
//! assembling its batches on the client, to the size the mount site names. The request-reply form
//! deliberately does not - see [`ZmqRpcSubscriber`] for why a batch of requests cannot be
//! answered.
//!
//! The frame layout is part of the public contract, because the peer on the other side
//! composes messages by hand: frame 0 is the name (also the subscription prefix for the
//! fan-out pattern), frame 1 the headers (`"name: value"` lines; may be empty), frame 2 the
//! payload. A Python peer sends
//! `socket.send_multipart([b"orders", b"", payload])`.
//!
//! Only [`ZmqFanout`] filters on that name; [`ZmqQueue`] and [`ZmqRpc`] hand a subscription every
//! message its socket receives, whatever frame 0 says. A reply on the request-reply pattern is
//! framed differently: frame 0 holds the literal `reply`, behind the ROUTER identity frame that
//! addresses the peer that asked.
//!
//! Scope and limits: delivery is at most once and there is no durability, so acknowledgement
//! is reported as unsupported rather than emulated; a subscriber that connects after a
//! publisher has started misses what was sent before it arrived; the implementation has no
//! encryption layer, so it is for trusted networks or for use inside an existing tunnel; and
//! it exposes no high-water-mark configuration - a slow reader exerts raw TCP back-pressure
//! on senders, except in the fan-out pattern, which drops unmatched messages.
//!
//! # Per-message settings
//!
//! There are none. A ZMTP send takes the frames and nothing else - no priority, no expiry, no
//! ordering key - so every publisher here declares `Options = ()` and no publish builder step
//! comes from this crate. A handler body therefore imports `ruststream::prelude::*` alone and
//! bounds an injected publisher with the capability it needs, with nothing of this crate in its
//! signature.
//!
//! Two things that look like settings are not. Which peer a reply reaches is a destination, and a
//! publish transform declaring `Destination = Names` supplies it per delivery. How long a send
//! waits for the ZMTP handshake is a constant of this crate, the same for every message.
//!
//! # Retries
//!
//! Nothing settles a delivery, so a handler asking for `retry_after` is served only by the copy
//! the runtime publishes once the delay is over, through the publisher a registration binds with
//! `.out_retry(policy)`. The one-way patterns report the subscription name as the address for that
//! copy, so binding one works. A responder reports none - the reply publisher routes to a peer
//! identity, not to a name - and a registration that binds a retry over a responder is refused at
//! startup rather than publishing copies into nothing.

#![forbid(unsafe_code)]

#[cfg(feature = "asyncapi")]
mod bindings;
mod common;
mod endpoint;
mod error;
mod message;
pub mod prelude;
mod wire;

// Public, not just re-export sources: each form carries its own prelude.
pub mod fanout;
pub mod queue;
pub mod rpc;
#[cfg(feature = "testing")]
pub mod testing;

pub use endpoint::ZmqEndpoint;
pub use error::ZmqError;
pub use fanout::{ConnectedZmqFanout, ZmqFanout, ZmqFanoutPublish, ZmqFanoutPublisher};
pub use message::ZmqMessage;
pub use queue::{ConnectedZmqQueue, ZmqQueue, ZmqQueuePublish, ZmqQueuePublisher, ZmqSubscriber};
pub use rpc::{ConnectedZmqRpc, ZmqRpc, ZmqRpcPublish, ZmqRpcPublisher, ZmqRpcSubscriber};
