#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

#[cfg(feature = "asyncapi")]
mod bindings;
mod common;
mod endpoint;
mod error;
#[cfg(feature = "testing")]
mod in_process;
mod message;
pub mod prelude;
mod wire;

// Public, not just re-export sources: each form carries its own prelude.
pub mod fanout;
pub mod queue;
pub mod rpc;

pub use endpoint::{Bind, Connect, EndpointRole, ZmqEndpoint};
pub use error::ZmqError;
pub use fanout::{ConnectedZmqFanout, ZmqFanout, ZmqFanoutPublish, ZmqFanoutPublisher};
pub use message::ZmqMessage;
pub use queue::{ConnectedZmqQueue, ZmqQueue, ZmqQueuePublish, ZmqQueuePublisher, ZmqSubscriber};
pub use rpc::{ConnectedZmqRpc, ZmqRpc, ZmqRpcPublish, ZmqRpcPublisher, ZmqRpcSubscriber};
