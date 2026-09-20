//! [`ZmqFanout`]: the PUB/SUB pattern - broadcast with prefix filtering by name.
//!
//! Honest scope, straight from the protocol: a subscriber that connects after a publisher has
//! started misses what was sent before it arrived (the slow joiner), and a message published
//! with no matching subscriber is dropped silently.

/// The publish policy of this form, under the name a mount site writes.
pub use self::ZmqFanoutPublish as Publish;

/// The imports a routes file on the PUB/SUB fan-out writes, in one glob: the framework's prelude,
/// the shared [`ZmqEndpoint`], the descriptor [`ZmqFanout`], and its publish policy as
/// [`Publish`].
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::fanout::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Event {
///     id: u64,
/// }
///
/// #[subscriber("events")]
/// async fn handle(event: &Event) -> HandlerOutcome {
///     let _ = event.id;
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("watcher", "0.1.0")).with_broker(
///         ZmqFanout::new(ZmqEndpoint::connect("tcp://ml:5556")),
///         |b| {
///             b.include(handle);
///         },
///     )
/// }
/// ```
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use crate::endpoint::ZmqEndpoint;

    // `Publish` is the mount-site vocabulary, and it is why this glob belongs in a routes file
    // rather than a handler one: a handler imports the framework prelude alone and bounds its
    // injected publisher with a broker capability trait, so the two names never meet.
    pub use super::{Publish, ZmqFanout};
}

use std::future::{Future, ready};
use std::sync::Arc;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer,
    OutgoingMessage, PairError, PublishPolicy, Publisher, ServerSpec, Subscribe, Take,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use zeromq::prelude::*;
use zeromq::{PubSocket, SubSocket};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
use crate::common::{DriverHandle, Lifecycle, SharedLifecycle, send_with_retry};
use crate::endpoint::ZmqEndpoint;
use crate::error::ZmqError;
use crate::message::ZmqMessage;
use crate::queue::ZmqSubscriber;
use crate::wire;

/// The PUB/SUB fan-out: each message reaches every subscriber whose name prefix matches.
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::{ZmqEndpoint, ZmqFanout};
///
/// let publisher_side = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556"));
/// let subscriber_side = ZmqFanout::new(ZmqEndpoint::connect("tcp://events:5556"));
/// # let _ = (publisher_side, subscriber_side);
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct ZmqFanout {
    endpoint: ZmqEndpoint,
    cell: Arc<OnceCell<SharedLifecycle>>,
}

impl ZmqFanout {
    /// Records the endpoint. No I/O.
    pub fn new(endpoint: ZmqEndpoint) -> Self {
        Self {
            endpoint,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// A publisher sharing this fan-out's state; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> ZmqFanoutPublisher {
        ZmqFanoutPublisher {
            cell: Arc::clone(&self.cell),
            socket: Arc::new(Mutex::new(None)),
        }
    }
}

impl Broker for ZmqFanout {
    type Error = ZmqError;
    type Connected = ConnectedZmqFanout;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let lifecycle = self
            .cell
            .get_or_try_init(async || {
                self.endpoint.validate()?;
                Ok::<_, ZmqError>(Arc::new(Lifecycle::new(self.endpoint.clone())))
            })
            .await?
            .clone();
        Ok(ConnectedZmqFanout {
            lifecycle,
            cell: self.cell,
        })
    }
}

impl DescribeServer for ZmqFanout {
    fn describe_server(&self) -> ServerSpec {
        self.endpoint.server_spec()
    }
}

/// The connected form of [`ZmqFanout`].
#[derive(Debug)]
pub struct ConnectedZmqFanout {
    lifecycle: SharedLifecycle,
    cell: Arc<OnceCell<SharedLifecycle>>,
}

impl ConnectedZmqFanout {
    /// The address a local subscription resolved by binding (useful with an ephemeral
    /// `tcp://...:0` endpoint); `None` until a subscription has bound.
    #[must_use]
    pub fn bound_address(&self) -> Option<String> {
        self.lifecycle.resolved.get().cloned()
    }

    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqFanoutPublisher {
        ZmqFanoutPublisher {
            cell: Arc::clone(&self.cell),
            socket: Arc::new(Mutex::new(None)),
        }
    }
}

impl ConnectedBroker for ConnectedZmqFanout {
    type Error = ZmqError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.lifecycle
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedZmqFanout {
    type Subscriber = ZmqSubscriber;

    /// The subscribe name is the address, because the two ends of this pattern are the same
    /// broker: the PUB socket a registration publishes through is attached to the endpoint the
    /// SUB socket subscribed on, and a name is a prefix of itself, so a publish by this process
    /// reaches its own subscriber. That is what a retry copy needs.
    ///
    /// The pattern's own scope applies to the copy as it does to any other message: every
    /// subscription whose prefix matches receives it, and a publisher whose filter table has not
    /// propagated yet drops it (the slow joiner).
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.lifecycle.ensure_open()?;
        let mut socket = SubSocket::new();
        self.lifecycle.attach_receiver(&mut socket).await?;
        // The name frame doubles as the subscription prefix; filtering happens on the
        // publisher side, per the protocol.
        socket
            .subscribe(name)
            .await
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                match socket.recv().await {
                    Ok(message) => {
                        let item =
                            wire::decode(message).map(|(name, headers, payload)| ZmqMessage {
                                name,
                                headers,
                                payload,
                            });
                        if tx.send(item).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        if tx.send(Err(ZmqError::Receive(err.to_string()))).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Ok(ZmqSubscriber::from_parts(
            name.to_owned(),
            rx,
            DriverHandle { task },
        ))
    }
}

/// Publishes to the fan-out over a lazily attached PUB socket.
///
/// A message with no matching subscriber is dropped silently - that is the pattern's
/// contract, not an error.
#[derive(Clone)]
pub struct ZmqFanoutPublisher {
    cell: Arc<OnceCell<SharedLifecycle>>,
    socket: Arc<Mutex<Option<PubSocket>>>,
}

impl std::fmt::Debug for ZmqFanoutPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqFanoutPublisher").finish_non_exhaustive()
    }
}

impl Publisher for ZmqFanoutPublisher {
    /// A frame owns its bytes: the payload becomes the message's third frame and the socket
    /// keeps it until the send completes.
    type Payload = Take;

    type Error = ZmqError;

    /// ZMTP carries no per-message setting: a send takes the frames and nothing else, so there is
    /// nothing for a call site to adjust. See the [crate documentation](crate#per-message-settings).
    type Options = ();

    /// # Cancel safety
    ///
    /// Not cancel-safe. Dropping the future can leave the message half-handed to the socket: the
    /// attach and the send share one guard, and a send that has begun is not undone. Publish from
    /// a task of its own rather than inside a `select!` arm.
    // The socket guard intentionally spans the lazy attach and the send: the socket takes
    // &mut for every operation.
    #[allow(clippy::significant_drop_tightening)]
    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let lifecycle = self.cell.get().ok_or(ZmqError::NotConnected)?;
        lifecycle.ensure_open()?;
        // Framed before the socket is touched: a message that cannot be written costs no attach.
        let frames = wire::encode_to(msg.name(), msg.name(), msg.headers(), msg.payload())?;
        let mut guard = self.socket.lock().await;
        if guard.is_none() {
            let mut socket = PubSocket::new();
            lifecycle.attach_sender(&mut socket).await?;
            *guard = Some(socket);
        }
        let socket = guard.as_mut().expect("just attached");
        // PUB never reports "no peers": an unmatched message is dropped by design, so the
        // retry helper only smooths transport-level failures.
        send_with_retry(socket, msg.name(), frames).await
    }
}

/// The publish policy for [`ZmqFanoutPublisher`].
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::ZmqFanoutPublish;
///
/// let policy = ZmqFanoutPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct ZmqFanoutPublish;

impl PublishPolicy<ConnectedZmqFanout> for ZmqFanoutPublish {
    type Live = ZmqFanoutPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqFanout,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The destination reaches the binding, because on this pattern it is both frame 0 of every
    /// message on the channel and the prefix a SUB peer subscribes with to receive it.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::PubSub, channel)
    }
}

impl DefaultPublish for ConnectedZmqFanout {
    type Policy = ZmqFanoutPublish;
}
