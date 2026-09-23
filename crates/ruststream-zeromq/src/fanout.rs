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
///     // The watcher binds and the publishers dial it, so a retry copy returns to its listener.
///     RustStream::new(AppInfo::new("watcher", "0.1.0")).with_broker(
///         ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
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

use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::lock::Mutex;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer,
    NamedCopies, OutgoingMessage, PairError, PublishPolicy, Publisher, ServerSpec, Subscribe, Take,
};
use tokio::sync::OnceCell;
use zeromq::prelude::*;
use zeromq::{PubSocket, SubSocket};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
use crate::common::{
    DEFAULT_READ_AHEAD, DriverHandle, Lifecycle, SharedLifecycle, delivery_channel,
    dials_the_sender, send_with_retry,
};
use crate::endpoint::{Bind, Connect, Endpoint, EndpointRole, ZmqEndpoint};
use crate::error::ZmqError;
use crate::message::ZmqMessage;
use crate::queue::ZmqSubscriber;
use crate::wire;

/// The PUB/SUB fan-out: each message reaches every subscriber whose name prefix matches.
///
/// `Role` is the side of the endpoint this process takes, fixed by the endpoint's constructor:
/// [`Bind`] for `ZmqEndpoint::bind`, [`Connect`] for `ZmqEndpoint::connect`. It decides where a
/// retry copy goes. A subscription that binds takes a copy this service publishes to its own
/// listener, so a mount site names no destination; one that dials reads from a PUB peer that
/// takes nothing, so a mount site names where its copies go.
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
#[must_use]
pub struct ZmqFanout<Role = Bind> {
    endpoint: Endpoint,
    read_ahead: NonZeroUsize,
    cell: Arc<OnceCell<SharedLifecycle>>,
    role: PhantomData<fn() -> Role>,
}

impl<Role: EndpointRole> ZmqFanout<Role> {
    /// Records the endpoint. No I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_zeromq::{ZmqEndpoint, ZmqFanout};
    ///
    /// let watcher = ZmqFanout::new(ZmqEndpoint::connect("tcp://events:5556"));
    /// # let _ = watcher;
    /// ```
    pub fn new(endpoint: ZmqEndpoint<Role>) -> Self {
        Self {
            endpoint: endpoint.into_inner(),
            read_ahead: DEFAULT_READ_AHEAD,
            cell: Arc::new(OnceCell::new()),
            role: PhantomData,
        }
    }
}

impl<Role> ZmqFanout<Role> {
    /// How many deliveries a subscription reads off its socket ahead of the handler; 1000 unless
    /// set, the receive high-water mark `ZeroMQ` itself gives a socket.
    ///
    /// Past this bound the subscription stops reading, the socket's buffers fill, and the sender
    /// waits: a handler slower than the wire slows the sender down rather than growing this
    /// process's memory.
    ///
    /// On PUB/SUB the sender that is held back is the PUB socket, and it waits for its slowest
    /// matching subscriber: the `zeromq` implementation writes a message to each matching peer in
    /// turn.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::nonzero;
    /// use ruststream_zeromq::{ZmqFanout, ZmqEndpoint};
    ///
    /// let subscriber_side = ZmqFanout::new(ZmqEndpoint::connect("tcp://events:5556"))
    ///     .read_ahead(nonzero!(64_usize));
    /// # let _ = subscriber_side;
    /// ```
    pub const fn read_ahead(mut self, read_ahead: NonZeroUsize) -> Self {
        self.read_ahead = read_ahead;
        self
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

// Written out rather than derived: `Role` is a type-level tag, and a derive would demand of it
// what is only asked of the fields.
impl<Role> Clone for ZmqFanout<Role> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            read_ahead: self.read_ahead,
            cell: Arc::clone(&self.cell),
            role: PhantomData,
        }
    }
}

impl<Role> fmt::Debug for ZmqFanout<Role> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqFanout")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl<Role: EndpointRole> Broker for ZmqFanout<Role> {
    type Error = ZmqError;
    type Connected = ConnectedZmqFanout<Role>;

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
            read_ahead: self.read_ahead,
            cell: self.cell,
            role: PhantomData,
        })
    }
}

impl<Role: EndpointRole> DescribeServer for ZmqFanout<Role> {
    fn describe_server(&self) -> ServerSpec {
        self.endpoint.server_spec()
    }
}

/// The connected form of [`ZmqFanout`].
pub struct ConnectedZmqFanout<Role = Bind> {
    lifecycle: SharedLifecycle,
    /// How far this subscription reads ahead, from the descriptor this form was connected from.
    read_ahead: NonZeroUsize,
    cell: Arc<OnceCell<SharedLifecycle>>,
    role: PhantomData<fn() -> Role>,
}

impl<Role> fmt::Debug for ConnectedZmqFanout<Role> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedZmqFanout")
            .field("lifecycle", &self.lifecycle)
            .finish_non_exhaustive()
    }
}

impl<Role> ConnectedZmqFanout<Role> {
    /// The address a local subscription resolved by binding (useful with an ephemeral
    /// `tcp://...:0` endpoint); `None` until a subscription has bound.
    #[must_use]
    pub fn bound_address(&self) -> Option<String> {
        self.lifecycle.bound_address()
    }

    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqFanoutPublisher {
        ZmqFanoutPublisher {
            cell: Arc::clone(&self.cell),
            socket: Arc::new(Mutex::new(None)),
        }
    }

    /// Opens a SUB subscription on the endpoint, bound or dialed per the side, filtering on the
    /// name as a prefix.
    async fn open(&self, name: &str) -> Result<ZmqSubscriber, ZmqError> {
        self.lifecycle.ensure_open()?;
        let mut socket = SubSocket::new();
        self.lifecycle.attach_receiver(&mut socket, name).await?;
        // The name frame doubles as the subscription prefix; filtering happens on the
        // publisher side, per the protocol.
        socket
            .subscribe(name)
            .await
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        let (tx, rx) = delivery_channel(self.read_ahead);
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
                        if tx.send(item).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        if tx
                            .send(Err(ZmqError::Receive(err.to_string())))
                            .await
                            .is_err()
                        {
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

impl<Role: EndpointRole> ConnectedBroker for ConnectedZmqFanout<Role> {
    type Error = ZmqError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.lifecycle.closed.store(true, Ordering::Release);
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedZmqFanout<Bind> {
    type Subscriber = ZmqSubscriber;

    /// The subscribe name is the address, because the two ends of this pattern are the same
    /// broker: the subscription binds the endpoint, the PUB socket a registration publishes
    /// through dials that listener, and a name is a prefix of itself, so a publish by this
    /// process reaches its own subscriber. That is what a retry copy needs.
    ///
    /// The pattern's own scope applies to the copy as it does to any other message: every
    /// subscription whose prefix matches receives it, and a publisher whose filter table has not
    /// propagated yet drops it (the slow joiner).
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.open(name).await
    }
}

impl Subscribe for ConnectedZmqFanout<Connect> {
    type Subscriber = ZmqSubscriber;

    /// A subscription that dials reads from the PUB peer at the endpoint, which publishes and
    /// takes nothing, so a copy published back to it is refused by the handshake. The
    /// subscription therefore addresses no copy, and every registration names where its copies
    /// go: `.out_retry(policy).to("name")` over a broker that reaches a consumer, or a transform
    /// that names one per delivery. A registration that names neither is refused before the
    /// subscription opens, and a `.build()` chain that names neither does not compile.
    type Copies = NamedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.open(name).await
    }
}

/// Publishes to the fan-out over a lazily attached PUB socket.
///
/// A message with no matching subscriber is dropped silently - that is the pattern's
/// contract, not an error. On an endpoint a subscription of this service dials, the peer there
/// only publishes, so every publish returns [`ZmqError::Send`] naming that subscription.
#[derive(Clone)]
pub struct ZmqFanoutPublisher {
    cell: Arc<OnceCell<SharedLifecycle>>,
    // The socket guard is the `futures` mutex rather than tokio's: the socket needs `&mut` per send,
    // so something must serialise, and this one's uncontended lock and unlock are a pair of atomics
    // where tokio's semaphore also takes its waiter list. It costs 1.8 of the 9 points a publish
    // spends over a raw socket loop (#28).
    socket: Arc<Mutex<Option<PubSocket>>>,
}

impl fmt::Debug for ZmqFanoutPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        let (name, payload, headers) = msg.into_parts();
        let frames = wire::encode_to(name, name, &headers, payload.freeze())?;
        let mut guard = self.socket.lock().await;
        if guard.is_none() {
            // Checked here rather than by the type: whether one broker both subscribes and
            // publishes is decided by the scopes a service mounts, and the peer at the far end is
            // the deployment's, so the refusal comes before the handshake would give it.
            if let Some(subscription) = lifecycle.dialer() {
                return Err(dials_the_sender(name, subscription, "PUB", "fan-out"));
            }
            let mut socket = PubSocket::new();
            lifecycle.attach_sender(&mut socket).await?;
            *guard = Some(socket);
        }
        let socket = guard.as_mut().expect("just attached");
        // PUB never reports "no peers": an unmatched message is dropped by design, so the
        // retry helper only smooths transport-level failures.
        send_with_retry(socket, name, frames).await
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

impl<Role: EndpointRole> PublishPolicy<ConnectedZmqFanout<Role>> for ZmqFanoutPublish {
    type Live = ZmqFanoutPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqFanout<Role>,
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

impl<Role: EndpointRole> DefaultPublish for ConnectedZmqFanout<Role> {
    type Policy = ZmqFanoutPublish;
}
