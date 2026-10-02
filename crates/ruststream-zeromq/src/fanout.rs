//! [`ZmqFanout`]: the PUB/SUB pattern - broadcast with prefix filtering by name.
//!
//! Honest scope, straight from the protocol: a subscriber that connects after a publisher has
//! started misses what was sent before it arrived (the slow joiner), and a message published
//! with no matching subscriber is dropped silently. A publisher that reaches a subscription of
//! its own service reads that subscription's filter before its first send, so it loses nothing
//! to the slow joiner.

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
///     println!("event {}", event.id);
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
use tokio::time::timeout;
use zeromq::prelude::*;
use zeromq::{PubSocket, SubSocket, XPubSocket, ZmqError as WireError};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
#[cfg(feature = "testing")]
use crate::common::DeliveryReceiver;
use crate::common::{
    DEFAULT_READ_AHEAD, DriverHandle, Lifecycle, SEND_RETRY_WINDOW, SendTarget, Sender,
    SharedLifecycle, attach_to, delivery_channel, dials_the_sender, send_with_retry,
};
use crate::endpoint::{Bind, Connect, Endpoint, EndpointRole, ZmqEndpoint};
use crate::error::{ZmqError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::Pick;
use crate::queue::ZmqSubscriber;
use crate::wire;

// A production publisher holds the socket it attached and nothing beside it.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Sender<PubSide>>() == size_of::<PubSide>());

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
/// A watcher that the order services dial, taking every message under `orders`, `orders.created`
/// and `orders.cancelled` included:
///
/// ```
/// use ruststream_zeromq::fanout::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct OrderEvent {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn watch(event: &OrderEvent, ctx: &mut Context<'_>) -> HandlerOutcome {
///     println!("{} for order {}", ctx.name(), event.id);
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("watcher", "0.1.0")).with_broker(
///         ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")),
///         |b| {
///             b.include(watch);
///         },
///     )
/// }
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
    /// A price feed that broadcasts a tick for each trade to every watcher that dials it:
    ///
    /// ```
    /// use ruststream_zeromq::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Trade {
    ///     price: f64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "prices")]
    /// struct Tick {
    ///     price: f64,
    /// }
    ///
    /// #[subscriber("trades", publish)]
    /// async fn tick(trade: &Trade) -> Tick {
    ///     Tick { price: trade.price }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let prices = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).bindable();
    ///     let to_prices = prices.bind(ZmqFanoutPublish);
    ///     RustStream::new(AppInfo::new("prices", "0.1.0"))
    ///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
    ///             b.include(tick).out_reply(to_prices);
    ///         })
    ///         .register_broker(prices)
    /// }
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
    /// An archiver that the event publishers dial, and that writes every event to slow storage,
    /// holds 64 events ahead, and the publishers wait beyond that:
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
    /// async fn archive(event: &Event) -> HandlerOutcome {
    ///     println!("archiving event {}", event.id);
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556"))
    ///         .read_ahead(nonzero!(64_usize));
    ///     RustStream::new(AppInfo::new("archiver", "0.1.0")).with_broker(events, |b| {
    ///         b.include(archive);
    ///     })
    /// }
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
        self.connect_with(Lifecycle::new).await
    }
}

impl<Role: EndpointRole> ZmqFanout<Role> {
    /// The connect transition over the transport `lifecycle` builds: the sockets, or the
    /// in-process transport the test harness connects instead.
    pub(crate) async fn connect_with(
        self,
        lifecycle: fn(Endpoint) -> Lifecycle,
    ) -> Result<ConnectedZmqFanout<Role>, ZmqError> {
        let lifecycle = self
            .cell
            .get_or_try_init(async || {
                self.endpoint.validate()?;
                Ok::<_, ZmqError>(Arc::new(lifecycle(self.endpoint.clone())))
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
    #[cfg(feature = "testing")]
    pub(crate) fn lifecycle(&self) -> &Lifecycle {
        &self.lifecycle
    }

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
        // The prefix filter is the transport's in process: it picks the subscriptions whose name
        // prefixes a message's name frame, as a SUB socket's filter does.
        #[cfg(feature = "testing")]
        if let Some(bus) = self.lifecycle.in_process_bus() {
            let slot = self.lifecycle.attach_in_process(name).await?;
            let (rx, registration) = bus.subscribe(name, wire::read_delivery);
            return Ok(ZmqSubscriber::from_parts(
                name.to_owned(),
                DeliveryReceiver::InProcess(rx),
                DriverHandle::InProcess(registration, slot),
            ));
        }
        let lifecycle = Arc::clone(&self.lifecycle);
        let subscription = name.to_owned();
        let (mut socket, slot) = self
            .lifecycle
            .on_runtime(async move {
                let mut socket = SubSocket::new();
                let slot = lifecycle
                    .attach_receiver(&mut socket, &subscription)
                    .await?;
                // The name frame doubles as the subscription prefix; filtering happens on the
                // publisher side, per the protocol.
                socket
                    .subscribe(&subscription)
                    .await
                    .map_err(|e| ZmqError::Receive(e.to_string()))?;
                Ok((socket, slot))
            })
            .await?;

        let (tx, rx) = delivery_channel(self.read_ahead);
        let task = self.lifecycle.spawn(async move {
            loop {
                match socket.recv().await {
                    Ok(message) => {
                        let Some(item) = wire::read_delivery(message) else {
                            continue;
                        };
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
            DriverHandle::Task(task, slot),
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
    /// subscription whose prefix matches receives it. The publisher reads the subscription's
    /// filter before its first send, so the first copy it publishes is not dropped.
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
    socket: Arc<Mutex<Option<Sender<PubSide>>>>,
}

/// The sending socket of a fan-out publisher, by the place it attached to.
enum PubSide {
    /// A peer outside this service: the socket learns its filters as they arrive, and what it
    /// sends before one arrives is dropped, as the pattern drops it.
    Remote(PubSocket),
    /// The listener a subscription of this service bound. The socket read that subscription's
    /// filter before its first send, so nothing it sends is dropped for want of one.
    Loopback(XPubSocket),
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
        // Checked here rather than by the type: whether one broker both subscribes and publishes is
        // decided by the scopes a service mounts, and the peer at the far end is the deployment's,
        // so the refusal comes before the handshake would give it. Checked on every publish, not
        // only on the attach: a subscription that dials after the socket attached turns the
        // publisher's peer into its own. One load of a set-once cell.
        if let Some(subscription) = lifecycle.dialer() {
            return Err(dials_the_sender(name, subscription, "PUB", "fan-out"));
        }
        let mut guard = self.socket.lock().await;
        if guard.is_none() {
            *guard = Some(attach(lifecycle).await?);
        }
        match guard.as_mut().expect("just attached") {
            // PUB never reports "no peers": an unmatched message is dropped by design, so the
            // retry helper only smooths transport-level failures.
            Sender::Socket(PubSide::Remote(socket)) => send_with_retry(socket, name, frames).await,
            Sender::Socket(PubSide::Loopback(socket)) => {
                send_with_retry(socket, name, frames).await
            }
            #[cfg(feature = "testing")]
            Sender::InProcess { bus, local } => {
                let pick = if *local {
                    Pick::Prefix(name)
                } else {
                    Pick::Nobody
                };
                bus.send(name, frames, None, pick);
                Ok(())
            }
        }
    }
}

/// Attaches a PUB socket per the endpoint's side, or its in-process counterpart.
///
/// On the listener a subscription of this service bound, the socket is an XPUB that reads the
/// subscription's filter before it returns: a PUB socket drops a message until the filter of the
/// peer it dialed has arrived, and on the loopback that peer is the subscription a retry copy is
/// published to. The wait is paid once per publisher, at the attach.
async fn attach(lifecycle: &SharedLifecycle) -> Result<Sender<PubSide>, ZmqError> {
    #[cfg(feature = "testing")]
    if let Some(bus) = lifecycle.in_process_bus() {
        return Ok(Sender::InProcess {
            bus: Arc::clone(bus),
            local: lifecycle.local_listener().is_some(),
        });
    }
    let attaching = Arc::clone(lifecycle);
    let socket = lifecycle
        .on_runtime(async move {
            match attaching.send_target() {
                target @ SendTarget::Local(_) => {
                    let address = target.address().to_owned();
                    let mut socket = XPubSocket::new();
                    attach_to(&mut socket, target).await?;
                    await_filter(&mut socket, &address).await?;
                    Ok(PubSide::Loopback(socket))
                }
                target => {
                    let mut socket = PubSocket::new();
                    attach_to(&mut socket, target).await?;
                    Ok(PubSide::Remote(socket))
                }
            }
        })
        .await?;
    Ok(Sender::Socket(socket))
}

/// Reads the subscribe message the SUB socket at `address` sends once the handshake is done,
/// which is what enters its filter into the socket's table.
///
/// The subscription subscribes once, to its own name, and a SUB socket sends its filters to every
/// peer that connects, so the first message from it is the whole table.
async fn await_filter(socket: &mut XPubSocket, address: &str) -> Result<(), ZmqError> {
    timeout(SEND_RETRY_WINDOW, socket.recv())
        .await
        .unwrap_or(Err(WireError::Other("the subscription sent no filter")))
        .map(drop)
        .map_err(|err| ZmqError::Endpoint {
            endpoint: address.to_owned(),
            source: box_err(err),
        })
}

/// The publish policy for [`ZmqFanoutPublisher`].
///
/// # Examples
///
/// An auditor that the order services dial, broadcasting an audit record for each order event on
/// a fan-out of its own:
///
/// ```
/// use ruststream_zeromq::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct OrderEvent {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "audit.orders")]
/// struct AuditRecord {
///     order: u64,
/// }
///
/// #[subscriber("orders", publish)]
/// async fn audit(event: &OrderEvent) -> AuditRecord {
///     AuditRecord { order: event.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let records = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")).bindable();
///     let to_records = records.bind(ZmqFanoutPublish);
///     RustStream::new(AppInfo::new("auditor", "0.1.0"))
///         .with_broker(ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")), |b| {
///             b.include(audit).out_reply(to_records);
///         })
///         .register_broker(records)
/// }
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
