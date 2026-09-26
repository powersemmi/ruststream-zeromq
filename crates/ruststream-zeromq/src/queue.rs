//! [`ZmqQueue`]: the PUSH/PULL pattern - competing consumers, round-robin.

/// The publish policy of this form, under the name a mount site writes.
pub use self::ZmqQueuePublish as Publish;

/// The imports a routes file on the PUSH/PULL queue writes, in one glob: the framework's prelude,
/// the shared [`ZmqEndpoint`], the descriptor [`ZmqQueue`], and its publish policy as [`Publish`].
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// // The result queue belongs to the message, so the type names it and the clause stays bare.
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", publish)]
/// async fn handle(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     // The results travel on a queue of their own: the one the jobs arrive on would hand a
///     // result back to `handle` as its next job.
///     let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
///     let to_results = results.bind(Publish);
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(handle).out_reply(to_results);
///         })
///         .with_broker(results, |_b| {})
/// }
/// ```
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use crate::endpoint::ZmqEndpoint;

    // `Publish` is the mount-site vocabulary, and it is why this glob belongs in a routes file
    // rather than a handler one: a handler imports the framework prelude alone and bounds its
    // injected publisher with a broker capability trait, so the two names never meet.
    pub use super::{Publish, ZmqQueue};
}

use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::Stream;
use futures::lock::Mutex;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    AddressedCopies, BatchSubscriber, Broker, BufferedSubscriber, BytesMut, ConnectedBroker,
    DefaultPublish, DescribeServer, NamedCopies, OutgoingMessage, PairError, PublishPolicy,
    Publisher, ServerSpec, Subscribe, Subscriber, Take,
};
use tokio::sync::OnceCell;
use zeromq::prelude::*;
use zeromq::{PullSocket, PushSocket};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
use crate::common::{
    BATCH_MAX_WAIT, DEFAULT_READ_AHEAD, DeliveryReceiver, DriverHandle, Lifecycle, Outbox, Sender,
    SharedLifecycle, WireSubscriber, delivery_channel, dials_the_sender, returns_to_subscription,
};
use crate::endpoint::{Bind, Connect, Endpoint, EndpointRole, ZmqEndpoint};
use crate::error::ZmqError;
#[cfg(feature = "testing")]
use crate::in_process::Pick;
use crate::message::ZmqMessage;
use crate::wire;

// A production publisher holds the outbox of the socket it attached and nothing beside it.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Sender<Outbox<PushSocket>>>() == size_of::<Outbox<PushSocket>>());

/// The PUSH/PULL queue: each message reaches one of the competing consumers.
///
/// `Role` is the side of the endpoint this process takes, fixed by the endpoint's constructor:
/// [`Bind`] for `ZmqEndpoint::bind`, [`Connect`] for `ZmqEndpoint::connect`. It decides where a
/// retry copy goes. A subscription that binds takes a copy published to its own listener, so a
/// mount site names no destination; one that dials reads from a PUSH peer that takes nothing, so
/// a mount site names where its copies go.
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::{ZmqEndpoint, ZmqQueue};
///
/// let consumer = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555"));
/// let producer = ZmqQueue::new(ZmqEndpoint::connect("tcp://worker:5555"));
/// # let _ = (consumer, producer);
/// ```
#[must_use]
pub struct ZmqQueue<Role = Bind> {
    endpoint: Endpoint,
    read_ahead: NonZeroUsize,
    cell: Arc<OnceCell<SharedLifecycle>>,
    role: PhantomData<fn() -> Role>,
}

impl<Role: EndpointRole> ZmqQueue<Role> {
    /// Records the endpoint. No I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_zeromq::{ZmqEndpoint, ZmqQueue};
    ///
    /// let consumer = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555"));
    /// # let _ = consumer;
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

impl<Role> ZmqQueue<Role> {
    /// How many deliveries a subscription reads off its socket ahead of the handler; 1000 unless
    /// set, the receive high-water mark `ZeroMQ` itself gives a socket.
    ///
    /// Past this bound the subscription stops reading, the socket's buffers fill, and the sender
    /// waits: a handler slower than the wire slows the sender down rather than growing this
    /// process's memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::nonzero;
    /// use ruststream_zeromq::{ZmqQueue, ZmqEndpoint};
    ///
    /// let subscriber_side = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555"))
    ///     .read_ahead(nonzero!(64_usize));
    /// # let _ = subscriber_side;
    /// ```
    pub const fn read_ahead(mut self, read_ahead: NonZeroUsize) -> Self {
        self.read_ahead = read_ahead;
        self
    }

    /// A publisher sharing this queue's state; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> ZmqQueuePublisher {
        ZmqQueuePublisher {
            cell: Arc::clone(&self.cell),
            push: Arc::new(Mutex::new(None)),
        }
    }
}

// Written out rather than derived: `Role` is a type-level tag, and a derive would demand of it
// what is only asked of the fields.
impl<Role> Clone for ZmqQueue<Role> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            read_ahead: self.read_ahead,
            cell: Arc::clone(&self.cell),
            role: PhantomData,
        }
    }
}

impl<Role> fmt::Debug for ZmqQueue<Role> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqQueue")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl<Role: EndpointRole> Broker for ZmqQueue<Role> {
    type Error = ZmqError;
    type Connected = ConnectedZmqQueue<Role>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::new).await
    }
}

impl<Role: EndpointRole> ZmqQueue<Role> {
    /// The connect transition over the transport `lifecycle` builds: the sockets, or the
    /// in-process transport the test harness connects instead.
    pub(crate) async fn connect_with(
        self,
        lifecycle: fn(Endpoint) -> Lifecycle,
    ) -> Result<ConnectedZmqQueue<Role>, ZmqError> {
        let lifecycle = self
            .cell
            .get_or_try_init(async || {
                self.endpoint.validate()?;
                Ok::<_, ZmqError>(Arc::new(lifecycle(self.endpoint.clone())))
            })
            .await?
            .clone();
        Ok(ConnectedZmqQueue {
            lifecycle,
            read_ahead: self.read_ahead,
            cell: self.cell,
            role: PhantomData,
        })
    }
}

impl<Role: EndpointRole> DescribeServer for ZmqQueue<Role> {
    fn describe_server(&self) -> ServerSpec {
        self.endpoint.server_spec()
    }
}

/// The connected form of [`ZmqQueue`]; sockets attach lazily per subscription and publisher.
pub struct ConnectedZmqQueue<Role = Bind> {
    lifecycle: SharedLifecycle,
    /// How far this subscription reads ahead, from the descriptor this form was connected from.
    read_ahead: NonZeroUsize,
    cell: Arc<OnceCell<SharedLifecycle>>,
    role: PhantomData<fn() -> Role>,
}

impl<Role> fmt::Debug for ConnectedZmqQueue<Role> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedZmqQueue")
            .field("lifecycle", &self.lifecycle)
            .finish_non_exhaustive()
    }
}

impl<Role> ConnectedZmqQueue<Role> {
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
    pub fn publisher(&self) -> ZmqQueuePublisher {
        ZmqQueuePublisher {
            cell: Arc::clone(&self.cell),
            push: Arc::new(Mutex::new(None)),
        }
    }

    /// Opens a PULL subscription on the endpoint, bound or dialed per the side.
    async fn open(&self, name: &str) -> Result<ZmqSubscriber, ZmqError> {
        self.lifecycle.ensure_open()?;
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
        let mut socket = PullSocket::new();
        let slot = self.lifecycle.attach_receiver(&mut socket, name).await?;

        let (tx, rx) = delivery_channel(self.read_ahead);
        let task = tokio::spawn(async move {
            // Held for as long as the socket is open: the endpoint is free again once it closes.
            let _slot = slot;
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
            DriverHandle::Task(task),
        ))
    }
}

impl<Role: EndpointRole> ConnectedBroker for ConnectedZmqQueue<Role> {
    type Error = ZmqError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.lifecycle.closed.store(true, Ordering::Release);
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedZmqQueue<Bind> {
    type Subscriber = ZmqSubscriber;

    /// The subscribe name is the address: the subscription binds the endpoint, a PUSH socket of
    /// this service dials that listener, and a publish under the subscription's name reaches it.
    /// The runtime publishes a retry copy there, and nothing has to be written at the mount site.
    ///
    /// This is the whole retry path on `ZeroMQ`. Nothing settles a delivery, so a handler asking
    /// for `retry_after` is served only by the deferred copy the runtime publishes here.
    /// Competing consumers still compete for that copy, so the worker that retries is not
    /// necessarily the one that asked.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.open(name).await
    }
}

impl Subscribe for ConnectedZmqQueue<Connect> {
    type Subscriber = ZmqSubscriber;

    /// A subscription that dials reads from the PUSH peer at the endpoint, a ventilator that
    /// sends and takes nothing, so a copy published back to it is refused by the handshake. The
    /// subscription therefore addresses no copy, and every registration names where its copies
    /// go: `.out_retry(policy).to("name")` over a broker that reaches a consumer, or a transform
    /// that names one per delivery. A registration that names neither is refused before the
    /// subscription opens, and a `.build()` chain that names neither does not compile.
    type Copies = NamedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.open(name).await
    }
}

/// A subscription on one of the one-way patterns - PUSH/PULL or PUB/SUB - yielding
/// [`ZmqMessage`]s singly or in batches.
pub struct ZmqSubscriber {
    name: String,
    inner: BufferedSubscriber<WireSubscriber>,
}

impl fmt::Debug for ZmqSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqSubscriber")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl ZmqSubscriber {
    pub(crate) fn from_parts(name: String, rx: DeliveryReceiver, driver: DriverHandle) -> Self {
        Self {
            name,
            inner: BufferedSubscriber::new(WireSubscriber {
                rx,
                _driver: driver,
            })
            .max_wait(BATCH_MAX_WAIT),
        }
    }
}

impl Subscriber for ZmqSubscriber {
    type Message = ZmqMessage;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<ZmqMessage, ZmqError>> + Send + '_ {
        self.inner.stream()
    }
}

/// The transport has no batches of its own - a receive yields one multipart message - so they are
/// assembled on the client, to the size the registration named. The deadline that closes a partial
/// batch is the crate's own (20 ms); the size is not, it arrives per subscription.
impl BatchSubscriber for ZmqSubscriber {
    type Batch = Vec<ZmqMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, ZmqError>> + Send + '_ {
        self.inner.batches(size)
    }
}

/// Publishes into the queue over a lazily attached PUSH socket.
///
/// On an endpoint a subscription of this service bound, the socket dials that subscription, so a
/// publish reaches it and nothing else: one under the subscription's own name (a retry copy, a
/// job the service feeds itself) is sent, and one under any other name returns
/// [`ZmqError::Send`] rather than arriving at the subscription as its next delivery. On an
/// endpoint a subscription of this service dials, the peer there only pushes, so every publish
/// returns [`ZmqError::Send`] naming that subscription.
#[derive(Clone)]
pub struct ZmqQueuePublisher {
    cell: Arc<OnceCell<SharedLifecycle>>,
    push: Arc<Mutex<Option<Attached>>>,
}

/// A PUSH socket, and the subscription of this service it reaches when it dialed one.
struct Attached {
    socket: Sender<Outbox<PushSocket>>,
    /// Set when a subscription of this service bound the endpoint and the socket dialed it: that
    /// subscription receives every message sent, so only its own name may be sent.
    local: Option<String>,
}

impl Attached {
    /// Attaches a PUSH socket per the endpoint's side, or its in-process counterpart.
    async fn attach(lifecycle: &Lifecycle) -> Result<Self, ZmqError> {
        #[cfg(feature = "testing")]
        if let Some(bus) = lifecycle.in_process_bus() {
            let local = lifecycle
                .local_listener()
                .map(|listener| listener.subscription.clone());
            return Ok(Self {
                socket: Sender::InProcess {
                    bus: Arc::clone(bus),
                    local: local.is_some(),
                },
                local,
            });
        }
        let mut socket = PushSocket::new();
        let local = lifecycle
            .attach_sender(&mut socket)
            .await?
            .map(|listener| listener.subscription.clone());
        Ok(Self {
            socket: Sender::Socket(Outbox::new(socket)),
            local,
        })
    }

    async fn send(&mut self, name: &str, frames: zeromq::ZmqMessage) -> Result<(), ZmqError> {
        match &mut self.socket {
            Sender::Socket(outbox) => outbox.send(name, frames).await,
            // The subscription that bound the endpoint takes whatever is pushed into it.
            #[cfg(feature = "testing")]
            Sender::InProcess { bus, local } => {
                let pick = if *local { Pick::All } else { Pick::Nobody };
                bus.send(name, frames, None, pick);
                Ok(())
            }
        }
    }
}

impl fmt::Debug for ZmqQueuePublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqQueuePublisher").finish_non_exhaustive()
    }
}

impl Publisher for ZmqQueuePublisher {
    /// A frame owns its bytes: the payload becomes the message's third frame and the socket
    /// keeps it until the send completes.
    type Payload = Take;

    type Error = ZmqError;

    /// ZMTP carries no per-message setting: a send takes the frames and nothing else, so there is
    /// nothing for a call site to adjust. See the [crate documentation](crate#per-message-settings).
    type Options = ();

    /// # Cancel safety
    ///
    /// Cancel-safe. A publish dropped before the socket takes its message sends nothing. One
    /// dropped mid-send, while the peer applies back-pressure, leaves its message with the
    /// publisher: the next publish through it or a clone completes that send before its own, and
    /// a failure of it is logged against its name.
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
            return Err(dials_the_sender(name, subscription, "PUSH", "queue"));
        }
        let mut push = self.push.lock().await;
        if push.is_none() {
            *push = Some(Attached::attach(lifecycle).await?);
        }
        let attached = push.as_mut().expect("just attached");
        // Checked per message because the name is per message: a slot names it at the call site
        // and a transform may rewrite it per delivery, while whether a subscription of this
        // service holds the endpoint is deployment configuration read once one has bound. Nothing
        // before the first publish sees both. A socket that dials out or listens carries no name
        // and skips it.
        if let Some(subscription) = attached.local.as_deref()
            && subscription != name
        {
            return Err(returns_to_subscription(name, subscription));
        }
        attached.send(name, frames).await
    }
}

/// The publish policy for [`ZmqQueuePublisher`].
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::ZmqQueuePublish;
///
/// let policy = ZmqQueuePublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct ZmqQueuePublish;

impl<Role: EndpointRole> PublishPolicy<ConnectedZmqQueue<Role>> for ZmqQueuePublish {
    type Live = ZmqQueuePublisher;

    fn pair(
        self,
        connected: &ConnectedZmqQueue<Role>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The destination reaches the binding, because on this pattern it is also what frame 0 of
    /// every message on the channel holds.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::PushPull, channel)
    }
}

impl<Role: EndpointRole> DefaultPublish for ConnectedZmqQueue<Role> {
    type Policy = ZmqQueuePublish;
}

#[cfg(test)]
mod tests {
    use ruststream::nonzero;

    use super::*;

    #[tokio::test]
    async fn clones_sharing_a_lifecycle_keep_their_own_read_ahead() {
        let wide = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"));
        let narrow = wide.clone().read_ahead(nonzero!(4_usize));
        let wide = wide.connect().await.expect("connects");
        let narrow = narrow.connect().await.expect("connects");

        assert!(
            Arc::ptr_eq(&wide.lifecycle, &narrow.lifecycle),
            "one lifecycle"
        );
        assert_eq!(wide.read_ahead, DEFAULT_READ_AHEAD);
        assert_eq!(narrow.read_ahead, nonzero!(4_usize));
    }
}
