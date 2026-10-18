//! [`ZmqQueue`]: the PUSH/PULL pattern, competing consumers served in turn.
//!
//! - [ZmqQueue][queue]: [`ZmqQueue`], connected into [`ConnectedZmqQueue`], with
//!   [`read_ahead`](ZmqQueue::read_ahead) as its one setting beside the endpoint.
//! - [Batches][batches]: [`ZmqSubscriber`], the subscription of both one-way patterns, singly or
//!   in batches.
//! - [Publishing][publishing]: [`ZmqQueuePublish`], offered as [`Publish`] by [`prelude`], and its
//!   live [`ZmqQueuePublisher`].
//! - [Replies][replies]: results and replies through a queue on an endpoint of its own.
//! - [out_retry][out-retry]: retry copies, back into a bound queue or to a place the mount names.
//!
//! [queue]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/zmq-queue/
//! [batches]: https://powersemmi.github.io/ruststream-zeromq/latest/components/subscriptions/batches/
//! [publishing]: https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/
//! [replies]: https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/replies/
//! [out-retry]: https://powersemmi.github.io/ruststream-zeromq/latest/components/settlement/out-retry/

/// The publish policy of this form, under the name a mount site writes.
pub use self::ZmqQueuePublish as Publish;

/// The imports a routes file on the PUSH/PULL queue writes, in one glob: the framework's prelude,
/// the shared [`ZmqEndpoint`], the broker [`ZmqQueue`], and its publish policy as [`Publish`].
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
/// #[subscriber("jobs", reply)]
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
    BATCH_MAX_WAIT, DEFAULT_READ_AHEAD, DeliveryReceiver, DriverHandle, Lifecycle, Outbox,
    OwnListener, Sender, SharedLifecycle, WireSubscriber, delivery_channel, dials_the_sender,
    release, returns_to_subscription,
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
/// A worker that binds its queue retries on it. The copy goes back into the queue, whichever
/// worker is free takes it, and the framework's retry-count header caps the deliveries at five:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn handle(job: &Job) -> HandlerOutcome {
///     if job.id == 0 {
///         return HandlerOutcome::retry_after(Duration::from_secs(30));
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
///         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
///         |b| {
///             b.include(handle)
///                 .max_attempts(nonzero!(5u32))
///                 .out_retry(Publish);
///         },
///     )
/// }
/// ```
///
/// One of several workers that dial the same ventilator and compete for its jobs. The ventilator
/// takes nothing back, so the retry copies go to a queue the worker binds itself, where the same
/// handler takes them. The copies carry the framework's counter, so the cap holds across both
/// subscriptions:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn work(job: &Job) -> HandlerOutcome {
///     if job.id == 0 {
///         return HandlerOutcome::retry_after(Duration::from_secs(30));
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
///     let copies = retries.bind(Publish);
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::connect("tcp://ventilator:5555")), |b| {
///             b.include(work)
///                 .max_attempts(nonzero!(5u32))
///                 .out_retry(copies)
///                 .to("jobs");
///         })
///         .with_broker(retries, |b| {
///             b.include(work).max_attempts(nonzero!(5u32));
///         })
/// }
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
    /// A worker that listens for the jobs its producers push:
    ///
    /// ```
    /// use ruststream_zeromq::queue::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     tracing::info!(job.id, "working on the job");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
    ///         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
    ///         |b| {
    ///             b.include(work);
    ///         },
    ///     )
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
    /// A worker whose renders take seconds holds 64 jobs ahead, and the producers wait beyond
    /// that:
    ///
    /// ```
    /// use ruststream_zeromq::queue::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Render {
    ///     scene: String,
    /// }
    ///
    /// #[subscriber("renders")]
    /// async fn render(job: &Render) -> HandlerOutcome {
    ///     tracing::info!(scene = %job.scene, "rendering");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let renders = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555"))
    ///         .read_ahead(nonzero!(64_usize));
    ///     RustStream::new(AppInfo::new("renderer", "0.1.0")).with_broker(renders, |b| {
    ///         b.include(render);
    ///     })
    /// }
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

/// The connected form of [`ZmqQueue`]; sockets attach per subscription and per publisher.
pub struct ConnectedZmqQueue<Role = Bind> {
    lifecycle: SharedLifecycle,
    /// How far this subscription reads ahead, from the broker this form was connected from.
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

    /// The publisher a publish policy pairs: one whose side binds attaches its socket now, rather
    /// than on its first publish, so a peer that dials the endpoint connects before anything is
    /// sent. Startup work, once per pairing: the publish path itself is unchanged.
    async fn paired(&self) -> Result<ZmqQueuePublisher, PairError> {
        let publisher = self.publisher();
        if self.lifecycle.attaches_on_pairing() {
            publisher
                .attach(&self.lifecycle)
                .await
                .map_err(PairError::new)?;
        }
        Ok(publisher)
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
        let lifecycle = Arc::clone(&self.lifecycle);
        let subscription = name.to_owned();
        let (mut socket, slot) = self
            .lifecycle
            .on_runtime(async move {
                let mut socket = PullSocket::new();
                let slot = lifecycle
                    .attach_receiver(&mut socket, &subscription)
                    .await?;
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
///
/// A receive on a socket yields one multipart message, so a batch is collected on this side: it
/// closes once it holds the size the mount named, or 20 ms after its first delivery, whichever
/// comes first. The 20 ms is a constant of this crate.
///
/// # Examples
///
/// A handler that takes a slice receives a whole batch, and the mount site caps its size:
///
/// ```
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn drain(jobs: &[Job]) -> HandlerOutcome {
///     tracing::info!(size = jobs.len(), "working on a batch");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
///         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
///         |b| {
///             b.include(drain.batch(nonzero!(32)));
///         },
///     )
/// }
/// ```
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

/// Publishes into the queue over a PUSH socket of its own.
///
/// Paired by the runtime on the bind side, the socket listens from the moment the service starts,
/// so a PULL peer that dials the endpoint connects before the first push. A publisher that dials,
/// or one taken from a `publisher()` constructor, attaches on its first publish.
///
/// On an endpoint a subscription of this service bound, the socket dials that subscription, so a
/// publish reaches it and nothing else: one under the subscription's own name (a retry copy, a
/// job the service feeds itself) is sent, and one under any other name returns
/// [`ZmqError::Send`] rather than arriving at the subscription as its next delivery. On an
/// endpoint a subscription of this service dials, the peer there only pushes, so every publish
/// returns [`ZmqError::Send`] naming that subscription.
///
/// A publisher that sends before any subscription of its service binds the endpoint listens on
/// the endpoint itself, for peers to dial. When a subscription of the same broker binds the
/// endpoint later, the publisher gives the endpoint up first, on a fixed port and on port zero
/// alike, and its next publish dials that subscription.
///
/// # Examples
///
/// A worker's results leave through a queue on an endpoint of their own, here one a sink binds.
/// The jobs arrive on the socket the `jobs` subscription binds, and that socket takes every
/// message pushed into it: a result published there would come back as the next job, so the
/// worker's own publisher refuses it, and the mount binds the reply to the second broker:
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
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", reply)]
/// async fn work(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
///     let to_results = results.bind(Publish);
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(work).out_reply(to_results);
///         })
///         .with_broker(results, |_b| {})
/// }
/// ```
#[derive(Clone)]
pub struct ZmqQueuePublisher {
    cell: Arc<OnceCell<SharedLifecycle>>,
    push: Arc<Mutex<Option<Attached>>>,
}

/// A PUSH socket, and the subscription of this service it reaches when it dialed one.
pub(crate) struct Attached {
    socket: Sender<Outbox<PushSocket>>,
    /// Set when a subscription of this service bound the endpoint and the socket dialed it: that
    /// subscription receives every message sent, so only its own name may be sent.
    local: Option<String>,
    /// Set when the socket dialed a peer at the endpoint. The client does not dial a PUSH socket
    /// again when that peer goes, so a failed send drops the socket and the next publish dials
    /// anew.
    dials: bool,
}

impl Attached {
    /// Attaches a PUSH socket per the endpoint's side, or its in-process counterpart.
    async fn attach(lifecycle: &SharedLifecycle) -> Result<Self, ZmqError> {
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
                dials: false,
            });
        }
        let attaching = Arc::clone(lifecycle);
        let (socket, local) = lifecycle
            .on_runtime(async move {
                let mut socket = PushSocket::new();
                let local = attaching
                    .attach_sender(&mut socket)
                    .await?
                    .map(|listener| listener.subscription.clone());
                Ok((socket, local))
            })
            .await?;
        Ok(Self {
            socket: Sender::Socket(Outbox::new(socket)),
            local,
            dials: lifecycle.dials(),
        })
    }

    /// Closes the listener of a socket that listens on the endpoint itself, so a subscription of
    /// this service can bind it.
    pub(crate) async fn give_way(self) {
        match self.socket {
            Sender::Socket(outbox) => {
                if let Some(mut socket) = outbox.into_socket().await {
                    release(&mut socket).await;
                }
            }
            #[cfg(feature = "testing")]
            Sender::InProcess { .. } => {}
        }
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

impl ZmqQueuePublisher {
    /// Attaches the socket under the lifecycle's attach lock. One that listens on the endpoint
    /// itself is recorded there, for a subscription of this service that binds it later to make it
    /// give way.
    async fn attach(&self, lifecycle: &SharedLifecycle) -> Result<(), ZmqError> {
        // Attaches run one at a time under this lock, so the slot found empty stays empty until
        // the socket is installed, without holding the slot across the attach.
        let _attaching = lifecycle.attaching().await;
        if self.push.lock().await.is_some() {
            return Ok(());
        }
        let attached = Attached::attach(lifecycle).await?;
        if lifecycle.listens_here(attached.local.is_some()) {
            lifecycle.listens_alongside(OwnListener::Queue(Arc::downgrade(&self.push)));
        }
        *self.push.lock().await = Some(attached);
        Ok(())
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
    /// nothing for a call site to adjust. The
    /// [Publishing](https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/)
    /// page shows what a publish carries.
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
        // A loop, because a subscription of this service that binds the endpoint may take back a
        // socket that listened there between the attach and this lock; the next attach dials it.
        while push.is_none() {
            drop(push);
            self.attach(lifecycle).await?;
            push = self.push.lock().await;
        }
        let attached = push.as_mut().expect("the loop leaves an attached socket");
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
        let dials = attached.dials;
        let sent = attached.send(name, frames).await;
        // A send fails on a dialed socket when its peer is gone: the connection broke, or no
        // peer is left. The client keeps the socket peerless for good, so it goes, and the next
        // publish dials the address again and reaches the peer once it is back. Only the failure
        // path pays for it.
        if sent.is_err() && dials {
            *push = None;
        }
        sent
    }
}

/// The publish policy for [`ZmqQueuePublisher`].
///
/// # Examples
///
/// A handler that splits each order into picks and pushes them to the pickers' queue, bound at the
/// mount site:
///
/// ```
/// use std::error::Error;
///
/// use ruststream_zeromq::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
///     lines: Vec<u64>,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "picks")]
/// struct Pick {
///     order: u64,
///     item: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn split(
///     order: &Order,
///     Out(picks): Out<impl Publisher>,
/// ) -> Result<(), Box<dyn Error + Send + Sync>> {
///     for &item in &order.lines {
///         picks.message(&Pick { order: order.id, item }).publish().await?;
///     }
///     Ok(())
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let pickers = ZmqQueue::new(ZmqEndpoint::connect("tcp://pickers:5556")).bindable();
///     let to_pickers = pickers.bind(ZmqQueuePublish);
///     RustStream::new(AppInfo::new("splitter", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(split).out(DefaultSlot, to_pickers).build();
///         })
///         .register_broker(pickers)
/// }
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct ZmqQueuePublish;

impl<Role: EndpointRole> PublishPolicy<ConnectedZmqQueue<Role>> for ZmqQueuePublish {
    type Live = ZmqQueuePublisher;

    /// On the bind side the publisher's socket attaches here, so it listens once the service has
    /// started; a dialing publisher attaches on its first publish.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when the socket of a binding publisher cannot take the endpoint.
    fn pair(
        self,
        connected: &ConnectedZmqQueue<Role>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        connected.paired()
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
