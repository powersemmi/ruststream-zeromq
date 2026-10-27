//! [`ZmqRpc`]: the DEALER/ROUTER pattern, request and reply.
//!
//! The responder subscribes on a ROUTER socket: each request arrives with a `reply-to` header
//! addressing the requesting peer, and the publisher routes the answer back through the same
//! ROUTER. The requester uses the [`RequestReply`] capability: one DEALER per request, matched by
//! the `correlation-id` header.
//!
//! - [ZmqRpc][rpc]: [`ZmqRpc`], connected into [`ConnectedZmqRpc`], on either side of its
//!   endpoint.
//! - [Request-reply][request-reply]: [`ZmqRpcPublisher`] issues requests through
//!   [`RequestReply`].
//! - [Responders][responders]: [`ZmqRpcSubscriber`], answered at each request's `reply-to`
//!   address through [`ZmqRpcPublish`], offered as [`Publish`] by [`prelude`].
//!
//! [rpc]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/zmq-rpc/
//! [request-reply]: https://powersemmi.github.io/ruststream-zeromq/latest/components/request-reply/
//! [responders]: https://powersemmi.github.io/ruststream-zeromq/latest/components/request-reply/responders/

/// The publish policy of this form, under the name a mount site writes.
///
/// There is no separate request-side policy to name: both directions of the exchange run on one
/// [`ZmqRpcPublisher`], which routes replies through the responder's ROUTER and issues requests
/// over its own DEALER.
pub use self::ZmqRpcPublish as Publish;

/// The imports a routes file on the DEALER/ROUTER exchange writes, in one glob.
///
/// The framework's prelude, the shared [`ZmqEndpoint`], the broker [`ZmqRpc`], its publish
/// policy as [`Publish`], and the [`RequestReply`] capability.
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::rpc::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Greeting {
///     who: String,
/// }
///
/// // An answer has no destination of its own: the ROUTER addresses it per request. The type
/// // derives `Outgoing` without a name, and the clause's literal is the placeholder a publish
/// // transform replaces with the request's `reply-to` address.
/// #[derive(Serialize, Outgoing)]
/// struct Answer {
///     text: String,
/// }
///
/// #[subscriber("greeter", reply("reply"))]
/// async fn greet(request: &Greeting) -> Answer {
///     Answer {
///         text: format!("hello {}", request.who),
///     }
/// }
///
/// // Answers at the `reply-to` address each request carries, under its `correlation-id`; the
/// // `ZmqRpcPublisher` documentation shows it in full.
/// # use ruststream::runtime::{Outgoing, PublishContext};
/// # struct ReplyToRequester;
/// # impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
/// #     type Destination = Names;
/// #     fn apply(
/// #         &self,
/// #         out: &mut Outgoing<'_>,
/// #         _options: &mut Option<Options>,
/// #         cx: &PublishContext<'_, C>,
/// #     ) {
/// #         if let Some(reply_to) = cx.headers().get_shared("reply-to")
/// #             && let Ok(reply_to) = Str::try_from(reply_to)
/// #         {
/// #             out.set_name(reply_to);
/// #         }
/// #         if let Some(correlation) = cx.headers().get_shared("correlation-id") {
/// #             out.headers_mut().insert(Str::from_static("correlation-id"), correlation);
/// #         }
/// #     }
/// # }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
///         ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
///         |b| {
///             b.include(greet)
///                 .out_reply(Publish)
///                 .transform(ReplyToRequester)
///                 .out_retry(Publish)
///                 .to("greeter.retry");
///         },
///     )
/// }
/// ```
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use crate::endpoint::ZmqEndpoint;

    // Only this form implements it; keep it out of the queue and fan-out preludes.
    pub use ruststream::RequestReply;

    // `Publish` is the mount-site vocabulary, and it is why this glob belongs in a routes file
    // rather than a handler one: a handler imports the framework prelude alone and bounds its
    // injected publisher with a broker capability trait, so the two names never meet.
    pub use super::{Publish, ZmqRpc};
}

use std::fmt;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use futures::lock::Mutex;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer, IncomingMessage,
    NamedCopies, OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply, ServerSpec,
    Str, Subscribe, Subscriber, Take,
};
use tokio::sync::OnceCell;
use zeromq::prelude::*;
use zeromq::util::PeerIdentity;
use zeromq::{DealerSocket, RouterSendHalf, RouterSocket, SocketOptions};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
#[cfg(feature = "testing")]
use crate::common::DeliveryReceiver;
use crate::common::{
    DEFAULT_READ_AHEAD, DriverHandle, Lifecycle, SharedLifecycle, WireSubscriber, delivery_channel,
    send_with_retry,
};
use crate::endpoint::{Endpoint, EndpointRole, ZmqEndpoint};
use crate::envelope::{self, Envelope, Enveloped, Frames, Pattern};
use crate::error::ZmqError;
#[cfg(feature = "testing")]
use crate::in_process::{self, Bus};
use crate::message::ZmqMessage;

/// The prefix of reply destinations minted by the responder subscription.
pub(crate) const REPLY_PREFIX: &str = "zmq-reply:";

/// The header a request carries the address of its answer in.
pub(crate) const REPLY_TO_HEADER: &str = "reply-to";

/// The header a request and its answer are matched on.
pub(crate) const CORRELATION_ID_HEADER: &str = "correlation-id";

/// Where a client reads that address, as the specification's runtime expression.
///
/// A reply on this pattern is addressed per request, so the generated document reports the reply
/// channel without an address and points here instead.
#[cfg(feature = "asyncapi")]
pub(crate) const REPLY_ADDRESS_LOCATION: &str = "$message.header#/reply-to";

/// Builds the reply destination addressing one requesting peer.
///
/// The responder's ROUTER derives `identity` from the peer that sent the request, over a socket and
/// in process alike.
pub(crate) fn reply_address(identity: &[u8]) -> String {
    format!("{REPLY_PREFIX}{}", hex_encode(identity))
}

/// Mints the correlation id a request carries when the caller supplied none.
pub(crate) fn new_correlation_id() -> String {
    format!("req-{}-{}", std::process::id(), hex_encode(&rand_suffix()))
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

/// The DEALER/ROUTER request-reply pattern.
///
/// The endpoint may take either side, and the pattern answers the same on both: a copy of a
/// request has no address of its own, so every responder registration names where its retry
/// copies go.
///
/// # Examples
///
/// A queue worker that asks a model service for a score before it finishes each job:
///
/// ```
/// use std::error::Error;
/// use std::time::Duration;
///
/// use ruststream::OutgoingMessage;
/// use ruststream::codec::{Codec, JsonCodec};
/// use ruststream_zeromq::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Deserialize)]
/// struct Score {
///     value: f64,
/// }
///
/// #[subscriber("jobs")]
/// async fn rate(
///     job: &Job,
///     Out(model): Out<impl RequestReply>,
/// ) -> Result<(), Box<dyn Error + Send + Sync>> {
///     let request = JsonCodec.encode(job)?;
///     let reply = model
///         .request(OutgoingMessage::new("score", request.as_ref()), Duration::from_secs(5))
///         .await?;
///     let score: Score = JsonCodec.decode(reply.payload())?;
///     tracing::info!(job.id, score = score.value, "scored");
///     Ok(())
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let model = ZmqRpc::new(ZmqEndpoint::connect("tcp://model:5557")).bindable();
///     let ask = model.bind(ZmqRpcPublish);
///     RustStream::new(AppInfo::new("scorer", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(rate).out(DefaultSlot, ask).build();
///         })
///         .register_broker(model)
/// }
/// ```
#[derive(Clone)]
#[must_use]
pub struct ZmqRpc<Env = Frames> {
    endpoint: Endpoint,
    read_ahead: NonZeroUsize,
    envelope: Env,
    cell: Arc<OnceCell<RpcShared>>,
}

impl<Env: Envelope> fmt::Debug for ZmqRpc<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqRpc")
            .field("endpoint", &self.endpoint)
            .field("envelope", &Env::NAME)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct RpcShared {
    lifecycle: SharedLifecycle,
    /// What replies route through; set when a subscription attaches.
    router_tx: Arc<OnceCell<ReplyRoute>>,
}

/// What a reply routes through: the responder's ROUTER send half, or, in process, the transport
/// that routes by the same identities.
///
/// Without the `testing` feature it is the send half itself: one variant, no tag, no branch.
#[derive(Clone)]
enum ReplyRoute {
    /// The responder's ROUTER send half; empty between a responder closing and the next one
    /// opening.
    Router(Arc<Mutex<Option<RouterSendHalf>>>),
    #[cfg(feature = "testing")]
    InProcess(Arc<Bus>),
}

#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<ReplyRoute>() == size_of::<Arc<Mutex<Option<RouterSendHalf>>>>());

impl fmt::Debug for RpcShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcShared").finish_non_exhaustive()
    }
}

impl ZmqRpc {
    /// Records the endpoint, on either side. No I/O.
    ///
    /// # Examples
    ///
    /// A model service that answers each score request at the peer that sent it:
    ///
    /// ```
    /// use ruststream_zeromq::rpc::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Features {
    ///     values: Vec<f64>,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// struct Score {
    ///     value: f64,
    /// }
    ///
    /// #[subscriber("score", reply("reply"))]
    /// async fn score(request: &Features) -> Score {
    ///     Score {
    ///         value: request.values.iter().sum(),
    ///     }
    /// }
    ///
    /// // Answers at the request's `reply-to` address; the `ZmqRpcPublisher` documentation shows it
    /// // in full.
    /// # use ruststream::runtime::{Outgoing, PublishContext};
    /// # struct ReplyToRequester;
    /// # impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    /// #     type Destination = Names;
    /// #     fn apply(
    /// #         &self,
    /// #         out: &mut Outgoing<'_>,
    /// #         _options: &mut Option<Options>,
    /// #         cx: &PublishContext<'_, C>,
    /// #     ) {
    /// #         if let Some(reply_to) = cx.headers().get_shared("reply-to")
    /// #             && let Ok(reply_to) = Str::try_from(reply_to)
    /// #         {
    /// #             out.set_name(reply_to);
    /// #         }
    /// #         if let Some(correlation) = cx.headers().get_shared("correlation-id") {
    /// #             out.headers_mut().insert(Str::from_static("correlation-id"), correlation);
    /// #         }
    /// #     }
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("model", "0.1.0")).with_broker(
    ///         ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
    ///         |b| {
    ///             b.include(score)
    ///                 .out_reply(Publish)
    ///                 .transform(ReplyToRequester)
    ///                 .out_retry(Publish)
    ///                 .to("score.retry");
    ///         },
    ///     )
    /// }
    /// ```
    pub fn new<Role: EndpointRole>(endpoint: ZmqEndpoint<Role>) -> Self {
        Self {
            endpoint: endpoint.into_inner(),
            read_ahead: DEFAULT_READ_AHEAD,
            envelope: Frames,
            cell: Arc::new(OnceCell::new()),
        }
    }
}

impl<Env: Envelope> ZmqRpc<Env> {
    /// The layout this exchange's requests and answers travel in, [`Frames`] unless set. A
    /// publish policy may name another for the answers it writes.
    ///
    /// # Examples
    ///
    /// A responder answers plain DEALER peers that send each request as one frame and read the
    /// answer the same way:
    ///
    /// ```
    /// use ruststream_zeromq::envelope::NoOp;
    /// use ruststream_zeromq::rpc::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Greeting {
    ///     who: String,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// struct Answer {
    ///     text: String,
    /// }
    ///
    /// #[subscriber("greeter", reply("reply"))]
    /// async fn greet(request: &Greeting) -> Answer {
    ///     Answer {
    ///         text: format!("hello {}", request.who),
    ///     }
    /// }
    ///
    /// // Answers at the `reply-to` address the ROUTER gives each request; the `ZmqRpcPublisher`
    /// // documentation shows it in full.
    /// # use ruststream::runtime::{Outgoing, PublishContext};
    /// # struct ReplyToRequester;
    /// # impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    /// #     type Destination = Names;
    /// #     fn apply(
    /// #         &self,
    /// #         out: &mut Outgoing<'_>,
    /// #         _options: &mut Option<Options>,
    /// #         cx: &PublishContext<'_, C>,
    /// #     ) {
    /// #         if let Some(reply_to) = cx.headers().get_shared("reply-to")
    /// #             && let Ok(reply_to) = Str::try_from(reply_to)
    /// #         {
    /// #             out.set_name(reply_to);
    /// #         }
    /// #     }
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let greeter = ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")).envelope(NoOp);
    ///     RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(greeter, |b| {
    ///         b.include(greet)
    ///             .out_reply(Publish)
    ///             .transform(ReplyToRequester);
    ///     })
    /// }
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> ZmqRpc<Other> {
        ZmqRpc {
            endpoint: self.endpoint,
            read_ahead: self.read_ahead,
            envelope,
            cell: self.cell,
        }
    }

    /// How many deliveries a subscription reads off its socket ahead of the handler; 1000 unless
    /// set, the receive high-water mark `ZeroMQ` itself gives a socket.
    ///
    /// Past this bound the subscription stops reading, the socket's buffers fill, and the sender
    /// waits: a handler slower than the wire slows the sender down rather than growing this
    /// process's memory.
    ///
    /// # Examples
    ///
    /// A responder whose answers take a database round trip holds 64 requests ahead, and the
    /// requesters wait beyond that:
    ///
    /// ```
    /// use ruststream_zeromq::rpc::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Lookup {
    ///     customer: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// struct Customer {
    ///     name: String,
    /// }
    ///
    /// #[subscriber("customers", reply("reply"))]
    /// async fn lookup(request: &Lookup) -> Customer {
    ///     Customer {
    ///         name: format!("customer {}", request.customer),
    ///     }
    /// }
    ///
    /// // Answers at the request's `reply-to` address; the `ZmqRpcPublisher` documentation shows it
    /// // in full.
    /// # use ruststream::runtime::{Outgoing, PublishContext};
    /// # struct ReplyToRequester;
    /// # impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    /// #     type Destination = Names;
    /// #     fn apply(
    /// #         &self,
    /// #         out: &mut Outgoing<'_>,
    /// #         _options: &mut Option<Options>,
    /// #         cx: &PublishContext<'_, C>,
    /// #     ) {
    /// #         if let Some(reply_to) = cx.headers().get_shared("reply-to")
    /// #             && let Ok(reply_to) = Str::try_from(reply_to)
    /// #         {
    /// #             out.set_name(reply_to);
    /// #         }
    /// #         if let Some(correlation) = cx.headers().get_shared("correlation-id") {
    /// #             out.headers_mut().insert(Str::from_static("correlation-id"), correlation);
    /// #         }
    /// #     }
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let requests = ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557"))
    ///         .read_ahead(nonzero!(64_usize));
    ///     RustStream::new(AppInfo::new("customers", "0.1.0")).with_broker(requests, |b| {
    ///         b.include(lookup)
    ///             .out_reply(Publish)
    ///             .transform(ReplyToRequester)
    ///             .out_retry(Publish)
    ///             .to("customers.retry");
    ///     })
    /// }
    /// ```
    pub const fn read_ahead(mut self, read_ahead: NonZeroUsize) -> Self {
        self.read_ahead = read_ahead;
        self
    }

    /// A publisher sharing this pattern's state; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> ZmqRpcPublisher<Env> {
        ZmqRpcPublisher {
            cell: Arc::clone(&self.cell),
            envelope: self.envelope.clone(),
        }
    }
}

impl<Env: Envelope> Broker for ZmqRpc<Env> {
    type Error = ZmqError;
    type Connected = ConnectedZmqRpc<Env>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::new).await
    }
}

impl<Env: Envelope> ZmqRpc<Env> {
    /// The connect transition over the transport `lifecycle` builds: the sockets, or the
    /// in-process transport the test harness connects instead.
    pub(crate) async fn connect_with(
        self,
        lifecycle: fn(Endpoint) -> Lifecycle,
    ) -> Result<ConnectedZmqRpc<Env>, ZmqError> {
        let shared = self
            .cell
            .get_or_try_init(async || {
                self.endpoint.validate()?;
                Ok::<_, ZmqError>(RpcShared {
                    lifecycle: Arc::new(lifecycle(self.endpoint.clone())),
                    router_tx: Arc::new(OnceCell::new()),
                })
            })
            .await?
            .clone();
        Ok(ConnectedZmqRpc {
            shared,
            read_ahead: self.read_ahead,
            envelope: self.envelope,
            cell: self.cell,
        })
    }
}

impl<Env: Envelope> DescribeServer for ZmqRpc<Env> {
    fn describe_server(&self) -> ServerSpec {
        self.endpoint.server_spec::<Env>()
    }
}

/// The connected form of [`ZmqRpc`].
pub struct ConnectedZmqRpc<Env = Frames> {
    shared: RpcShared,
    /// How far this subscription reads ahead, from the broker this form was connected from.
    read_ahead: NonZeroUsize,
    envelope: Env,
    cell: Arc<OnceCell<RpcShared>>,
}

impl<Env> fmt::Debug for ConnectedZmqRpc<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedZmqRpc")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> ConnectedZmqRpc<Env> {
    #[cfg(feature = "testing")]
    pub(crate) fn lifecycle(&self) -> &Lifecycle {
        &self.shared.lifecycle
    }

    #[cfg(feature = "testing")]
    pub(crate) const fn envelope(&self) -> &Env {
        &self.envelope
    }

    /// The address the responder resolved by binding (useful with an ephemeral
    /// `tcp://...:0` endpoint); `None` until a subscription has bound.
    #[must_use]
    pub fn bound_address(&self) -> Option<String> {
        self.shared.lifecycle.bound_address()
    }

    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqRpcPublisher<Env> {
        ZmqRpcPublisher {
            cell: Arc::clone(&self.cell),
            envelope: self.envelope.clone(),
        }
    }
}

impl<Env: Envelope> ConnectedBroker for ConnectedZmqRpc<Env> {
    type Error = ZmqError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.shared.lifecycle.closed.store(true, Ordering::Release);
        ready(Ok(()))
    }
}

/// A responder subscription on the ROUTER socket, yielding one request at a time.
///
/// It is the one subscriber of this crate that is no
/// [`BatchSubscriber`](ruststream::BatchSubscriber), so `.batch(..)` on a `ZmqRpc` registration
/// does not compile - deliberately. A responder exists to answer, and the address it answers at
/// travels per request in the `reply-to` header the ROUTER stamps on it; a batch carries one
/// publish context for the whole batch, so a batch's answers could not be addressed to the peers
/// that asked. Keeping the capability off the type turns that into a compile error instead of a
/// run of misrouted replies. Batch the one-way patterns ([`ZmqQueue`](crate::ZmqQueue),
/// [`ZmqFanout`](crate::ZmqFanout)) instead, and answer requests one at a time.
pub struct ZmqRpcSubscriber<Env: Envelope = Frames> {
    name: String,
    inner: WireSubscriber<Env>,
}

impl<Env: Envelope> fmt::Debug for ZmqRpcSubscriber<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqRpcSubscriber")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> Subscriber for ZmqRpcSubscriber<Env> {
    type Message = ZmqMessage<Env>;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<ZmqMessage<Env>, ZmqError>> + Send + '_ {
        self.inner.stream()
    }
}

impl<Env: Envelope> Subscribe for ConnectedZmqRpc<Env> {
    type Subscriber = ZmqRpcSubscriber<Env>;

    /// A copy of a request has no address of its own, so the mount site names where one goes.
    ///
    /// The name a responder subscribes under is not a publish destination on this pattern:
    /// [`ZmqRpcPublisher`] routes to the peer identity a request carried and refuses a plain
    /// name, so a copy published under the subscription name would reach nothing. A registration
    /// that binds `.out_retry(..)` over a responder therefore has to say where the copies go,
    /// with `.to("name")` or with a transform that names one per delivery, and one that says
    /// neither is refused before the subscription opens. Ask a requester again rather than
    /// retrying its request from the responder side.
    type Copies = NamedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.shared.lifecycle.ensure_open()?;
        #[cfg(feature = "testing")]
        if let Some(bus) = self.shared.lifecycle.in_process_bus() {
            let slot = self.shared.lifecycle.attach_in_process(name).await?;
            let envelope = self.envelope.clone();
            let (rx, registration) =
                bus.subscribe(name, move |message| read_request(&envelope, message));
            let _ = self
                .shared
                .router_tx
                .set(ReplyRoute::InProcess(Arc::clone(bus)));
            return Ok(ZmqRpcSubscriber {
                name: name.to_owned(),
                inner: WireSubscriber {
                    rx: DeliveryReceiver::InProcess(rx),
                    _driver: DriverHandle::InProcess(registration, slot),
                },
            });
        }
        let lifecycle = Arc::clone(&self.shared.lifecycle);
        let subscription = name.to_owned();
        let (socket, slot) = self
            .shared
            .lifecycle
            .on_runtime(async move {
                let mut socket = RouterSocket::new();
                let slot = lifecycle
                    .attach_receiver(&mut socket, &subscription)
                    .await?;
                Ok((socket, slot))
            })
            .await?;
        let (send_half, mut recv_half) = socket.split();
        // One responder ROUTER per pattern instance: replies route through it. A responder that
        // reopens after the last one closed replaces the half, inside the lock a reply takes
        // anyway.
        let route = self
            .shared
            .router_tx
            .get_or_init(async || ReplyRoute::Router(Arc::new(Mutex::new(None))))
            .await;
        match route {
            ReplyRoute::Router(router) => *router.lock().await = Some(send_half),
            // A socket subscription never opens on a broker connected in process.
            #[cfg(feature = "testing")]
            ReplyRoute::InProcess(_) => {}
        }

        let (tx, rx) = delivery_channel(self.read_ahead);
        let envelope = self.envelope.clone();
        let task = self.shared.lifecycle.spawn(async move {
            loop {
                match recv_half.recv().await {
                    Ok(message) => {
                        let Some(item) = read_request(&envelope, message) else {
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
        Ok(ZmqRpcSubscriber {
            name: name.to_owned(),
            inner: WireSubscriber {
                rx,
                _driver: DriverHandle::Task(task, slot),
            },
        })
    }
}

/// A request as the ROUTER's driver reads it: the identity frame the ROUTER put in front becomes
/// the `reply-to` address the answer goes to, and the rest is the documented layout. A message
/// with no identity frame is skipped.
pub(crate) fn read_request<Env: Envelope>(
    envelope: &Env,
    message: zeromq::ZmqMessage,
) -> Option<Result<ZmqMessage<Env>, ZmqError>> {
    let mut frames = message.into_vecdeque();
    let identity = frames.pop_front()?;
    if frames.is_empty() {
        return Some(Err(ZmqError::Wire(
            "a request needs a frame behind the peer identity".into(),
        )));
    }
    Some(envelope::read(
        envelope,
        Pattern::DealerRouter,
        frames,
        Some(identity),
    ))
}

/// Publishes replies back through the responder's ROUTER, and issues requests via
/// [`RequestReply`].
///
/// A reply goes to the peer whose request it answers. The ROUTER stamps a `reply-to` header on
/// each request, and a publish transform that declares `Destination = Names` sets the answer's
/// destination to that address. The answer type therefore declares no destination of its own,
/// and the name in the `reply("..")` clause is the fallback: what the generated document reports,
/// and where a delivery the transform left alone is answered. The address travels from the
/// request's own buffer: [`HeaderMap::get_shared`](ruststream::HeaderMap::get_shared) hands the
/// header over as a counted handle and [`Str`] checks it is text, so answering costs a reference
/// count rather than a copy. An answer whose requester has gone is refused with
/// [`ZmqError::Send`], naming the reply address: the ROUTER holds no peer under that identity.
///
/// A request goes out over a DEALER socket of its own and returns the answer matched by the
/// `correlation-id` header. A correlation id set on the request is kept, so an upper layer can
/// match on its own identifier, and no answer within the timeout returns
/// [`ZmqError::RequestTimeout`].
///
/// # Examples
///
/// A greeter that answers each request at the peer that sent it, and asks itself once it has
/// started:
///
/// ```
/// use std::io;
/// use std::time::Duration;
///
/// use ruststream::codec::{Codec, JsonCodec};
/// use ruststream::runtime::{ForReply, Names, Outgoing, PublishContext, PublishTransform};
/// use ruststream::{IncomingMessage, OutgoingMessage, Str};
/// use ruststream_zeromq::rpc::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct Greeting {
///     who: String,
/// }
///
/// // No destination of its own: the ROUTER addresses each answer per request.
/// #[derive(Deserialize, Serialize, ruststream::Outgoing)]
/// struct Answer {
///     text: String,
/// }
///
/// struct ReplyToRequester;
///
/// impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
///     type Destination = Names;
///
///     fn apply(
///         &self,
///         out: &mut Outgoing<'_>,
///         _options: &mut Option<Options>,
///         cx: &PublishContext<'_, C>,
///     ) {
///         if let Some(reply_to) = cx.headers().get_shared("reply-to")
///             && let Ok(reply_to) = Str::try_from(reply_to)
///         {
///             out.set_name(reply_to);
///         }
///         if let Some(correlation) = cx.headers().get_shared("correlation-id") {
///             out.headers_mut().insert(Str::from_static("correlation-id"), correlation);
///         }
///     }
/// }
///
/// // The literal destination is the fallback the transform replaces per delivery.
/// #[subscriber("greeter", reply("reply"))]
/// async fn greet(request: &Greeting) -> Answer {
///     Answer {
///         text: format!("hello {}", request.who),
///     }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     // Port zero binds an ephemeral port; the requester dials whatever it resolved to.
///     RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
///         ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")),
///         |b| {
///             // A responder addresses no retry copies, so the mount says where they go.
///             b.include(greet)
///                 .out_reply(Publish)
///                 .transform(ReplyToRequester)
///                 .out_retry(Publish)
///                 .to("greeter.retry");
///
///             b.after_startup(Publish, async move |publisher| -> io::Result<()> {
///                 let request = JsonCodec
///                     .encode(&Greeting {
///                         who: "world".to_owned(),
///                     })
///                     .map_err(io::Error::other)?;
///                 let answer = publisher
///                     .request(
///                         OutgoingMessage::new("greeter", request.as_ref()),
///                         Duration::from_secs(5),
///                     )
///                     .await
///                     .map_err(io::Error::other)?;
///                 let answer: Answer = JsonCodec
///                     .decode(answer.payload())
///                     .map_err(io::Error::other)?;
///                 tracing::info!(text = %answer.text, "reply");
///                 Ok(())
///             });
///         },
///     )
/// }
/// ```
#[derive(Clone)]
pub struct ZmqRpcPublisher<Env = Frames> {
    cell: Arc<OnceCell<RpcShared>>,
    envelope: Env,
}

impl<Env> fmt::Debug for ZmqRpcPublisher<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqRpcPublisher").finish_non_exhaustive()
    }
}

impl<Env> ZmqRpcPublisher<Env> {
    fn shared(&self) -> Result<&RpcShared, ZmqError> {
        let shared = self.cell.get().ok_or(ZmqError::NotConnected)?;
        shared.lifecycle.ensure_open()?;
        Ok(shared)
    }
}

impl<Env: Envelope> Publisher for ZmqRpcPublisher<Env> {
    /// A frame owns its bytes: the payload becomes the message's third frame and the ROUTER
    /// keeps it until the send completes.
    type Payload = Take;

    type Error = ZmqError;

    /// ZMTP carries no per-message setting: a send takes the frames and nothing else, so there is
    /// nothing for a call site to adjust. Which peer a reply reaches is a destination, not a
    /// setting, and a naming publish transform supplies it. The
    /// [Publishing](https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/)
    /// page shows what a publish carries.
    type Options = ();

    /// # Cancel safety
    ///
    /// Not cancel-safe. Dropping the future can leave the reply half-handed to the ROUTER, and the
    /// requester then waits out its timeout. Answer from a task of its own rather than inside a
    /// `select!` arm.
    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let shared = self.shared()?;
        let Some(identity_hex) = msg.name().strip_prefix(REPLY_PREFIX) else {
            return Err(ZmqError::Send {
                name: msg.name().to_owned(),
                reason: format!(
                    "the rpc publisher routes '{REPLY_PREFIX}...' replies; use request() for outbound requests"
                ),
            });
        };
        let identity = hex_decode(identity_hex).ok_or_else(|| ZmqError::Send {
            name: msg.name().to_owned(),
            reason: "malformed reply address".to_owned(),
        })?;
        let router = shared.router_tx.get().ok_or_else(|| ZmqError::Send {
            name: msg.name().to_owned(),
            reason: "no responder subscription is attached".to_owned(),
        })?;

        // Frame 0 of a reply is the literal "reply". The ROUTER identity frame pushed in front of
        // it is what addresses the requester, so the name position carries nothing to route on.
        let (name, payload, headers) = msg.into_parts();
        let message = envelope::write(
            &self.envelope,
            Pattern::DealerRouter,
            name,
            Some(Bytes::from(identity)),
            ("reply", &headers, payload.freeze()),
        )?;
        match router {
            ReplyRoute::Router(router) => {
                let mut guard = router.lock().await;
                let Some(half) = guard.as_mut() else {
                    return Err(ZmqError::Send {
                        name: name.to_owned(),
                        reason: "no responder subscription is attached".to_owned(),
                    });
                };
                let sent = half.send(message).await;
                drop(guard);
                sent.map_err(|e| ZmqError::Send {
                    name: name.to_owned(),
                    reason: e.to_string(),
                })
            }
            #[cfg(feature = "testing")]
            ReplyRoute::InProcess(bus) => bus.reply(&self.envelope, name, message),
        }
    }
}

impl<Env: Envelope> RequestReply for ZmqRpcPublisher<Env> {
    type Reply = ZmqMessage<Env>;

    /// # Cancel safety
    ///
    /// Not cancel-safe. Dropping the future closes the DEALER this request was issued on, so an
    /// answer already in flight is lost, and the request itself may have reached the responder
    /// already. Give up through `timeout` rather than by cancelling.
    async fn request(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let shared = self.shared()?;
        #[cfg(feature = "testing")]
        if let Some(bus) = shared.lifecycle.in_process_bus() {
            return request_in_process(&self.envelope, bus, &shared.lifecycle, msg, timeout).await;
        }
        let address = shared.lifecycle.send_target().address().to_owned();

        // One DEALER per request: simple and correct; a shared correlated link is a later
        // optimisation. The identity is random, so replies route to this request alone.
        let mut options = SocketOptions::default();
        options.peer_identity(PeerIdentity::new());
        options.connect_timeout(timeout.min(Duration::from_secs(10)));
        let mut dealer = DealerSocket::with_options(options);
        dealer
            .connect(&address)
            .await
            .map_err(|e| ZmqError::Endpoint {
                endpoint: address.clone(),
                source: crate::error::box_err(e),
            })?;

        let (name, request, correlation) = stamped(&self.envelope, msg)?;
        send_with_retry(&mut dealer, name, request).await?;

        let exchange = async {
            loop {
                let reply = dealer
                    .recv()
                    .await
                    .map_err(|e| ZmqError::Receive(e.to_string()))?;
                let reply = envelope::read(
                    &self.envelope,
                    Pattern::DealerRouter,
                    reply.into_vecdeque(),
                    None,
                )?;
                if answers(&reply, correlation.as_deref()) {
                    return Ok(reply);
                }
            }
        };
        tokio::time::timeout(timeout, exchange)
            .await
            .unwrap_or(Err(ZmqError::RequestTimeout))
    }
}

/// A request over the in-process transport, framed and correlated the way the DEALER does it: it
/// reaches the responder this service binds, when the DEALER would dial one, and a peer outside
/// this service otherwise.
/// Frames a request through `envelope`, stamped with a `correlation-id` where the envelope
/// carries headers: the caller's own when it set one, since an upper layer may match on it too.
fn stamped<'a, Env: Envelope>(
    envelope: &Env,
    msg: OutgoingMessage<'a, BytesMut>,
) -> Result<(&'a str, zeromq::ZmqMessage, Option<String>), ZmqError> {
    let correlation = Env::CARRIES_HEADERS.then(|| {
        msg.headers()
            .correlation_id()
            .map_or_else(new_correlation_id, str::to_owned)
    });
    let (name, payload, mut headers) = msg.into_parts();
    if let Some(correlation) = &correlation {
        headers.insert(Str::from_static(CORRELATION_ID_HEADER), correlation.clone());
    }
    let request = envelope::write(
        envelope,
        Pattern::DealerRouter,
        name,
        None,
        (name, &headers, payload.freeze()),
    )?;
    Ok((name, request, correlation))
}

/// Whether `reply` answers the request stamped with `correlation`. A request through an envelope
/// without headers carries none, and its DEALER is its own, so the first answer on it is the one.
pub(crate) fn answers<Env: Envelope>(reply: &ZmqMessage<Env>, correlation: Option<&str>) -> bool {
    correlation.is_none_or(|correlation| reply.headers().correlation_id() == Some(correlation))
}

#[cfg(feature = "testing")]
async fn request_in_process<Env: Envelope>(
    envelope: &Env,
    bus: &Bus,
    lifecycle: &Lifecycle,
    msg: OutgoingMessage<'_, BytesMut>,
    timeout: Duration,
) -> Result<ZmqMessage<Env>, ZmqError> {
    let (name, request, correlation) = stamped(envelope, msg)?;
    let local = lifecycle.local_listener().is_some();
    in_process::request(
        envelope,
        bus,
        local,
        (name, request),
        correlation.as_deref(),
        timeout,
    )
    .await
}

/// A peer identity no other peer of this process carries, standing in for the random identity a
/// DEALER greets with.
#[cfg(feature = "testing")]
pub(crate) fn peer_identity() -> Bytes {
    Bytes::copy_from_slice(&rand_suffix())
}

/// A per-request unique suffix without a randomness dependency: the next value of a counter this
/// process shares, as eight big-endian bytes.
fn rand_suffix() -> [u8; 8] {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    seq.to_be_bytes()
}

/// The publish policy for [`ZmqRpcPublisher`].
///
/// # Examples
///
/// A service that does not start until the model it depends on answers a request:
///
/// ```
/// use std::io;
/// use std::time::Duration;
///
/// use ruststream::OutgoingMessage;
/// use ruststream_zeromq::prelude::*;
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("scorer", "0.1.0")).with_broker(
///         ZmqRpc::new(ZmqEndpoint::connect("tcp://model:5557")),
///         |b| {
///             b.after_startup(ZmqRpcPublish, async move |model| -> io::Result<()> {
///                 model
///                     .request(
///                         OutgoingMessage::new("health", b"ping".as_slice()),
///                         Duration::from_secs(5),
///                     )
///                     .await
///                     .map_err(io::Error::other)?;
///                 Ok(())
///             });
///         },
///     )
/// }
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct ZmqRpcPublish;

impl ZmqRpcPublish {
    /// This policy writing through `envelope` rather than through its broker's.
    ///
    /// # Examples
    ///
    /// A responder reads requests in the default layout and answers each one as a bare payload:
    ///
    /// ```
    /// use ruststream_zeromq::envelope::NoOp;
    /// use ruststream_zeromq::rpc::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Greeting {
    ///     who: String,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// struct Answer {
    ///     text: String,
    /// }
    ///
    /// #[subscriber("greeter", reply("reply"))]
    /// async fn greet(request: &Greeting) -> Answer {
    ///     Answer {
    ///         text: format!("hello {}", request.who),
    ///     }
    /// }
    ///
    /// // Answers at the `reply-to` address the ROUTER gives each request; the `ZmqRpcPublisher`
    /// // documentation shows it in full.
    /// # use ruststream::runtime::{Outgoing, PublishContext};
    /// # struct ReplyToRequester;
    /// # impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyToRequester {
    /// #     type Destination = Names;
    /// #     fn apply(
    /// #         &self,
    /// #         out: &mut Outgoing<'_>,
    /// #         _options: &mut Option<Options>,
    /// #         cx: &PublishContext<'_, C>,
    /// #     ) {
    /// #         if let Some(reply_to) = cx.headers().get_shared("reply-to")
    /// #             && let Ok(reply_to) = Str::try_from(reply_to)
    /// #         {
    /// #             out.set_name(reply_to);
    /// #         }
    /// #     }
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
    ///         ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
    ///         |b| {
    ///             b.include(greet)
    ///                 .out_reply(Publish.envelope(NoOp))
    ///                 .transform(ReplyToRequester);
    ///         },
    ///     )
    /// }
    /// ```
    pub const fn envelope<Env: Envelope>(self, envelope: Env) -> Enveloped<Self, Env> {
        Enveloped {
            policy: self,
            envelope,
        }
    }
}

impl<Env: Envelope> PublishPolicy<ConnectedZmqRpc<Env>> for ZmqRpcPublish {
    type Live = ZmqRpcPublisher<Env>;

    fn pair(
        self,
        connected: &ConnectedZmqRpc<Env>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The destination reaches the binding and stays out of it: a reply travels to the identity
    /// the ROUTER supplies, so no name on this channel is an address a peer can send to.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::DealerRouter, channel)
    }

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, _channel: &str) -> Bindings {
        bindings::message::<Env>()
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(REPLY_ADDRESS_LOCATION)
    }
}

/// The exchange's policy writing its answers through an envelope of its own, whatever its broker
/// reads.
impl<BrokerEnv, Env> PublishPolicy<ConnectedZmqRpc<BrokerEnv>> for Enveloped<ZmqRpcPublish, Env>
where
    BrokerEnv: Envelope,
    Env: Envelope,
{
    type Live = ZmqRpcPublisher<Env>;

    fn pair(
        self,
        connected: &ConnectedZmqRpc<BrokerEnv>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(ZmqRpcPublisher {
            cell: Arc::clone(&connected.cell),
            envelope: self.envelope,
        }))
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::DealerRouter, channel)
    }

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, _channel: &str) -> Bindings {
        bindings::message::<Env>()
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(REPLY_ADDRESS_LOCATION)
    }
}

impl<Env: Envelope> DefaultPublish for ConnectedZmqRpc<Env> {
    type Policy = ZmqRpcPublish;
}

#[cfg(test)]
mod tests {
    //! Where the batching capability sits, pinned as bounds.
    //!
    //! The one-way patterns batch; the responder only delivers, so a `.batch(..)` registration on
    //! [`ZmqRpc`] fails to compile rather than answering a batch of requests at one address. The
    //! absence cannot be written as a bound, so it lives in [`ZmqRpcSubscriber`]'s own docs; what
    //! is checkable is that the split has not quietly collapsed.

    use ruststream::{BatchSubscriber, Subscriber};

    use super::ZmqRpcSubscriber;
    use crate::ZmqSubscriber;

    fn batches<S: BatchSubscriber>() {}

    fn delivers<S: Subscriber>() {}

    // The instantiations are the assertion, so the body has nothing to run; a test rather than an
    // uncalled function because a chain of uncalled helpers is dead code on one compiler and live
    // on another, and the floor toolchain disagrees with stable about which.
    #[test]
    fn the_one_way_patterns_batch_and_the_responder_only_delivers() {
        batches::<ZmqSubscriber>();
        delivers::<ZmqRpcSubscriber>();
    }
}
