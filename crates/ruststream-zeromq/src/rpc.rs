//! [`ZmqRpc`]: the DEALER/ROUTER pattern - request and reply.
//!
//! The responder side subscribes (a ROUTER socket): each request arrives with a `reply-to`
//! header addressing the requesting peer, and the plain publisher routes replies back through
//! the same ROUTER. The requester side uses the [`RequestReply`] capability: one DEALER per
//! request, correlated by the `correlation-id` header.

/// The publish policy of this form, under the name a mount site writes.
///
/// There is no separate request-side policy to name: both directions of the exchange run on one
/// [`ZmqRpcPublisher`], which routes replies through the responder's ROUTER and issues requests
/// over its own DEALER.
pub use self::ZmqRpcPublish as Publish;

/// The imports a routes file on the DEALER/ROUTER exchange writes, in one glob.
///
/// The framework's prelude, the shared [`ZmqEndpoint`], the descriptor [`ZmqRpc`], its publish
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
/// #[subscriber("greeter", publish("reply"))]
/// async fn greet(request: &Greeting) -> Answer {
///     Answer {
///         text: format!("hello {}", request.who),
///     }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
///         ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
///         |b| {
///             b.include(greet).out_reply(Publish).out_retry(Publish).to("greeter.retry");
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

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer, NamedCopies,
    OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply, ServerSpec, Str, Subscribe,
    Subscriber, Take,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use zeromq::prelude::*;
use zeromq::util::PeerIdentity;
use zeromq::{DealerSocket, RouterSendHalf, RouterSocket, SocketOptions};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
use crate::common::{DriverHandle, Lifecycle, SharedLifecycle, WireSubscriber, send_with_retry};
use crate::endpoint::ZmqEndpoint;
use crate::error::ZmqError;
use crate::message::ZmqMessage;
use crate::wire;

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
/// The responder's ROUTER derives `identity` from the peer that sent the request; the in-process
/// stand-in mints one per request. Both go through here, so a publish transform that rewrites a
/// reply destination reads the same shape under the harness as it does over a socket.
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
/// # Examples
///
/// ```
/// use ruststream_zeromq::{ZmqEndpoint, ZmqRpc};
///
/// let responder = ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557"));
/// let requester = ZmqRpc::new(ZmqEndpoint::connect("tcp://ml:5557"));
/// # let _ = (responder, requester);
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct ZmqRpc {
    endpoint: ZmqEndpoint,
    cell: Arc<OnceCell<RpcShared>>,
}

#[derive(Clone)]
pub(crate) struct RpcShared {
    lifecycle: SharedLifecycle,
    /// The responder's ROUTER send half; set when a subscription attaches.
    router_tx: Arc<OnceCell<Arc<Mutex<RouterSendHalf>>>>,
}

impl std::fmt::Debug for RpcShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcShared").finish_non_exhaustive()
    }
}

impl ZmqRpc {
    /// Records the endpoint. No I/O.
    pub fn new(endpoint: ZmqEndpoint) -> Self {
        Self {
            endpoint,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// A publisher sharing this pattern's state; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> ZmqRpcPublisher {
        ZmqRpcPublisher {
            cell: Arc::clone(&self.cell),
        }
    }
}

impl Broker for ZmqRpc {
    type Error = ZmqError;
    type Connected = ConnectedZmqRpc;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let shared = self
            .cell
            .get_or_try_init(async || {
                self.endpoint.validate()?;
                Ok::<_, ZmqError>(RpcShared {
                    lifecycle: Arc::new(Lifecycle::new(self.endpoint.clone())),
                    router_tx: Arc::new(OnceCell::new()),
                })
            })
            .await?
            .clone();
        Ok(ConnectedZmqRpc {
            shared,
            cell: self.cell,
        })
    }
}

impl DescribeServer for ZmqRpc {
    fn describe_server(&self) -> ServerSpec {
        self.endpoint.server_spec()
    }
}

/// The connected form of [`ZmqRpc`].
#[derive(Debug)]
pub struct ConnectedZmqRpc {
    shared: RpcShared,
    cell: Arc<OnceCell<RpcShared>>,
}

impl ConnectedZmqRpc {
    /// The address the responder resolved by binding (useful with an ephemeral
    /// `tcp://...:0` endpoint); `None` until a subscription has bound.
    #[must_use]
    pub fn bound_address(&self) -> Option<String> {
        self.shared.lifecycle.resolved.get().cloned()
    }

    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqRpcPublisher {
        ZmqRpcPublisher {
            cell: Arc::clone(&self.cell),
        }
    }
}

impl ConnectedBroker for ConnectedZmqRpc {
    type Error = ZmqError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.shared
            .lifecycle
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
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
pub struct ZmqRpcSubscriber {
    name: String,
    inner: WireSubscriber,
}

impl std::fmt::Debug for ZmqRpcSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqRpcSubscriber")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Subscriber for ZmqRpcSubscriber {
    type Message = ZmqMessage;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<ZmqMessage, ZmqError>> + Send + '_ {
        self.inner.stream()
    }
}

impl Subscribe for ConnectedZmqRpc {
    type Subscriber = ZmqRpcSubscriber;

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
        let mut socket = RouterSocket::new();
        self.shared.lifecycle.attach_receiver(&mut socket).await?;
        let (send_half, mut recv_half) = socket.split();
        // One responder ROUTER per pattern instance: replies route through it.
        let _ = self.shared.router_tx.set(Arc::new(Mutex::new(send_half)));

        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                match recv_half.recv().await {
                    Ok(message) => {
                        let mut frames = message.into_vecdeque();
                        let Some(identity) = frames.pop_front() else {
                            continue;
                        };
                        let rest: Result<zeromq::ZmqMessage, _> = frames.try_into();
                        let Ok(rest) = rest else {
                            let _ = tx.send(Err(ZmqError::Wire(
                                "a request needs name and payload frames".into(),
                            )));
                            continue;
                        };
                        let item = wire::decode(rest).map(|(name, mut headers, payload)| {
                            headers.insert(
                                Str::from_static(REPLY_TO_HEADER),
                                reply_address(&identity),
                            );
                            ZmqMessage {
                                name,
                                headers,
                                payload,
                            }
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
        Ok(ZmqRpcSubscriber {
            name: name.to_owned(),
            inner: WireSubscriber {
                rx,
                _driver: DriverHandle { task },
            },
        })
    }
}

/// Publishes replies back through the responder's ROUTER, and issues requests via
/// [`RequestReply`].
#[derive(Clone)]
pub struct ZmqRpcPublisher {
    cell: Arc<OnceCell<RpcShared>>,
}

impl std::fmt::Debug for ZmqRpcPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqRpcPublisher").finish_non_exhaustive()
    }
}

impl ZmqRpcPublisher {
    fn shared(&self) -> Result<&RpcShared, ZmqError> {
        let shared = self.cell.get().ok_or(ZmqError::NotConnected)?;
        shared.lifecycle.ensure_open()?;
        Ok(shared)
    }
}

impl Publisher for ZmqRpcPublisher {
    /// A frame owns its bytes: the payload becomes the message's third frame and the ROUTER
    /// keeps it until the send completes.
    type Payload = Take;

    type Error = ZmqError;

    /// ZMTP carries no per-message setting: a send takes the frames and nothing else, so there is
    /// nothing for a call site to adjust. Which peer a reply reaches is a destination, not a
    /// setting, and a naming publish transform supplies it. See the
    /// [crate documentation](crate#per-message-settings).
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
        let message = wire::encode_addressed_to(
            name,
            Bytes::from(identity),
            "reply",
            &headers,
            payload.freeze(),
        )?;
        let mut router = router.lock().await;
        router.send(message).await.map_err(|e| ZmqError::Send {
            name: name.to_owned(),
            reason: e.to_string(),
        })
    }
}

impl RequestReply for ZmqRpcPublisher {
    type Reply = ZmqMessage;

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
        let (address, _) = shared.lifecycle.sender_address()?;

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

        // Respect a caller-supplied correlation id (an upper layer may match on it too).
        let correlation = msg
            .headers()
            .correlation_id()
            .map_or_else(new_correlation_id, str::to_owned);
        let (name, payload, mut headers) = msg.into_parts();
        headers.insert(Str::from_static(CORRELATION_ID_HEADER), correlation.clone());
        let request = wire::encode_to(name, name, &headers, payload.freeze())?;
        send_with_retry(&mut dealer, name, request).await?;

        let exchange = async {
            loop {
                let reply = dealer
                    .recv()
                    .await
                    .map_err(|e| ZmqError::Receive(e.to_string()))?;
                let (name, reply_headers, payload) = wire::decode(reply)?;
                if reply_headers.correlation_id() == Some(correlation.as_str()) {
                    return Ok(ZmqMessage {
                        name,
                        headers: reply_headers,
                        payload,
                    });
                }
            }
        };
        tokio::time::timeout(timeout, exchange)
            .await
            .unwrap_or(Err(ZmqError::RequestTimeout))
    }
}

/// Mints a reply address for one in-process request, standing in for the peer identity the
/// responder's ROUTER supplies over a socket.
#[cfg(feature = "testing")]
pub(crate) fn new_reply_address() -> String {
    reply_address(&rand_suffix())
}

/// A per-request unique suffix without a randomness dependency: the address of a fresh
/// allocation mixed with a monotonic counter.
fn rand_suffix() -> [u8; 8] {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    seq.to_be_bytes()
}

/// The publish policy for [`ZmqRpcPublisher`].
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::ZmqRpcPublish;
///
/// let policy = ZmqRpcPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct ZmqRpcPublish;

impl PublishPolicy<ConnectedZmqRpc> for ZmqRpcPublish {
    type Live = ZmqRpcPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqRpc,
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
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(REPLY_ADDRESS_LOCATION)
    }
}

impl DefaultPublish for ConnectedZmqRpc {
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
