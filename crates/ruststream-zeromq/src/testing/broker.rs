//! [`ZmqTestBroker`]: the in-process stands, one per socket pattern, and their connected forms.

use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DescribeServer, HeaderMap, NamedCopies,
    OutgoingMessage, RawMessage, ServerSpec, Subscribe,
};

#[cfg(feature = "asyncapi")]
use crate::bindings;
use crate::common::endpoint_taken;
use crate::endpoint::{Bind, Connect, EndpointRole, Side, ZMTP_VERSION};
use crate::error::ZmqError;
use crate::testing::publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
use crate::testing::router::{AddressRouter, Routing};
use crate::testing::subscriber::{ZmqTestRpcSubscriber, ZmqTestSubscriber};

/// Shared state of one in-process broker: the router, the harness coordinator, the side of the
/// endpoint the stand stands in for, the subscription holding it, and whether the transport is
/// still open.
#[derive(Debug)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    /// Whether the stand dials the endpoint: the side its `Role` names, read the way the socket
    /// brokers read it. Set by [`ZmqTestBroker::dialing`], which keeps the state a publisher or a
    /// clone taken earlier shares.
    dials: AtomicBool,
    /// The first subscription opened on the stand. On the bind side it stands for the
    /// subscription that binds the endpoint, which every publish of a queue then reaches; on the
    /// connect side, for the one that dials the peer that only sends.
    holder: OnceLock<String>,
    closed: AtomicBool,
}

impl TestState {
    fn new(side: Side) -> Self {
        Self {
            router: AddressRouter::default(),
            coordinator: OnceLock::new(),
            dials: AtomicBool::new(matches!(side, Side::Connect)),
            holder: OnceLock::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// The side of the endpoint the stand stands in for.
    pub(crate) fn side(&self) -> Side {
        if self.dials.load(Ordering::Acquire) {
            Side::Connect
        } else {
            Side::Bind
        }
    }

    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// The subscription holding the stand, once one has opened.
    pub(crate) fn holder(&self) -> Option<&str> {
        self.holder.get().map(String::as_str)
    }

    /// Marks the transport shut down. Set before the router is cleared, so no handle can slip a
    /// publish in between.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// The check every handle makes before touching the transport.
    ///
    /// The ladder makes owner-side misuse a compile error, but a publisher handed out earlier
    /// outlives the connection, and that handle must report the dead transport rather than
    /// succeed against it - the same answer, [`ZmqError::NotConnected`], that the real publishers
    /// give through their lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::NotConnected`] once the connected form has been shut down.
    pub(crate) fn ensure_open(&self) -> Result<(), ZmqError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ZmqError::NotConnected);
        }
        Ok(())
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: HeaderMap, routing: Routing) {
        self.router
            .publish(name, payload, headers, routing, self.coordinator());
    }
}

mod sealed {
    use crate::testing::router::Routing;

    pub trait Sealed {
        /// Which subscriptions a message from the pattern's foreign peer reaches: a PUSH or a
        /// DEALER hands it to one of the sockets that dialed it, a PUB to every one whose
        /// prefix matches.
        const FROM_PEER: Routing;
    }
}

/// Which socket pattern a stand stands in for.
///
/// Sealed, and the set is the crate's own: a fourth pattern would need a transport behind it
/// before it could have a stand.
pub trait TestPattern: sealed::Sealed + Send + Sync + 'static {
    /// How this pattern's stand names itself where a deployment names a coordinate, so a document
    /// built over a stand says which of the three it was built over.
    const STAND: &'static str;
}

/// PUSH/PULL, the pattern of [`ZmqQueue`](crate::ZmqQueue).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Queue;

/// PUB/SUB, the pattern of [`ZmqFanout`](crate::ZmqFanout).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Fanout;

/// DEALER/ROUTER, the pattern of [`ZmqRpc`](crate::ZmqRpc).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Rpc;

impl sealed::Sealed for Queue {
    const FROM_PEER: Routing = Routing::AnyConsumer;
}

impl sealed::Sealed for Fanout {
    const FROM_PEER: Routing = Routing::Prefix;
}

impl sealed::Sealed for Rpc {
    const FROM_PEER: Routing = Routing::AnyConsumer;
}

impl TestPattern for Queue {
    const STAND: &'static str = "ZmqTestBroker::queue";
}

impl TestPattern for Fanout {
    const STAND: &'static str = "ZmqTestBroker::fanout";
}

impl TestPattern for Rpc {
    const STAND: &'static str = "ZmqTestBroker::rpc";
}

/// An in-process stand-in for one of the crate's three patterns: no server, no sockets, and that
/// pattern's own answers.
///
/// A `ZeroMQ` deployment has no server in the middle, so a stand is a pattern rather than a
/// broker installation, and the constructor picks which: [`queue`](Self::queue),
/// [`fanout`](Self::fanout), [`rpc`](Self::rpc). Everything that differs between the three
/// differs here too - which subscriptions a publish reaches, which publish policy pairs, which
/// publisher the reply of an unnamed mount takes, whether a subscription can be batched, and
/// where a retry copy is addressed - so a routes file that compiles and starts against a stand
/// compiles and starts against the socket it stands in for.
///
/// `Role` is the side of the endpoint the stand stands in for, as on the socket brokers: a
/// constructor gives the [`Bind`] side, and [`dialing`](Self::dialing) the [`Connect`] one.
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::testing::ZmqTestBroker;
///
/// let worker = ZmqTestBroker::queue();
/// let watcher = ZmqTestBroker::fanout();
/// let responder = ZmqTestBroker::rpc();
/// # let _ = (worker, watcher, responder);
/// ```
#[must_use]
pub struct ZmqTestBroker<Pattern, Role = Bind> {
    state: Arc<TestState>,
    pattern: PhantomData<fn() -> (Pattern, Role)>,
}

impl<Pattern, Role: EndpointRole> ZmqTestBroker<Pattern, Role> {
    fn stand() -> Self {
        Self {
            state: Arc::new(TestState::new(Side::of::<Role>())),
            pattern: PhantomData,
        }
    }
}

impl<Pattern> ZmqTestBroker<Pattern, Bind> {
    /// The same stand on the side that dials the endpoint, as `ZmqEndpoint::connect` is.
    ///
    /// Synchronous and I/O-free. On this side a one-way subscription reads from a peer that only
    /// sends, so it addresses no retry copy, and once a subscription has opened the stand's
    /// publisher refuses every publish in the words the socket uses.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_zeromq::Connect;
    /// use ruststream_zeromq::testing::{Queue, ZmqTestBroker};
    ///
    /// let worker: ZmqTestBroker<Queue, Connect> = ZmqTestBroker::queue().dialing();
    /// # let _ = worker;
    /// ```
    pub fn dialing(self) -> ZmqTestBroker<Pattern, Connect> {
        // The same state: a publisher or a clone taken earlier sees the dialing side too, as the
        // handles of one socket broker share its endpoint.
        self.state.dials.store(true, Ordering::Release);
        ZmqTestBroker {
            state: self.state,
            pattern: PhantomData,
        }
    }
}

impl ZmqTestBroker<Queue> {
    /// A stand for the PUSH/PULL queue. Synchronous and I/O-free, like the real `new`.
    pub fn queue() -> Self {
        Self::stand()
    }
}

impl<Role> ZmqTestBroker<Queue, Role> {
    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::queue(Arc::clone(&self.state))
    }
}

impl ZmqTestBroker<Fanout> {
    /// A stand for the PUB/SUB fan-out. Synchronous and I/O-free, like the real `new`.
    pub fn fanout() -> Self {
        Self::stand()
    }
}

impl<Role> ZmqTestBroker<Fanout, Role> {
    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::fanout(Arc::clone(&self.state))
    }
}

impl ZmqTestBroker<Rpc> {
    /// A stand for the DEALER/ROUTER exchange. Synchronous and I/O-free, like the real `new`.
    pub fn rpc() -> Self {
        Self::stand()
    }
}

impl<Role> ZmqTestBroker<Rpc, Role> {
    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestRpcPublisher {
        ZmqTestRpcPublisher::new(Arc::clone(&self.state))
    }
}

// Written out rather than derived: the markers are type-level tags, and a derive would demand
// `Clone` and `Debug` from types that are never held.
impl<Pattern, Role> Clone for ZmqTestBroker<Pattern, Role> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            pattern: PhantomData,
        }
    }
}

impl<Pattern, Role> fmt::Debug for ZmqTestBroker<Pattern, Role> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqTestBroker").finish_non_exhaustive()
    }
}

impl<Pattern: TestPattern, Role: EndpointRole> DescribeServer for ZmqTestBroker<Pattern, Role> {
    /// A stand has no coordinate to attach to, so it describes itself as an in-process server
    /// over the `zeromq` protocol its pattern speaks, greeting with the ZMTP version the real
    /// implementation greets with.
    ///
    /// Everything below the server comes from the pattern itself, so a document built over a
    /// stand is the document the service ships apart from where it says to attach. That is what
    /// makes a document worth asserting under the harness.
    fn describe_server(&self) -> ServerSpec {
        let spec = ServerSpec::in_process("zeromq").protocol_version(ZMTP_VERSION);
        #[cfg(feature = "asyncapi")]
        let spec = spec.bindings(bindings::in_process_server(Pattern::STAND));
        spec
    }
}

impl<Pattern: TestPattern, Role: EndpointRole> Broker for ZmqTestBroker<Pattern, Role> {
    type Error = ZmqError;
    type Connected = ConnectedZmqTestBroker<Pattern, Role>;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedZmqTestBroker {
            state: self.state,
            pattern: PhantomData,
        }))
    }
}

/// The connected form of [`ZmqTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
pub struct ConnectedZmqTestBroker<Pattern, Role = Bind> {
    state: Arc<TestState>,
    pattern: PhantomData<fn() -> (Pattern, Role)>,
}

impl<Pattern, Role> ConnectedZmqTestBroker<Pattern, Role> {
    /// The subscription every stand opens; the pattern decides what it is wrapped in.
    ///
    /// The first one opened holds the stand, as the first subscription to attach holds a socket
    /// broker's endpoint: a publish through the stand is then refused where the socket refuses
    /// it.
    fn open(&self, name: &str) -> Result<ZmqTestSubscriber, ZmqError> {
        self.state.ensure_open()?;
        // A bound endpoint is one socket, so the stand of that side takes one subscription, as
        // the socket brokers do; the side that dials opens as many as the peer serves.
        if self.state.holder.set(name.to_owned()).is_err()
            && self.state.side() == Side::Bind
            && let Some(holder) = self.state.holder()
        {
            return Err(endpoint_taken(name, holder));
        }
        let (id, rx) = self.state.router.subscribe(name.to_owned());
        Ok(ZmqTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            self.state.coordinator().cloned(),
        ))
    }
}

impl<Pattern, Role> Clone for ConnectedZmqTestBroker<Pattern, Role> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            pattern: PhantomData,
        }
    }
}

impl<Pattern, Role> fmt::Debug for ConnectedZmqTestBroker<Pattern, Role> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedZmqTestBroker")
            .finish_non_exhaustive()
    }
}

impl<Role> ConnectedZmqTestBroker<Queue, Role> {
    /// A publisher into the queue: each message reaches one of the consumers on the destination.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::queue(Arc::clone(&self.state))
    }
}

impl<Role> ConnectedZmqTestBroker<Fanout, Role> {
    /// A publisher into the fan-out: each message reaches every subscription whose name is a
    /// prefix of the destination, and none when nothing matches.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::fanout(Arc::clone(&self.state))
    }
}

impl<Role> ConnectedZmqTestBroker<Rpc, Role> {
    /// A publisher for the request-reply exchange: it routes a reply to the address the request
    /// carried, and issues requests through [`RequestReply`](ruststream::RequestReply).
    #[must_use]
    pub fn publisher(&self) -> ZmqTestRpcPublisher {
        ZmqTestRpcPublisher::new(Arc::clone(&self.state))
    }
}

impl<Pattern: TestPattern, Role: EndpointRole> ConnectedBroker
    for ConnectedZmqTestBroker<Pattern, Role>
{
    type Error = ZmqError;
    type Closed = ();

    /// Closes the transport, then drops what it was carrying.
    ///
    /// The order matters: handles that outlived the connection see the closed transport rather
    /// than a router that is merely empty, which is the difference between an error and a publish
    /// that silently reaches nobody.
    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.close();
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedZmqTestBroker<Queue, Bind> {
    type Subscriber = ZmqTestSubscriber;

    /// What [`ConnectedZmqQueue<Bind>`](crate::ConnectedZmqQueue) declares: a publish under the
    /// subscribe name reaches the subscription, so a mount site binds the retry publisher and
    /// names no destination.
    type Copies = AddressedCopies;

    /// Opens the subscription, and the first one opened holds the stand: a publish through the
    /// queue's policy then reaches it under its own name and is refused under any other, the
    /// answer the socket gives once a subscription has bound the endpoint. A subscription under
    /// another name is refused as it opens, as its socket fails to bind the endpoint the first
    /// one holds.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        if let Some(holder) = self.state.holder.get()
            && holder != name
        {
            return ready(Err(ZmqError::Endpoint {
                endpoint: format!("the queue endpoint the subscription {holder:?} binds"),
                source: format!(
                    "the subscription {name:?} cannot bind it again: one queue endpoint carries \
                     one subscription name"
                )
                .into(),
            }));
        }
        let opened = self.open(name);
        if opened.is_ok() {
            self.state.holder.get_or_init(|| name.to_owned());
        }
        ready(opened)
    }
}

impl Subscribe for ConnectedZmqTestBroker<Queue, Connect> {
    type Subscriber = ZmqTestSubscriber;

    /// What [`ConnectedZmqQueue<Connect>`](crate::ConnectedZmqQueue) declares: the peer a
    /// dialing subscription reads from only pushes, so a mount site names where the copies go.
    type Copies = NamedCopies;

    /// Opens the subscription, and the first one opened holds the stand: a publish through the
    /// queue's policy is then refused, the answer the socket gives once a subscription has
    /// dialed the peer that only pushes.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

impl Subscribe for ConnectedZmqTestBroker<Fanout, Bind> {
    type Subscriber = ZmqTestSubscriber;

    /// What [`ConnectedZmqFanout<Bind>`](crate::ConnectedZmqFanout) declares, for the same
    /// reason: a name is a prefix of itself, so a publish under it comes back on the
    /// subscription.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

impl Subscribe for ConnectedZmqTestBroker<Fanout, Connect> {
    type Subscriber = ZmqTestSubscriber;

    /// What [`ConnectedZmqFanout<Connect>`](crate::ConnectedZmqFanout) declares: the peer a
    /// dialing subscription reads from only publishes, so a mount site names where the copies go.
    type Copies = NamedCopies;

    /// Opens the subscription, and the first one opened holds the stand: a publish through the
    /// fan-out's policy is then refused, the answer the socket gives once a subscription has
    /// dialed the peer that only publishes.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

impl<Role: EndpointRole> Subscribe for ConnectedZmqTestBroker<Rpc, Role> {
    /// A responder answers one request at a time, so this subscriber is deliberately no
    /// [`BatchSubscriber`](ruststream::BatchSubscriber): `.batch(..)` on a responder mount fails
    /// to compile here exactly as it fails against [`ZmqRpc`](crate::ZmqRpc).
    type Subscriber = ZmqTestRpcSubscriber;

    /// What [`ConnectedZmqRpc`](crate::ConnectedZmqRpc) declares: a copy of a request has no
    /// address of its own, so a mount site that binds a retry here names the destination or is
    /// refused before the subscription opens - under the stand and on a socket alike.
    type Copies = NamedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name).map(ZmqTestRpcSubscriber::new))
    }
}

impl<Pattern: TestPattern, Role: EndpointRole> TestableBroker
    for ConnectedZmqTestBroker<Pattern, Role>
{
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    /// Injects a message the way the pattern's foreign peer sends it.
    ///
    /// On the queue and the responder the peer is a PUSH or a DEALER socket, which hands each
    /// message to one of the sockets that dialed it whatever their names, so an injection reaches
    /// one of the stand's subscriptions, taken in turn. On the fan-out the peer is a PUB socket,
    /// so an injection reaches every subscription whose name is a prefix of the destination.
    ///
    /// It does not consult the closed flag the publishers consult: this is the harness reaching
    /// into the transport, not a handle a service holds, and it has no error to report through.
    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
            <Pattern as sealed::Sealed>::FROM_PEER,
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedZmqTestBroker<Queue, Bind>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Queue, Connect>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Fanout, Bind>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Fanout, Connect>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Rpc, Bind>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Rpc, Connect>);
