//! [`ZmqTestBroker`]: the in-process stands, one per socket pattern, and their connected forms.

use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DescribeServer, NamedCopies, OutgoingMessage,
    RawMessage, ServerSpec, Subscribe,
};

#[cfg(feature = "asyncapi")]
use crate::bindings;
use crate::endpoint::ZMTP_VERSION;
use crate::error::ZmqError;
use crate::testing::publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
use crate::testing::router::{AddressRouter, Routing};
use crate::testing::subscriber::{ZmqTestRpcSubscriber, ZmqTestSubscriber};

/// Shared state of one in-process broker: the router, the harness coordinator, the subscription
/// holding a queue stand, and whether the transport is still open.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    /// The first subscription opened on a queue stand. It stands for the subscription that binds
    /// the endpoint on a socket, which every publish of the same broker then reaches.
    holder: OnceLock<String>,
    closed: AtomicBool,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// The subscription holding a queue stand, once one has opened.
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

    pub(crate) fn publish(
        &self,
        name: &str,
        payload: Bytes,
        headers: ruststream::HeaderMap,
        routing: Routing,
    ) {
        self.router
            .publish(name, payload, headers, routing, self.coordinator());
    }
}

mod sealed {
    pub trait Sealed {}
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

impl sealed::Sealed for Queue {}
impl sealed::Sealed for Fanout {}
impl sealed::Sealed for Rpc {}
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
pub struct ZmqTestBroker<Pattern> {
    state: Arc<TestState>,
    pattern: PhantomData<fn() -> Pattern>,
}

impl<Pattern> ZmqTestBroker<Pattern> {
    fn stand() -> Self {
        Self {
            state: Arc::new(TestState::default()),
            pattern: PhantomData,
        }
    }
}

impl ZmqTestBroker<Queue> {
    /// A stand for the PUSH/PULL queue. Synchronous and I/O-free, like the real `new`.
    pub fn queue() -> Self {
        Self::stand()
    }

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

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestRpcPublisher {
        ZmqTestRpcPublisher::new(Arc::clone(&self.state))
    }
}

// Written out rather than derived: the marker is a type-level tag, and a derive would demand
// `Pattern: Clone` and `Pattern: Debug` from a type that is never held.
impl<Pattern> Clone for ZmqTestBroker<Pattern> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            pattern: PhantomData,
        }
    }
}

impl<Pattern> fmt::Debug for ZmqTestBroker<Pattern> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqTestBroker").finish_non_exhaustive()
    }
}

impl<Pattern: TestPattern> DescribeServer for ZmqTestBroker<Pattern> {
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

impl<Pattern: TestPattern> Broker for ZmqTestBroker<Pattern> {
    type Error = ZmqError;
    type Connected = ConnectedZmqTestBroker<Pattern>;

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
pub struct ConnectedZmqTestBroker<Pattern> {
    state: Arc<TestState>,
    pattern: PhantomData<fn() -> Pattern>,
}

impl<Pattern> ConnectedZmqTestBroker<Pattern> {
    /// The subscription every stand opens; the pattern decides what it is wrapped in.
    fn open(&self, name: &str) -> Result<ZmqTestSubscriber, ZmqError> {
        self.state.ensure_open().map(|()| {
            let (id, rx) = self.state.router.subscribe(name.to_owned());
            ZmqTestSubscriber::new(
                Arc::clone(&self.state),
                id,
                rx,
                self.state.coordinator().cloned(),
            )
        })
    }
}

impl<Pattern> Clone for ConnectedZmqTestBroker<Pattern> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            pattern: PhantomData,
        }
    }
}

impl<Pattern> fmt::Debug for ConnectedZmqTestBroker<Pattern> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedZmqTestBroker")
            .finish_non_exhaustive()
    }
}

impl ConnectedZmqTestBroker<Queue> {
    /// A publisher into the queue: each message reaches one of the consumers on the destination.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::queue(Arc::clone(&self.state))
    }
}

impl ConnectedZmqTestBroker<Fanout> {
    /// A publisher into the fan-out: each message reaches every subscription whose name is a
    /// prefix of the destination, and none when nothing matches.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::fanout(Arc::clone(&self.state))
    }
}

impl ConnectedZmqTestBroker<Rpc> {
    /// A publisher for the request-reply exchange: it routes a reply to the address the request
    /// carried, and issues requests through [`RequestReply`](ruststream::RequestReply).
    #[must_use]
    pub fn publisher(&self) -> ZmqTestRpcPublisher {
        ZmqTestRpcPublisher::new(Arc::clone(&self.state))
    }
}

impl<Pattern: TestPattern> ConnectedBroker for ConnectedZmqTestBroker<Pattern> {
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

impl Subscribe for ConnectedZmqTestBroker<Queue> {
    type Subscriber = ZmqTestSubscriber;

    /// What [`ConnectedZmqQueue`](crate::ConnectedZmqQueue) declares: a publish under the
    /// subscribe name reaches the subscription, so a mount site binds the retry publisher and
    /// names no destination.
    type Copies = AddressedCopies;

    /// Opens the subscription, and the first one opened holds the stand: a publish through the
    /// queue's policy then reaches it under its own name and is refused under any other, the
    /// answer the socket gives once a subscription has bound the endpoint.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let opened = self.open(name);
        if opened.is_ok() {
            self.state.holder.get_or_init(|| name.to_owned());
        }
        ready(opened)
    }
}

impl Subscribe for ConnectedZmqTestBroker<Fanout> {
    type Subscriber = ZmqTestSubscriber;

    /// What [`ConnectedZmqFanout`](crate::ConnectedZmqFanout) declares, for the same reason: a
    /// name is a prefix of itself, so a publish under it comes back on the subscription.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

impl Subscribe for ConnectedZmqTestBroker<Rpc> {
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

impl<Pattern: TestPattern> TestableBroker for ConnectedZmqTestBroker<Pattern> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    /// Injects a message the way a foreign peer would, by exact destination.
    ///
    /// Injection carries no pattern - a name is all the harness has - so it reaches every
    /// subscription spelled exactly like the destination. To exercise a pattern's own rule
    /// (competing consumers, prefix filtering), publish through that pattern's policy instead.
    ///
    /// It does not consult the closed flag the publishers consult: this is the harness reaching
    /// into the transport, not a handle a service holds, and it has no error to report through.
    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
            Routing::Exact,
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedZmqTestBroker<Queue>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Fanout>);
ruststream::register_testable_broker!(ConnectedZmqTestBroker<Rpc>);
