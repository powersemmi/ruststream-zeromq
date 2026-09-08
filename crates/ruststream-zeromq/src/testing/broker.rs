//! [`ZmqTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, RawMessage, Subscribe};

use crate::error::ZmqError;
use crate::testing::publisher::{ZmqTestPublisher, ZmqTestRpcPublisher};
use crate::testing::router::{AddressRouter, Routing};
use crate::testing::subscriber::ZmqTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
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

/// An in-process stand-in for the crate's three patterns: no server, no sockets, and each
/// pattern's own delivery rule.
///
/// One broker covers all three because a `ZeroMQ` deployment has no server in the middle either;
/// what differs between them is which subscriptions a publish reaches, and that travels with the
/// publish policy a mount site names ([`ZmqQueuePublish`](crate::ZmqQueuePublish),
/// [`ZmqFanoutPublish`](crate::ZmqFanoutPublish), [`ZmqRpcPublish`](crate::ZmqRpcPublish)).
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::testing::ZmqTestBroker;
///
/// let broker = ZmqTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct ZmqTestBroker {
    state: Arc<TestState>,
}

impl ZmqTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A queue publisher usable before `connect`, mirroring the real broker's early-publisher
    /// path.
    #[must_use]
    pub fn queue_publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::queue(Arc::clone(&self.state))
    }
}

impl Broker for ZmqTestBroker {
    type Error = ZmqError;
    type Connected = ConnectedZmqTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedZmqTestBroker { state: self.state }))
    }
}

/// The connected form of [`ZmqTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedZmqTestBroker {
    state: Arc<TestState>,
}

impl ConnectedZmqTestBroker {
    /// A publisher into the queue: each message reaches one of the consumers on the destination.
    #[must_use]
    pub fn queue_publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::queue(Arc::clone(&self.state))
    }

    /// A publisher into the fan-out: each message reaches every subscription whose name is a
    /// prefix of the destination, and none when nothing matches.
    #[must_use]
    pub fn fanout_publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher::fanout(Arc::clone(&self.state))
    }

    /// A publisher for the request-reply exchange: it routes replies to the address a request
    /// carried, and issues requests through [`RequestReply`](ruststream::RequestReply).
    #[must_use]
    pub fn rpc_publisher(&self) -> ZmqTestRpcPublisher {
        ZmqTestRpcPublisher::new(Arc::clone(&self.state))
    }
}

impl ConnectedBroker for ConnectedZmqTestBroker {
    type Error = ZmqError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedZmqTestBroker {
    type Subscriber = ZmqTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (id, rx) = self.state.router.subscribe(name.to_owned());
        ready(Ok(ZmqTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            self.state.coordinator().cloned(),
        )))
    }
}

impl TestableBroker for ConnectedZmqTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    /// Injects a message the way a foreign peer would, by exact destination.
    ///
    /// Injection carries no pattern - a name is all the harness has - so it reaches every
    /// subscription spelled exactly like the destination. To exercise a pattern's own rule
    /// (competing consumers, prefix filtering), publish through that pattern's policy instead.
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

ruststream::register_testable_broker!(ConnectedZmqTestBroker);
