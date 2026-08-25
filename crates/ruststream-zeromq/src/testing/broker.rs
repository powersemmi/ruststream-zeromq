//! [`ZmqTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::ZmqError;
use crate::testing::router::AddressRouter;
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

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::Headers) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }
}

/// An in-process stand-in for [`ZmqQueue`](crate::ZmqQueue): same core routing, no server.
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

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher {
            state: Arc::clone(&self.state),
        }
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
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqTestPublisher {
        ZmqTestPublisher {
            state: Arc::clone(&self.state),
        }
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
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        ready(Ok(ZmqTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )))
    }
}

impl TestableBroker for ConnectedZmqTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedZmqTestBroker);

/// Publisher for the in-process broker.
#[derive(Debug, Clone)]
pub struct ZmqTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for ZmqTestPublisher {
    type Error = ZmqError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
    }
}

/// The publish policy for [`ZmqTestPublisher`].
///
/// Mirrors the real brokers' policies ([`ZmqQueuePublish`](crate::ZmqQueuePublish),
/// [`ZmqFanoutPublish`](crate::ZmqFanoutPublish), [`ZmqRpcPublish`](crate::ZmqRpcPublish)).
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::testing::ZmqTestPublish;
///
/// let policy = ZmqTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct ZmqTestPublish;

impl PublishPolicy<ConnectedZmqTestBroker> for ZmqTestPublish {
    type Live = ZmqTestPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedZmqTestBroker {
    type Policy = ZmqTestPublish;
}
