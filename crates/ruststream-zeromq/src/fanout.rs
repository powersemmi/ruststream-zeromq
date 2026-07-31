//! [`ZmqFanout`]: the PUB/SUB pattern - broadcast with prefix filtering by name.
//!
//! Honest scope, straight from the protocol: a subscriber that connects after a publisher has
//! started misses what was sent before it arrived (the slow joiner), and a message published
//! with no matching subscriber is dropped silently.

use std::sync::Arc;

use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, PairError,
    PublishPolicy, Publisher, ServerSpec, Subscribe,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use zeromq::prelude::*;
use zeromq::{PubSocket, SubSocket};

use crate::common::{DriverHandle, Lifecycle, SharedLifecycle, send_with_retry};
use crate::endpoint::ZmqEndpoint;
use crate::error::ZmqError;
use crate::message::ZmqMessage;
use crate::queue::ZmqSubscriber;
use crate::wire;

/// The PUB/SUB fan-out: each message reaches every subscriber whose name prefix matches.
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::{ZmqEndpoint, ZmqFanout};
///
/// let publisher_side = ZmqFanout::new(ZmqEndpoint::bind("tcp://*:5556"));
/// let subscriber_side = ZmqFanout::new(ZmqEndpoint::connect("tcp://events:5556"));
/// # let _ = (publisher_side, subscriber_side);
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct ZmqFanout {
    endpoint: ZmqEndpoint,
    cell: Arc<OnceCell<SharedLifecycle>>,
}

impl ZmqFanout {
    /// Records the endpoint. No I/O.
    pub fn new(endpoint: ZmqEndpoint) -> Self {
        Self {
            endpoint,
            cell: Arc::new(OnceCell::new()),
        }
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

impl Broker for ZmqFanout {
    type Error = ZmqError;
    type Connected = ConnectedZmqFanout;

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
            cell: self.cell,
        })
    }
}

impl DescribeServer for ZmqFanout {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(self.endpoint.address(), "zeromq")
    }
}

/// The connected form of [`ZmqFanout`].
#[derive(Debug)]
pub struct ConnectedZmqFanout {
    lifecycle: SharedLifecycle,
    cell: Arc<OnceCell<SharedLifecycle>>,
}

impl ConnectedZmqFanout {
    /// The address a local subscription resolved by binding (useful with an ephemeral
    /// `tcp://...:0` endpoint); `None` until a subscription has bound.
    #[must_use]
    pub fn bound_address(&self) -> Option<String> {
        self.lifecycle.resolved.get().cloned()
    }

    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqFanoutPublisher {
        ZmqFanoutPublisher {
            cell: Arc::clone(&self.cell),
            socket: Arc::new(Mutex::new(None)),
        }
    }
}

impl ConnectedBroker for ConnectedZmqFanout {
    type Error = ZmqError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.lifecycle
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

impl Subscribe for ConnectedZmqFanout {
    type Subscriber = ZmqSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.lifecycle.ensure_open()?;
        let mut socket = SubSocket::new();
        self.lifecycle.attach_receiver(&mut socket).await?;
        // The name frame doubles as the subscription prefix; filtering happens on the
        // publisher side, per the protocol.
        socket
            .subscribe(name)
            .await
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        let (tx, rx) = mpsc::unbounded_channel();
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
        Ok(ZmqSubscriber::from_parts(
            name.to_owned(),
            rx,
            DriverHandle { task },
        ))
    }
}

/// Publishes to the fan-out over a lazily attached PUB socket.
///
/// A message with no matching subscriber is dropped silently - that is the pattern's
/// contract, not an error.
#[derive(Clone)]
pub struct ZmqFanoutPublisher {
    cell: Arc<OnceCell<SharedLifecycle>>,
    socket: Arc<Mutex<Option<PubSocket>>>,
}

impl std::fmt::Debug for ZmqFanoutPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqFanoutPublisher").finish_non_exhaustive()
    }
}

impl Publisher for ZmqFanoutPublisher {
    type Error = ZmqError;

    // The socket guard intentionally spans the lazy attach and the send: the socket takes
    // &mut for every operation.
    #[allow(clippy::significant_drop_tightening)]
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let lifecycle = self.cell.get().ok_or(ZmqError::NotConnected)?;
        lifecycle.ensure_open()?;
        let mut guard = self.socket.lock().await;
        if guard.is_none() {
            let mut socket = PubSocket::new();
            lifecycle.attach_sender(&mut socket).await?;
            *guard = Some(socket);
        }
        let socket = guard.as_mut().expect("just attached");
        // PUB never reports "no peers": an unmatched message is dropped by design, so the
        // retry helper only smooths transport-level failures.
        send_with_retry(
            socket,
            msg.name(),
            wire::encode(msg.name(), msg.headers(), msg.payload()),
        )
        .await
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

impl PublishPolicy<ConnectedZmqFanout> for ZmqFanoutPublish {
    type Live = ZmqFanoutPublisher;

    async fn pair(self, connected: &ConnectedZmqFanout) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedZmqFanout {
    type Policy = ZmqFanoutPublish;
}
