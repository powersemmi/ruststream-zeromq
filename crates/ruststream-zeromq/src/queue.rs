//! [`ZmqQueue`]: the PUSH/PULL pattern - competing consumers, round-robin.

/// The publish policy of this form, under the name every include site writes.
///
/// Every policy a form supports appears in its prelude under the prefix-free concept name, so
/// switching a service between forms, or between brokers, leaves the composition root untouched:
/// only the import at the top of the file changes. A concept name that is absent says the form
/// lacks that policy, the same statement the capability manifest makes one layer up. PUSH/PULL has
/// exactly one policy, so `Publish` is the whole vocabulary here.
///
/// The broker-prefixed original stays at the crate root, for a mixed-form file that names both.
pub use self::ZmqQueuePublish as Publish;

/// The imports a service on the PUSH/PULL queue writes, in one glob.
///
/// Carries the framework's prelude, the shared [`ZmqEndpoint`], this form's descriptor
/// [`ZmqQueue`], and its publish policy under the uniform name [`Publish`].
///
/// The capability manifest is empty: PUSH/PULL is a one-way pattern with no return path, no
/// transactions, no batch receive, no broker-side partitioning and no history, so this form
/// implements none of the framework's capability traits. That is a statement about the form, not a
/// gap.
///
/// Globbing two form preludes into one file makes `Publish` ambiguous: the first use of the name
/// is `E0659`, pointing at both globs. That file wants [`crate::prelude`] and qualified
/// `queue::Publish` instead.
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
/// #[derive(Serialize)]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", publish("results"))]
/// async fn handle(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
///         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
///         |b| {
///             b.include(handle).publisher(TypedPublisher::new(Publish));
///         },
///     )
/// }
/// ```
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use crate::endpoint::ZmqEndpoint;

    pub use super::{Publish, ZmqQueue};
    // No capability manifest: this form implements none of the framework's capability traits.
}

use std::sync::Arc;

use futures::Stream;
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, PairError,
    PublishPolicy, Publisher, ServerSpec, Subscribe, Subscriber,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use zeromq::prelude::*;
use zeromq::{PullSocket, PushSocket};

use crate::common::{DriverHandle, Lifecycle, SharedLifecycle, send_with_retry};
use crate::endpoint::ZmqEndpoint;
use crate::error::ZmqError;
use crate::message::ZmqMessage;
use crate::wire;

/// The PUSH/PULL queue: each message reaches one of the competing consumers.
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
#[derive(Debug, Clone)]
#[must_use]
pub struct ZmqQueue {
    endpoint: ZmqEndpoint,
    cell: Arc<OnceCell<SharedLifecycle>>,
}

impl ZmqQueue {
    /// Records the endpoint. No I/O.
    pub fn new(endpoint: ZmqEndpoint) -> Self {
        Self {
            endpoint,
            cell: Arc::new(OnceCell::new()),
        }
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

impl Broker for ZmqQueue {
    type Error = ZmqError;
    type Connected = ConnectedZmqQueue;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let lifecycle = self
            .cell
            .get_or_try_init(async || {
                self.endpoint.validate()?;
                Ok::<_, ZmqError>(Arc::new(Lifecycle::new(self.endpoint.clone())))
            })
            .await?
            .clone();
        Ok(ConnectedZmqQueue {
            lifecycle,
            cell: self.cell,
        })
    }
}

impl DescribeServer for ZmqQueue {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(self.endpoint.address(), "zeromq")
    }
}

/// The connected form of [`ZmqQueue`]; sockets attach lazily per subscription and publisher.
#[derive(Debug)]
pub struct ConnectedZmqQueue {
    lifecycle: SharedLifecycle,
    cell: Arc<OnceCell<SharedLifecycle>>,
}

impl ConnectedZmqQueue {
    /// The address a local subscription resolved by binding (useful with an ephemeral
    /// `tcp://...:0` endpoint); `None` until a subscription has bound.
    #[must_use]
    pub fn bound_address(&self) -> Option<String> {
        self.lifecycle.resolved.get().cloned()
    }

    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> ZmqQueuePublisher {
        ZmqQueuePublisher {
            cell: Arc::clone(&self.cell),
            push: Arc::new(Mutex::new(None)),
        }
    }
}

impl ConnectedBroker for ConnectedZmqQueue {
    type Error = ZmqError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.lifecycle
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

impl Subscribe for ConnectedZmqQueue {
    type Subscriber = ZmqSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.lifecycle.ensure_open()?;
        let mut socket = PullSocket::new();
        self.lifecycle.attach_receiver(&mut socket).await?;

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
        Ok(ZmqSubscriber {
            name: name.to_owned(),
            rx,
            _driver: DriverHandle { task },
        })
    }
}

/// A subscription on one of the transport's patterns; yields [`ZmqMessage`]s.
pub struct ZmqSubscriber {
    name: String,
    rx: mpsc::UnboundedReceiver<Result<ZmqMessage, ZmqError>>,
    _driver: DriverHandle,
}

impl std::fmt::Debug for ZmqSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqSubscriber")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl ZmqSubscriber {
    pub(crate) fn from_parts(
        name: String,
        rx: mpsc::UnboundedReceiver<Result<ZmqMessage, ZmqError>>,
        driver: DriverHandle,
    ) -> Self {
        Self {
            name,
            rx,
            _driver: driver,
        }
    }
}

impl Subscriber for ZmqSubscriber {
    type Message = ZmqMessage;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<ZmqMessage, ZmqError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

/// Publishes into the queue over a lazily attached PUSH socket.
#[derive(Clone)]
pub struct ZmqQueuePublisher {
    cell: Arc<OnceCell<SharedLifecycle>>,
    push: Arc<Mutex<Option<PushSocket>>>,
}

impl std::fmt::Debug for ZmqQueuePublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqQueuePublisher").finish_non_exhaustive()
    }
}

impl Publisher for ZmqQueuePublisher {
    type Error = ZmqError;

    // The socket guard intentionally spans the lazy attach and the send: the socket takes
    // &mut for every operation.
    #[allow(clippy::significant_drop_tightening)]
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let lifecycle = self.cell.get().ok_or(ZmqError::NotConnected)?;
        lifecycle.ensure_open()?;
        let mut push = self.push.lock().await;
        if push.is_none() {
            let mut socket = PushSocket::new();
            lifecycle.attach_sender(&mut socket).await?;
            *push = Some(socket);
        }
        let socket = push.as_mut().expect("just attached");
        send_with_retry(
            socket,
            msg.name(),
            wire::encode(msg.name(), msg.headers(), msg.payload()),
        )
        .await
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

impl PublishPolicy<ConnectedZmqQueue> for ZmqQueuePublish {
    type Live = ZmqQueuePublisher;

    async fn pair(self, connected: &ConnectedZmqQueue) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedZmqQueue {
    type Policy = ZmqQueuePublish;
}
