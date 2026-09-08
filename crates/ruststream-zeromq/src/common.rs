//! Machinery shared by the three socket patterns.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::Stream;
use ruststream::Subscriber;
use tokio::sync::{OnceCell, mpsc};
use zeromq::prelude::*;
use zeromq::{Socket, ZmqError as WireError};

use crate::endpoint::{Role, ZmqEndpoint};
use crate::error::{ZmqError, box_err};
use crate::message::ZmqMessage;

/// How long a send retries while the ZMTP handshake settles: the implementation returns the
/// message immediately when no peer is attached yet, which is routine right after `connect`.
pub(crate) const SEND_RETRY_WINDOW: Duration = Duration::from_secs(5);
pub(crate) const SEND_RETRY_STEP: Duration = Duration::from_millis(50);

/// How long a partial batch waits for more deliveries after its first one.
///
/// ZMTP carries one multipart message per receive, so batches are assembled on the client and this
/// deadline is the crate's own choice - the batch size is not, it arrives per subscription. Twenty
/// milliseconds coalesces a burst that is already queued behind the socket while costing an idle
/// subscription far less than the round trip it is waiting on anyway.
pub(crate) const BATCH_MAX_WAIT: Duration = Duration::from_millis(20);

/// Shared lifecycle state: the endpoint, the address a local subscription resolved by
/// binding (which is what a same-process publisher dials for the loopback arrangement), and
/// the closed flag aliased handles trip over.
#[derive(Debug)]
pub(crate) struct Lifecycle {
    pub(crate) endpoint: ZmqEndpoint,
    pub(crate) resolved: OnceCell<String>,
    pub(crate) closed: AtomicBool,
}

impl Lifecycle {
    pub(crate) fn new(endpoint: ZmqEndpoint) -> Self {
        Self {
            endpoint,
            resolved: OnceCell::new(),
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn ensure_open(&self) -> Result<(), ZmqError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ZmqError::NotConnected);
        }
        Ok(())
    }

    /// Attaches a receiving socket per the endpoint's role, recording the resolved address on
    /// bind so a same-process publisher can dial it.
    pub(crate) async fn attach_receiver<S: Socket>(&self, socket: &mut S) -> Result<(), ZmqError> {
        match self.endpoint.role {
            Role::Bind => {
                let resolved =
                    socket
                        .bind(self.endpoint.address())
                        .await
                        .map_err(|e| ZmqError::Endpoint {
                            endpoint: self.endpoint.address().to_owned(),
                            source: box_err(e),
                        })?;
                let _ = self.resolved.set(resolved.to_string());
            }
            Role::Connect => {
                socket
                    .connect(self.endpoint.address())
                    .await
                    .map_err(|e| ZmqError::Endpoint {
                        endpoint: self.endpoint.address().to_owned(),
                        source: box_err(e),
                    })?;
            }
        }
        Ok(())
    }

    /// The address a sending socket should use: the endpoint itself when dialing out, or the
    /// locally bound address for the loopback arrangement (a subscription in this process
    /// bound the listener).
    pub(crate) fn sender_address(&self) -> Result<(String, Role), ZmqError> {
        match self.endpoint.role {
            Role::Connect => Ok((self.endpoint.address().to_owned(), Role::Connect)),
            Role::Bind => self.resolved.get().map_or_else(
                || Ok((self.endpoint.address().to_owned(), Role::Bind)),
                |resolved| Ok((resolved.clone(), Role::Connect)),
            ),
        }
    }

    /// Attaches a sending socket per [`sender_address`](Self::sender_address).
    pub(crate) async fn attach_sender<S: Socket>(&self, socket: &mut S) -> Result<(), ZmqError> {
        let (address, role) = self.sender_address()?;
        let outcome = match role {
            Role::Bind => socket.bind(&address).await.map(|_| ()),
            Role::Connect => socket.connect(&address).await,
        };
        outcome.map_err(|e| ZmqError::Endpoint {
            endpoint: address,
            source: box_err(e),
        })
    }
}

/// Sends with a bounded retry while the handshake settles; `ReturnToSender` hands the message
/// back, so nothing is lost by retrying.
pub(crate) async fn send_with_retry<S: SocketSend>(
    socket: &mut S,
    name: &str,
    message: zeromq::ZmqMessage,
) -> Result<(), ZmqError> {
    let mut pending = message;
    let deadline = tokio::time::Instant::now() + SEND_RETRY_WINDOW;
    loop {
        match socket.send(pending).await {
            Ok(()) => return Ok(()),
            Err(WireError::ReturnToSender { message, .. }) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ZmqError::Send {
                        name: name.to_owned(),
                        reason: "no connected peer".to_owned(),
                    });
                }
                pending = message;
                tokio::time::sleep(SEND_RETRY_STEP).await;
            }
            Err(err) => {
                return Err(ZmqError::Send {
                    name: name.to_owned(),
                    reason: err.to_string(),
                });
            }
        }
    }
}

/// A subscriber handle over a driver task: the socket lives in the task (every operation
/// takes `&mut self`), and dropping the handle aborts it, which is the only reliable teardown
/// - a receive on a peerless socket pends forever by design of the implementation.
pub(crate) struct DriverHandle {
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl Drop for DriverHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The socket side of any subscription: the driver task's channel, one delivery at a time, which
/// is all a receive on a ZMTP socket yields.
///
/// The public subscribers wrap it - the batching patterns through the framework's client-side
/// buffer, the request-reply one directly.
pub(crate) struct WireSubscriber {
    pub(crate) rx: mpsc::UnboundedReceiver<Result<ZmqMessage, ZmqError>>,
    pub(crate) _driver: DriverHandle,
}

impl Subscriber for WireSubscriber {
    type Message = ZmqMessage;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<ZmqMessage, ZmqError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

pub(crate) type SharedLifecycle = Arc<Lifecycle>;
