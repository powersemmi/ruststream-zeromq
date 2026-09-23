//! Machinery shared by the three socket patterns.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::Stream;
use ruststream::{Subscriber, nonzero};
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

/// How many deliveries a subscription reads off its socket ahead of the handler, unless its
/// descriptor names another bound: the receive high-water mark `ZeroMQ` itself gives a socket.
pub(crate) const DEFAULT_READ_AHEAD: NonZeroUsize = nonzero!(1000_usize);

/// The channel between a subscription's driver task and its handler, `read_ahead` deep.
///
/// Bounded, so a full channel stops the driver, the driver stops reading the socket, and the
/// socket holds the sender back: a handler slower than the wire slows the wire down instead of
/// accumulating the difference in memory. The bound is the connected form's own, taken from the
/// descriptor it was connected from, so clones sharing one lifecycle keep their own bounds.
pub(crate) fn delivery_channel(read_ahead: NonZeroUsize) -> (DeliverySender, DeliveryReceiver) {
    mpsc::channel(read_ahead.get())
}

/// Shared lifecycle state: the endpoint, the listener a local subscription bound (which is what a
/// same-process publisher dials for the loopback arrangement), and the closed flag aliased handles
/// trip over.
#[derive(Debug)]
pub(crate) struct Lifecycle {
    pub(crate) endpoint: ZmqEndpoint,
    listener: OnceCell<Listener>,
    pub(crate) closed: AtomicBool,
}

/// The socket a subscription in this process bound on the endpoint.
#[derive(Debug)]
pub(crate) struct Listener {
    /// The address the bind resolved to: the port the operating system chose for a `:0`
    /// endpoint.
    pub(crate) address: String,
    /// The subscription that bound it, which is what receives a message sent there.
    pub(crate) subscription: String,
}

/// Where a sending socket attaches.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SendTarget<'a> {
    /// Another process listens on the endpoint, so the socket dials it.
    Dial(&'a str),
    /// Nothing in this process holds the endpoint, so the socket listens there and peers dial it.
    Listen(&'a str),
    /// A subscription in this process bound the endpoint, so the socket dials the address that
    /// bind resolved to.
    Local(&'a Listener),
}

impl<'a> SendTarget<'a> {
    /// The address the socket attaches to.
    pub(crate) fn address(self) -> &'a str {
        match self {
            Self::Dial(address) | Self::Listen(address) => address,
            Self::Local(listener) => &listener.address,
        }
    }
}

impl Lifecycle {
    pub(crate) fn new(endpoint: ZmqEndpoint) -> Self {
        Self {
            endpoint,
            listener: OnceCell::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// The address a local subscription resolved by binding, or `None` until one has bound.
    pub(crate) fn bound_address(&self) -> Option<String> {
        self.listener.get().map(|listener| listener.address.clone())
    }

    pub(crate) fn ensure_open(&self) -> Result<(), ZmqError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ZmqError::NotConnected);
        }
        Ok(())
    }

    /// Attaches a receiving socket for `subscription` per the endpoint's role, recording the
    /// listener on bind so a same-process publisher can dial it.
    pub(crate) async fn attach_receiver<S: Socket>(
        &self,
        socket: &mut S,
        subscription: &str,
    ) -> Result<(), ZmqError> {
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
                let _ = self.listener.set(Listener {
                    address: resolved.to_string(),
                    subscription: subscription.to_owned(),
                });
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

    /// Where a sending socket attaches: the endpoint itself when dialing out or when nothing in
    /// this process holds it, or the listener a subscription in this process bound (the loopback
    /// arrangement).
    pub(crate) fn send_target(&self) -> SendTarget<'_> {
        match self.endpoint.role {
            Role::Connect => SendTarget::Dial(self.endpoint.address()),
            Role::Bind => self.listener.get().map_or_else(
                || SendTarget::Listen(self.endpoint.address()),
                SendTarget::Local,
            ),
        }
    }

    /// Attaches a sending socket per [`send_target`](Self::send_target), and returns the local
    /// listener it dialed, if it dialed one.
    pub(crate) async fn attach_sender<S: Socket>(
        &self,
        socket: &mut S,
    ) -> Result<Option<&Listener>, ZmqError> {
        let target = self.send_target();
        let outcome = match target {
            SendTarget::Listen(address) => socket.bind(address).await.map(|_| ()),
            SendTarget::Dial(address) => socket.connect(address).await,
            SendTarget::Local(listener) => socket.connect(&listener.address).await,
        };
        outcome.map_err(|e| ZmqError::Endpoint {
            endpoint: target.address().to_owned(),
            source: box_err(e),
        })?;
        Ok(match target {
            SendTarget::Local(listener) => Some(listener),
            SendTarget::Dial(_) | SendTarget::Listen(_) => None,
        })
    }
}

/// The refusal of a queue publish that would come back to this service: the subscription that
/// bound the queue receives every message pushed into it, whatever its name, so a message named
/// anything else would arrive there as its next delivery.
///
/// The socket publisher and the in-process stand refuse in these words alike, so a test read
/// against the stand teaches the fix the deployment needs.
pub(crate) fn returns_to_subscription(name: &str, subscription: &str) -> ZmqError {
    ZmqError::Send {
        name: name.to_owned(),
        reason: format!(
            "this service's subscription '{subscription}' binds the queue and receives every \
             message pushed into it; publish '{name}' through a queue on an endpoint of its own"
        ),
    }
}

/// Sends with a bounded retry while the handshake settles; `ReturnToSender` hands the message
/// back, so nothing is lost by retrying.
///
/// The window is the time the peer is given to appear, so it opens at the first refusal rather
/// than at the call: a send that is taken keeps the clock out of the publish path entirely.
pub(crate) async fn send_with_retry<S: SocketSend>(
    socket: &mut S,
    name: &str,
    message: zeromq::ZmqMessage,
) -> Result<(), ZmqError> {
    let mut pending = message;
    let mut deadline = None;
    loop {
        match socket.send(pending).await {
            Ok(()) => return Ok(()),
            Err(WireError::ReturnToSender { message, .. }) => {
                match deadline {
                    None => deadline = Some(tokio::time::Instant::now() + SEND_RETRY_WINDOW),
                    Some(at) if tokio::time::Instant::now() >= at => {
                        return Err(ZmqError::Send {
                            name: name.to_owned(),
                            reason: "no connected peer".to_owned(),
                        });
                    }
                    Some(_) => {}
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

/// The driver task's end of a subscription's delivery channel.
pub(crate) type DeliverySender = mpsc::Sender<Result<ZmqMessage, ZmqError>>;

/// The handler's end of a subscription's delivery channel.
pub(crate) type DeliveryReceiver = mpsc::Receiver<Result<ZmqMessage, ZmqError>>;

/// The socket side of any subscription: the driver task's channel, one delivery at a time, which
/// is all a receive on a ZMTP socket yields.
///
/// The public subscribers wrap it - the batching patterns through the framework's client-side
/// buffer, the request-reply one directly.
pub(crate) struct WireSubscriber {
    pub(crate) rx: DeliveryReceiver,
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::time::advance;
    use zeromq::ZmqResult;

    use super::*;

    /// A socket that refuses the first send the way a peerless one does, after taking `stall` off
    /// the clock: the handshake a freshly dialled socket is still settling takes time of its own.
    struct Handshaking {
        refusals: usize,
        stall: Duration,
        stalled: bool,
    }

    #[async_trait]
    impl SocketSend for Handshaking {
        async fn send(&mut self, message: zeromq::ZmqMessage) -> ZmqResult<()> {
            if self.refusals == 0 {
                return Ok(());
            }
            if !self.stalled {
                self.stalled = true;
                advance(self.stall).await;
            }
            self.refusals -= 1;
            Err(WireError::ReturnToSender {
                reason: "no peer",
                message,
            })
        }
    }

    /// The window is the time the peer is given to appear, so it starts when the peer first
    /// refuses. A send that took longer than the window to reach that refusal has not used the
    /// window up - and a send that succeeds asks the clock nothing at all.
    #[tokio::test(start_paused = true)]
    async fn the_retry_window_starts_when_the_peer_first_refuses() {
        let mut socket = Handshaking {
            refusals: 1,
            stall: SEND_RETRY_WINDOW + Duration::from_secs(1),
            stalled: false,
        };

        send_with_retry(
            &mut socket,
            "orders",
            zeromq::ZmqMessage::from(Bytes::from_static(b"{}")),
        )
        .await
        .expect("the peer refused once and then took the message");
    }
}
