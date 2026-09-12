//! The in-process publish pairs.
//!
//! The stand-in has no policy of its own: the three production policies ([`ZmqQueuePublish`],
//! [`ZmqFanoutPublish`], [`ZmqRpcPublish`]) pair against it, so a mount site keeps the policy the
//! service ships and a routes file compiles unchanged against either broker. Which live publisher
//! a policy pairs to is what carries the pattern's capability set: the one-way patterns publish,
//! and only the DEALER/ROUTER policy answers and asks.

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ruststream::{
    DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply,
};

use crate::error::ZmqError;
use crate::rpc::{REPLY_PREFIX, new_correlation_id, new_reply_address};
use crate::testing::broker::{ConnectedZmqTestBroker, TestState};
use crate::testing::router::Routing;
use crate::testing::subscriber::ZmqTestMessage;
use crate::{ZmqFanoutPublish, ZmqQueuePublish, ZmqRpcPublish};

/// Publisher for the one-way patterns on the in-process broker, carrying the delivery rule of
/// the pattern whose policy paired it.
///
/// A queue publisher hands each message to one of the consumers on the destination, taken in
/// turn, so a job mounted on two workers is worked once. A fan-out publisher hands it to every
/// subscription whose name is a prefix of the destination - the protocol's own filter - and to
/// none when nothing matches. Both are client-side selection, so they are reproduced rather than
/// approximated.
///
/// What is not reproduced is everything that depends on a peer existing: PUSH blocks and then
/// fails when no socket is connected to it, while a publish here is recorded and dropped, because
/// "a connected peer" is a socket in another process and has no counterpart in a channel.
/// Ordering guarantees, high-water marks and the slow joiner are transport behaviour too;
/// exercise them on the loopback suite.
#[derive(Debug, Clone)]
pub struct ZmqTestPublisher {
    state: Arc<TestState>,
    routing: Routing,
}

impl ZmqTestPublisher {
    pub(crate) fn queue(state: Arc<TestState>) -> Self {
        Self {
            state,
            routing: Routing::Competing,
        }
    }

    pub(crate) fn fanout(state: Arc<TestState>) -> Self {
        Self {
            state,
            routing: Routing::Prefix,
        }
    }

    fn route(&self, msg: &OutgoingMessage<'_>) -> Result<(), ZmqError> {
        self.state.ensure_open()?;
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
            self.routing,
        );
        Ok(())
    }
}

impl Publisher for ZmqTestPublisher {
    type Error = ZmqError;

    /// Routes `msg` by this publisher's pattern rule.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::NotConnected`] once the transport this handle aliases has been shut
    /// down, rather than routing into a dead broker.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route(&msg))
    }
}

/// Publisher for the DEALER/ROUTER pattern on the in-process broker: it answers requests and, as
/// [`RequestReply`], issues them.
///
/// # Examples
///
/// ```
/// use ruststream::Broker;
/// use ruststream_zeromq::testing::ZmqTestBroker;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = ZmqTestBroker::new().connect().await?;
/// let requester = connected.rpc_publisher();
/// # let _ = requester;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ZmqTestRpcPublisher {
    state: Arc<TestState>,
}

impl ZmqTestRpcPublisher {
    pub(crate) fn new(state: Arc<TestState>) -> Self {
        Self { state }
    }

    /// The reply leg, shared by the sync and async entry points.
    fn route_reply(&self, msg: &OutgoingMessage<'_>) -> Result<(), ZmqError> {
        self.state.ensure_open()?;
        if !msg.name().starts_with(REPLY_PREFIX) {
            return Err(ZmqError::Send {
                name: msg.name().to_owned(),
                reason: format!(
                    "the rpc publisher routes '{REPLY_PREFIX}...' replies; use request() for outbound requests"
                ),
            });
        }
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
            Routing::Exact,
        );
        Ok(())
    }
}

impl Publisher for ZmqTestRpcPublisher {
    type Error = ZmqError;

    /// Routes a reply to the address the request carried, and refuses anything else.
    ///
    /// The destination check is the socket publisher's, verbatim: over a ROUTER the name is the
    /// peer identity to answer, so a reply that kept the literal destination from the handler's
    /// decorator has nowhere to go. Accepting it here would let a responder mounted without a
    /// reply-routing transform pass in process and fail on deployment.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::Send`] when the destination is not a reply address, and
    /// [`ZmqError::NotConnected`] once the transport this handle aliases has been shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route_reply(&msg))
    }
}

/// Reproduces the exchange a DEALER performs: the request carries a `reply-to` inbox and a
/// `correlation-id`, and only a reply echoing that id resolves it.
///
/// What a channel cannot stand in for is stated where it bites. There is no socket, so nothing
/// here fails the way a connect fails: `timeout` covers the wait for an answer alone, never
/// reaching a peer. The inbox is an ordinary address in the router rather than a live DEALER, so
/// a reply nobody is waiting for is recorded in the published log instead of being dropped by the
/// transport, and a reply routed while no responder subscription exists is silently discarded
/// where the socket publisher reports that no ROUTER is attached. Delivery guarantees,
/// back-pressure and the slow joiner are transport behaviour and are not reproduced at all;
/// exercise them on the loopback suite.
impl RequestReply for ZmqTestRpcPublisher {
    type Reply = ZmqTestMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        // Checked before the inbox is minted: a handle that outlived the transport reports the
        // dead connection, rather than waiting out a timeout nothing could ever answer.
        self.state.ensure_open()?;
        let inbox = new_reply_address();
        let (id, mut rx) = self.state.router.subscribe(inbox.clone());

        // A caller-supplied correlation id is respected, the way the socket publisher respects
        // it, so an upper layer that matches on its own identifier sees it on both.
        let correlation = msg
            .headers()
            .correlation_id()
            .map_or_else(new_correlation_id, str::to_owned);
        let mut headers = msg.headers().clone();
        headers.insert("reply-to", inbox);
        headers.insert("correlation-id", correlation.clone());
        // A request reaches one responder, the way a DEALER picks one connected ROUTER.
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            headers,
            Routing::Competing,
        );

        let correlated = async {
            loop {
                let delivery = rx.recv().await?;
                if delivery.headers.correlation_id() == Some(correlation.as_str()) {
                    return Some(delivery);
                }
            }
        };
        let delivery = tokio::time::timeout(timeout, correlated)
            .await
            .ok()
            .flatten();
        // The inbox lives exactly as long as the request: a late answer then reaches no
        // subscription, as one arriving at a closed DEALER reaches no socket.
        self.state.router.unsubscribe(id);

        Ok(ZmqTestMessage::from_reply(
            delivery.ok_or(ZmqError::RequestTimeout)?,
        ))
    }
}

/// The PUSH/PULL policy pairs against the stand-in, so a `ZmqQueue` mount site runs under the
/// harness as written, and competing consumers still compete: each message is worked once.
impl PublishPolicy<ConnectedZmqTestBroker> for ZmqQueuePublish {
    type Live = ZmqTestPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.queue_publisher()))
    }
}

/// The PUB/SUB policy pairs against the stand-in, keeping the pattern's prefix filter: a
/// subscription on `orders` sees `orders.eu.1`, and a message nothing matches is dropped.
impl PublishPolicy<ConnectedZmqTestBroker> for ZmqFanoutPublish {
    type Live = ZmqTestPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.fanout_publisher()))
    }
}

/// The DEALER/ROUTER policy pairs to the one live publisher that answers and asks, so the
/// capability split the real forms have survives into the harness: a handler binding
/// `Out<impl RequestReply, ..>` mounts on this policy and on no other, exactly as in production.
impl PublishPolicy<ConnectedZmqTestBroker> for ZmqRpcPublish {
    type Live = ZmqTestRpcPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.rpc_publisher()))
    }
}

/// The reply of a mount that names no policy takes the queue rule.
///
/// One stand-in covers three patterns but a connected broker names one default, so this is the
/// single place the harness cannot follow the pattern a service actually runs on: a fan-out
/// service whose mount omits `.out(Reply, ..)` gets its own policy in production and this one
/// here. Dropping the impl instead would be worse - a mount that compiles in production would
/// stop compiling under the harness - so the fix at a mount site is to name
/// [`ZmqFanoutPublish`], which is what a fan-out reply wants stated anyway.
impl DefaultPublish for ConnectedZmqTestBroker {
    type Policy = ZmqQueuePublish;
}
