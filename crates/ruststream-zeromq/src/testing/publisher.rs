//! The in-process publish pairs.
//!
//! A stand has no policy of its own: the three production policies ([`ZmqQueuePublish`],
//! [`ZmqFanoutPublish`], [`ZmqRpcPublish`]) pair against it, so a mount site keeps the policy the
//! service ships and a routes file compiles unchanged against either broker. Each pairs against
//! the stand of its own pattern and no other, the way it pairs against one connected form in
//! production, and which live publisher it pairs to carries that pattern's capability set: the
//! one-way patterns publish, and only the DEALER/ROUTER policy answers and asks.

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    BytesMut, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply,
    Str, Take,
};

#[cfg(feature = "asyncapi")]
use crate::bindings::{self, SocketPair};
use crate::error::ZmqError;
#[cfg(feature = "asyncapi")]
use crate::rpc::REPLY_ADDRESS_LOCATION;
use crate::rpc::{
    CORRELATION_ID_HEADER, REPLY_PREFIX, REPLY_TO_HEADER, new_correlation_id, new_reply_address,
};
use crate::testing::broker::{ConnectedZmqTestBroker, Fanout, Queue, Rpc, TestState};
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

    fn route(&self, msg: OutgoingMessage<'_, BytesMut>) -> Result<(), ZmqError> {
        self.state.ensure_open()?;
        let (name, payload, headers) = msg.into_parts();
        self.state
            .publish(name, payload.freeze(), headers, self.routing);
        Ok(())
    }
}

impl Publisher for ZmqTestPublisher {
    /// The same answer the socket publishers give: the delivery the stand records owns its
    /// payload.
    type Payload = Take;

    type Error = ZmqError;

    /// The transport's own answer: ZMTP has no per-message setting, so neither has the stand-in.
    type Options = ();

    /// Routes `msg` by this publisher's pattern rule.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::NotConnected`] once the transport this handle aliases has been shut
    /// down, rather than routing into a dead broker.
    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route(msg))
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
/// let connected = ZmqTestBroker::rpc().connect().await?;
/// let requester = connected.publisher();
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
    fn route_reply(&self, msg: OutgoingMessage<'_, BytesMut>) -> Result<(), ZmqError> {
        self.state.ensure_open()?;
        if !msg.name().starts_with(REPLY_PREFIX) {
            return Err(ZmqError::Send {
                name: msg.name().to_owned(),
                reason: format!(
                    "the rpc publisher routes '{REPLY_PREFIX}...' replies; use request() for outbound requests"
                ),
            });
        }
        let (name, payload, headers) = msg.into_parts();
        self.state
            .publish(name, payload.freeze(), headers, Routing::Exact);
        Ok(())
    }
}

impl Publisher for ZmqTestRpcPublisher {
    /// The same answer the socket publishers give: the delivery the stand records owns its
    /// payload.
    type Payload = Take;

    type Error = ZmqError;

    /// The transport's own answer: ZMTP has no per-message setting, so neither has the stand-in.
    type Options = ();

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
    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route_reply(msg))
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
        msg: OutgoingMessage<'_, BytesMut>,
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
        let (name, payload, mut headers) = msg.into_parts();
        headers.insert(Str::from_static(REPLY_TO_HEADER), inbox);
        headers.insert(Str::from_static(CORRELATION_ID_HEADER), correlation.clone());
        // A request reaches one responder, the way a DEALER picks one connected ROUTER.
        self.state
            .publish(name, payload.freeze(), headers, Routing::Competing);

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

/// The PUSH/PULL policy pairs against the queue stand and against no other, exactly as it pairs
/// against [`ConnectedZmqQueue`](crate::ConnectedZmqQueue) alone in production: competing
/// consumers still compete, so each message is worked once.
impl PublishPolicy<ConnectedZmqTestBroker<Queue>> for ZmqQueuePublish {
    type Live = ZmqTestPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker<Queue>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The stand describes the channel the pattern it stands in for describes, so a document
    /// built under the harness is the document the service ships apart from the server
    /// coordinate, which a stand has none of.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::PushPull, channel)
    }
}

/// The PUB/SUB policy pairs against the fan-out stand, keeping the pattern's prefix filter: a
/// subscription on `orders` sees `orders.eu.1`, and a message nothing matches is dropped.
impl PublishPolicy<ConnectedZmqTestBroker<Fanout>> for ZmqFanoutPublish {
    type Live = ZmqTestPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker<Fanout>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The stand describes the channel the pattern it stands in for describes, so a document
    /// built under the harness is the document the service ships apart from the server
    /// coordinate, which a stand has none of.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::PubSub, channel)
    }
}

/// The DEALER/ROUTER policy pairs against the responder stand, to the one live publisher that
/// answers and asks, so the capability split the real forms have survives into the harness: a
/// handler binding `Out<impl RequestReply, ..>` mounts on this policy and on no other.
impl PublishPolicy<ConnectedZmqTestBroker<Rpc>> for ZmqRpcPublish {
    type Live = ZmqTestRpcPublisher;

    fn pair(
        self,
        connected: &ConnectedZmqTestBroker<Rpc>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The stand describes the channel the pattern it stands in for describes, so a document
    /// built under the harness is the document the service ships apart from the server
    /// coordinate, which a stand has none of.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::channel(SocketPair::DealerRouter, channel)
    }

    /// The stand mints a reply address per request the way the ROUTER does, and carries it in the
    /// same header, so the document reports the same expression on both.
    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(REPLY_ADDRESS_LOCATION)
    }
}

/// Each stand names the default its pattern names, so a mount that omits `.out_reply(..)` takes
/// the publisher it would take in production rather than the queue's for want of anything better.
impl DefaultPublish for ConnectedZmqTestBroker<Queue> {
    type Policy = ZmqQueuePublish;
}

impl DefaultPublish for ConnectedZmqTestBroker<Fanout> {
    type Policy = ZmqFanoutPublish;
}

impl DefaultPublish for ConnectedZmqTestBroker<Rpc> {
    type Policy = ZmqRpcPublish;
}
