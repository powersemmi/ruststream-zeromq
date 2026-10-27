//! The in-process mode, behind the `testing` feature: each production broker of this crate runs
//! inside the test process, over a transport that carries the frames a socket would carry.
//!
//! A broker connected in process keeps its own connected form, lifecycle bookkeeping and publish
//! policies; only the socket underneath is replaced. A publish is framed the way the socket
//! publisher frames it, and a subscription reads the frames the way its socket's driver reads
//! them, so a header the wire cannot carry is refused and a request is stamped with its peer
//! exactly as over ZMTP. Which subscriptions a message reaches is the socket pattern's own rule,
//! named once in [`Pick`].
//!
//! Each broker is a transport of its own. A message that leaves for a peer outside the broker
//! (a queue this service dials, a listener it binds before any subscription) is recorded in the
//! published log and reaches no subscription, since that peer is another process.

use std::any::TypeId;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
#[cfg(feature = "json")]
use ruststream::codec::JsonCodec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{OutgoingMessage, RawMessage};
use tokio::sync::mpsc;
use zeromq::ZmqError as WireError;
use zeromq::ZmqMessage as WireMessage;

use crate::common::Lifecycle;
use crate::endpoint::{Bind, Connect, EndpointRole, Side};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::envelope::Framed;
use crate::envelope::{self, Envelope, Frames, NoOp, Pattern, frames};
use crate::error::ZmqError;
use crate::fanout::{ConnectedZmqFanout, ZmqFanout};
use crate::message::ZmqMessage;
use crate::queue::{ConnectedZmqQueue, ZmqQueue};
use crate::rpc::{ConnectedZmqRpc, REPLY_PREFIX, ZmqRpc, answers, peer_identity};

/// The channel a subscription reads its deliveries from, the same one a socket driver feeds.
pub(crate) type Deliveries<Env> = mpsc::UnboundedReceiver<Result<ZmqMessage<Env>, ZmqError>>;

/// Hands a subscription the frames it receives: read the way its socket's driver reads them,
/// counted with the harness's coordinator, and sent to the subscription's channel. Erased, since
/// the subscriptions of one transport may read through different envelopes.
type Deliver = Arc<dyn Fn(WireMessage, Option<&Coordinator>) + Send + Sync>;

/// Which subscriptions of a broker one message reaches: the socket pattern's own rule.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Pick<'a> {
    /// Every subscription. On an endpoint a subscription binds there is one, and its socket
    /// takes whatever a peer sends there, whatever the name frame says.
    All,
    /// One subscription, taken in turn: a PUSH or a DEALER hands each message to one of the
    /// sockets that dialed it, whatever the name frame says.
    OneInTurn,
    /// Every subscription whose name is a prefix of this name: the SUB filter.
    Prefix(&'a str),
    /// Every subscription whose name is a prefix of the message's first frame: the SUB filter
    /// over the frames a message is sent in.
    FirstFrame,
    /// None: the message leaves for a peer outside this broker.
    Nobody,
}

/// Who answers at a peer identity a request carried.
#[derive(Debug)]
enum Peer {
    /// A request of this service, waiting on its DEALER for the answer.
    Waiting(mpsc::UnboundedSender<WireMessage>),
    /// A peer outside this service that sent a request the test injected. It stays connected,
    /// so an answer to it is taken, and recorded.
    Foreign,
}

struct Subscription {
    id: u64,
    name: String,
    deliver: Deliver,
}

#[derive(Default)]
struct State {
    next_id: u64,
    /// In the order they opened, which is the order a peer takes them in turn.
    subscriptions: Vec<Subscription>,
    turn: usize,
    log: HashMap<String, Vec<RawMessage>>,
    peers: HashMap<Bytes, Peer>,
}

/// One broker's in-process transport: its subscriptions, the peers its requests wait at, and a
/// log of every message it carried.
#[derive(Default)]
pub(crate) struct Bus {
    state: Mutex<State>,
    coordinator: OnceLock<Coordinator>,
}

impl fmt::Debug for Bus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bus").finish_non_exhaustive()
    }
}

/// A subscription's place on the in-process transport, which its driver handle gives up when it
/// drops.
pub(crate) struct Registration {
    bus: Arc<Bus>,
    id: u64,
}

impl Registration {
    /// Takes the subscription off the transport: a message sent later does not reach it.
    pub(crate) fn give_up(&self) {
        self.bus
            .state()
            .subscriptions
            .retain(|sub| sub.id != self.id);
    }
}

/// The harness's count of one delivery in flight, handed back when the delivery goes.
///
/// A delivery is settled or dropped exactly once, and either way its `Drop` runs, so this is
/// where the count comes down. A delivery the harness does not drive carries none.
#[derive(Debug, Default)]
pub(crate) struct Release(Option<Coordinator>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.0 {
            coordinator.consumed();
        }
    }
}

impl Bus {
    fn state(&self) -> MutexGuard<'_, State> {
        // A panic under this lock leaves no invariant half-kept: every critical section is a
        // single insert, remove or lookup.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// Opens a subscription named `name`, whose deliveries are read with `read`; `None` is a
    /// message its socket's driver skips.
    pub(crate) fn subscribe<Env, Read, Item>(
        self: &Arc<Self>,
        name: &str,
        read: Read,
    ) -> (Deliveries<Env>, Registration)
    where
        Env: Envelope,
        Read: Fn(WireMessage) -> Item + Send + Sync + 'static,
        Item: Into<Option<Result<ZmqMessage<Env>, ZmqError>>>,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let deliver: Deliver = Arc::new(move |frames, coordinator| {
            let Some(delivery) = read(frames).into() else {
                return;
            };
            let delivery = match (delivery, coordinator) {
                (Ok(message), Some(coordinator)) => {
                    // Counted before it is handed over, so the harness never sees the delivery
                    // consumed before it saw it enqueued.
                    coordinator.enqueued();
                    Ok(message.counted(Release(Some(coordinator.clone()))))
                }
                (delivery, _) => delivery,
            };
            // A subscription that closed between the pick and the send takes nothing; its
            // delivery drops here and gives its count back.
            let _ = tx.send(delivery);
        });
        let mut state = self.state();
        let id = state.next_id;
        state.next_id += 1;
        state.subscriptions.push(Subscription {
            id,
            name: name.to_owned(),
            deliver,
        });
        drop(state);
        (
            rx,
            Registration {
                bus: Arc::clone(self),
                id,
            },
        )
    }

    /// Carries `frames`, published to `destination` through `envelope` over `pattern`, to the
    /// subscriptions `pick` names, behind the peer `identity` when a ROUTER is to read it, and
    /// records it in the log.
    pub(crate) fn send<Env: Envelope>(
        &self,
        (envelope, pattern): (&Env, Pattern),
        destination: &str,
        frames: WireMessage,
        identity: Option<&Bytes>,
        pick: Pick<'_>,
    ) -> usize {
        let mut state = self.state();
        record(&mut state, envelope, pattern, destination, &frames);
        let first = frames.get(0).map_or(&[][..], Bytes::as_ref);
        let chosen: Vec<_> = match pick {
            Pick::Nobody => Vec::new(),
            Pick::All => state.subscriptions.iter().collect(),
            Pick::Prefix(name) => state
                .subscriptions
                .iter()
                .filter(|sub| name.starts_with(sub.name.as_str()))
                .collect(),
            Pick::FirstFrame => state
                .subscriptions
                .iter()
                .filter(|sub| first.starts_with(sub.name.as_bytes()))
                .collect(),
            Pick::OneInTurn if state.subscriptions.is_empty() => Vec::new(),
            Pick::OneInTurn => {
                let at = state.turn % state.subscriptions.len();
                state.turn = state.turn.wrapping_add(1);
                vec![&state.subscriptions[at]]
            }
        };
        let chosen: Vec<Deliver> = chosen
            .into_iter()
            .map(|sub| Arc::clone(&sub.deliver))
            .collect();
        // The sends are the subscribers' business, not the registry's.
        drop(state);

        let mut received = frames;
        if let Some(identity) = identity {
            received.push_front(identity.clone());
        }
        let reached = chosen.len();
        for deliver in chosen {
            deliver(received.clone(), self.coordinator.get());
        }
        reached
    }

    /// Answers the peer whose identity leads `frames`, the way a ROUTER routes a reply: to a
    /// request of this service that is waiting, to a foreign peer that asked, and to nobody
    /// else.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::Send`] in the ROUTER's own words when no peer carries the identity.
    pub(crate) fn reply<Env: Envelope>(
        &self,
        envelope: &Env,
        destination: &str,
        frames: WireMessage,
    ) -> Result<(), ZmqError> {
        let mut frames = frames.into_vecdeque();
        let identity = frames.pop_front().unwrap_or_default();
        let frames = WireMessage::try_from(frames)
            .map_err(|_| ZmqError::Wire("a reply needs a frame behind the peer identity".into()))?;
        let mut state = self.state();
        let waiting = match state.peers.get(&identity) {
            Some(Peer::Waiting(inbox)) => Some(inbox.clone()),
            Some(Peer::Foreign) => {
                // A foreign peer the harness connected asks once, and leaves with its answer.
                state.peers.remove(&identity);
                None
            }
            None => {
                return Err(ZmqError::Send {
                    name: destination.to_owned(),
                    reason: WireError::Other("Destination client not found by identity")
                        .to_string(),
                });
            }
        };
        record(
            &mut state,
            envelope,
            Pattern::DealerRouter,
            destination,
            &frames,
        );
        drop(state);
        if let Some(inbox) = waiting {
            let _ = inbox.send(frames);
        }
        Ok(())
    }

    /// Connects a DEALER of this service at `identity`, and returns what it receives.
    pub(crate) fn open_inbox(&self, identity: Bytes) -> mpsc::UnboundedReceiver<WireMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.state().peers.insert(identity, Peer::Waiting(tx));
        rx
    }

    /// Disconnects the DEALER at `identity`: an answer that comes later reaches no peer.
    pub(crate) fn close_inbox(&self, identity: &Bytes) {
        self.state().peers.remove(identity);
    }

    /// Connects a foreign DEALER and returns its identity.
    fn foreign_peer(&self) -> Bytes {
        let identity = peer_identity();
        self.state().peers.insert(identity.clone(), Peer::Foreign);
        identity
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state().log.get(name).cloned().unwrap_or_default()
    }
}

/// Records what a peer reading `frames` through the `envelope` they were written in reads: the
/// payload as it was written, and the headers. A message no such peer could read is recorded as
/// its last frame.
fn record<Env: Envelope>(
    state: &mut State,
    envelope: &Env,
    pattern: Pattern,
    destination: &str,
    frames: &WireMessage,
) {
    let entry = match envelope.open(pattern, frames.clone().into_vecdeque()) {
        Ok(opened) => RawMessage::new(destination, opened.payload)
            .with_headers(Env::headers(&opened.head).unwrap_or_default()),
        Err(_) => RawMessage::new(
            destination,
            frames.iter().last().cloned().unwrap_or_default(),
        ),
    };
    state
        .log
        .entry(destination.to_owned())
        .or_default()
        .push(entry);
}

/// Builds the frames a peer that writes `envelope` sends for `message`.
///
/// A peer composing [`Frames`] by hand writes whatever bytes it likes into the header frame, so a
/// header this crate would refuse to write reaches the subscription as such a peer sends it.
///
/// # Panics
///
/// Panics when another envelope cannot carry the message, a header it has no place for: no peer
/// writing it could send the message the test injects.
fn foreign<Env: Envelope>(
    envelope: &Env,
    pattern: Pattern,
    message: &OutgoingMessage<'_>,
) -> WireMessage {
    envelope::write(
        envelope,
        pattern,
        message.name(),
        None,
        (
            message.name(),
            message.headers(),
            Bytes::copy_from_slice(message.payload()),
        ),
    )
    .unwrap_or_else(|err| {
        // The one layout with a frame a peer writes by hand; the check runs once per injected
        // message, in the test harness only.
        if TypeId::of::<Env>() == TypeId::of::<Frames>() {
            let frames = frames::foreign(
                message.name(),
                message.headers(),
                Bytes::copy_from_slice(message.payload()),
            );
            return WireMessage::try_from(frames)
                .expect("a list built from named frames is never empty");
        }
        panic!(
            "a peer writing the {} envelope cannot send the injected message: {err}",
            Env::NAME
        )
    })
}

/// The positions of the subscriptions `pick` reaches, answered from the names alone.
fn positions(subscriptions: &[&str], pick: Pick<'_>) -> Vec<usize> {
    match pick {
        Pick::Nobody | Pick::OneInTurn | Pick::FirstFrame => Vec::new(),
        Pick::All => (0..subscriptions.len()).collect(),
        Pick::Prefix(name) => subscriptions
            .iter()
            .enumerate()
            .filter(|(_, subscription)| name.starts_with(**subscription))
            .map(|(position, _)| position)
            .collect(),
    }
}

impl<Role: EndpointRole, Env: Envelope> InProcess for ZmqQueue<Role, Env> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::in_process).await
    }
}

impl<Role: EndpointRole, Env: Envelope> InProcess for ZmqFanout<Role, Env> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::in_process).await
    }
}

impl<Env: Envelope> InProcess for ZmqRpc<Env> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::in_process).await
    }
}

/// The queue's routing: a peer that pushes into an endpoint a subscription binds reaches that
/// subscription, and one that subscriptions dial hands each message to one of them in turn,
/// whatever its name frame says. A publish of this service reaches its own subscription only
/// through the listener that subscription bound.
impl<Role: EndpointRole, Env: Envelope> TestableBroker for ConnectedZmqQueue<Role, Env> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            let pick = match self.lifecycle().endpoint.side() {
                Side::Bind => Pick::All,
                Side::Connect => Pick::OneInTurn,
            };
            let frames = foreign(self.envelope(), Pattern::PushPull, &message);
            bus.send(
                (self.envelope(), Pattern::PushPull),
                message.name(),
                frames,
                None,
                pick,
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.lifecycle()
            .in_process_bus()
            .map(|bus| bus.published(name))
            .unwrap_or_default()
    }

    fn routes(&self, _destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let pick = match self.lifecycle().endpoint.side() {
            Side::Bind => Pick::All,
            // A publish here goes to the peer the subscriptions read from, which is not one of
            // them; a subscription that dialed refuses the publish outright.
            Side::Connect => Pick::Nobody,
        };
        positions(subscriptions, pick)
    }
}

/// The fan-out's routing: every subscription whose name prefixes the message's name, on either
/// side, and for a publish of this service only through the listener its subscription bound.
impl<Role: EndpointRole, Env: Envelope> TestableBroker for ConnectedZmqFanout<Role, Env> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            let frames = foreign(self.envelope(), Pattern::PubSub, &message);
            bus.send(
                (self.envelope(), Pattern::PubSub),
                message.name(),
                frames,
                None,
                Pick::FirstFrame,
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.lifecycle()
            .in_process_bus()
            .map(|bus| bus.published(name))
            .unwrap_or_default()
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let pick = match self.lifecycle().endpoint.side() {
            Side::Bind => Pick::Prefix(destination),
            Side::Connect => Pick::Nobody,
        };
        positions(subscriptions, pick)
    }
}

/// The exchange's routing: a request reaches the responder that binds the endpoint, or one of the
/// responders that dial it in turn, stamped with the peer that asked; an answer reaches that peer
/// and no subscription.
impl<Env: Envelope> TestableBroker for ConnectedZmqRpc<Env> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            let pick = match self.lifecycle().endpoint.side() {
                Side::Bind => Pick::All,
                Side::Connect => Pick::OneInTurn,
            };
            let identity = bus.foreign_peer();
            let frames = foreign(self.envelope(), Pattern::DealerRouter, &message);
            let sent = bus.send(
                (self.envelope(), Pattern::DealerRouter),
                message.name(),
                frames,
                Some(&identity),
                pick,
            );
            if sent == 0 {
                // No responder took the request, so no answer will come for the peer.
                bus.close_inbox(&identity);
            }
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.lifecycle()
            .in_process_bus()
            .map(|bus| bus.published(name))
            .unwrap_or_default()
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let pick = match self.lifecycle().endpoint.side() {
            Side::Bind if !destination.starts_with(REPLY_PREFIX) => Pick::All,
            Side::Bind | Side::Connect => Pick::Nobody,
        };
        positions(subscriptions, pick)
    }
}

/// Registers every broker type of this crate on the envelope `$envelope`.
macro_rules! register_envelope {
    ($envelope:ty) => {
        ruststream::register_testable_broker!(ZmqQueue<Bind, $envelope>);
        ruststream::register_testable_broker!(ZmqQueue<Connect, $envelope>);
        ruststream::register_testable_broker!(ZmqFanout<Bind, $envelope>);
        ruststream::register_testable_broker!(ZmqFanout<Connect, $envelope>);
        ruststream::register_testable_broker!(ZmqRpc<$envelope>);
    };
}

register_envelope!(Frames);
register_envelope!(NoOp);
#[cfg(feature = "json")]
register_envelope!(Framed<JsonCodec>);
#[cfg(feature = "cbor")]
register_envelope!(Framed<CborCodec>);
#[cfg(feature = "msgpack")]
register_envelope!(Framed<MsgpackCodec>);

/// A request of this service over the in-process transport: the request leaves the way the
/// DEALER sends it, reaches the responder this service binds when there is one, and waits for
/// the answer that echoes its correlation id.
pub(crate) async fn request<Env: Envelope>(
    envelope: &Env,
    bus: &Bus,
    local: bool,
    (name, frames): (&str, WireMessage),
    correlation: Option<&str>,
    timeout: Duration,
) -> Result<ZmqMessage<Env>, ZmqError> {
    let identity = peer_identity();
    let mut inbox = bus.open_inbox(identity.clone());
    // Disconnects the DEALER however the request ends, a caller dropping it included.
    let _connected = Connected {
        bus,
        identity: &identity,
    };
    let pick = if local { Pick::All } else { Pick::Nobody };
    let _ = bus.send(
        (envelope, Pattern::DealerRouter),
        name,
        frames,
        Some(&identity),
        pick,
    );
    let exchange = async {
        while let Some(reply) = inbox.recv().await {
            let reply =
                envelope::read(envelope, Pattern::DealerRouter, reply.into_vecdeque(), None)?;
            if answers(&reply, correlation) {
                return Ok(reply);
            }
        }
        Err(ZmqError::RequestTimeout)
    };
    tokio::time::timeout(timeout, exchange)
        .await
        .unwrap_or(Err(ZmqError::RequestTimeout))
}

/// A DEALER of this service connected at `identity` for one request.
struct Connected<'a> {
    bus: &'a Bus,
    identity: &'a Bytes,
}

impl Drop for Connected<'_> {
    fn drop(&mut self) {
        self.bus.close_inbox(self.identity);
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use ruststream::HeaderMap;

    use super::*;
    use crate::endpoint::ZmqEndpoint;

    impl Bus {
        fn peers(&self) -> usize {
            self.state().peers.len()
        }
    }

    #[tokio::test]
    async fn an_injected_request_no_responder_takes_leaves_no_peer() {
        let rpc = ZmqRpc::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
            .connect_in_process()
            .await
            .expect("connects in process");
        TestableBroker::inject(&rpc, OutgoingMessage::new("orders", b"x".as_slice()));
        let bus = rpc.lifecycle().in_process_bus().expect("in process");
        assert_eq!(bus.peers(), 0);
    }

    #[tokio::test]
    async fn a_request_dropped_while_it_waits_leaves_no_peer() {
        let bus = Bus::default();
        let frames = envelope::write(
            &Frames,
            Pattern::DealerRouter,
            "orders",
            None,
            ("orders", &HeaderMap::new(), Bytes::from_static(b"x")),
        )
        .expect("frames");
        let pending = request(
            &Frames,
            &bus,
            false,
            ("orders", frames),
            Some("c-1"),
            Duration::from_mins(1),
        )
        .now_or_never();
        assert!(pending.is_none(), "the request was still waiting");
        assert_eq!(bus.peers(), 0);
    }
}
