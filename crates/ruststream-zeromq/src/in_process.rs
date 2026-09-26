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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{OutgoingMessage, RawMessage};
use tokio::sync::mpsc;
use zeromq::ZmqError as WireError;
use zeromq::ZmqMessage as WireMessage;

use crate::common::Lifecycle;
use crate::endpoint::{Bind, Connect, EndpointRole, Side};
use crate::error::ZmqError;
use crate::fanout::{ConnectedZmqFanout, ZmqFanout};
use crate::message::ZmqMessage;
use crate::queue::{ConnectedZmqQueue, ZmqQueue};
use crate::rpc::{ConnectedZmqRpc, REPLY_PREFIX, ZmqRpc, peer_identity};
use crate::wire;

/// The channel a subscription reads its deliveries from, the same one a socket driver feeds.
pub(crate) type Deliveries = mpsc::UnboundedReceiver<Result<ZmqMessage, ZmqError>>;

/// How a subscription reads the frames it receives: the reader its socket's driver uses.
pub(crate) type Read = fn(WireMessage) -> Option<Result<ZmqMessage, ZmqError>>;

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
    deliveries: mpsc::UnboundedSender<Result<ZmqMessage, ZmqError>>,
    read: Read,
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

    /// Opens a subscription named `name`, whose deliveries are read with `read`.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        name: &str,
        read: Read,
    ) -> (Deliveries, Registration) {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut state = self.state();
        let id = state.next_id;
        state.next_id += 1;
        state.subscriptions.push(Subscription {
            id,
            name: name.to_owned(),
            deliveries: tx,
            read,
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

    /// Carries `frames`, published to `destination`, to the subscriptions `pick` names, behind
    /// the peer `identity` when a ROUTER is to read it, and records it in the log.
    pub(crate) fn send(
        &self,
        destination: &str,
        frames: WireMessage,
        identity: Option<&Bytes>,
        pick: Pick<'_>,
    ) -> usize {
        let mut state = self.state();
        record(&mut state, destination, &frames);
        let chosen: Vec<_> = match pick {
            Pick::Nobody => Vec::new(),
            Pick::All => state.subscriptions.iter().collect(),
            Pick::Prefix(name) => state
                .subscriptions
                .iter()
                .filter(|sub| name.starts_with(sub.name.as_str()))
                .collect(),
            Pick::OneInTurn if state.subscriptions.is_empty() => Vec::new(),
            Pick::OneInTurn => {
                let at = state.turn % state.subscriptions.len();
                state.turn = state.turn.wrapping_add(1);
                vec![&state.subscriptions[at]]
            }
        };
        let chosen: Vec<_> = chosen
            .into_iter()
            .map(|sub| (sub.deliveries.clone(), sub.read))
            .collect();
        // The sends are the subscribers' business, not the registry's.
        drop(state);

        let mut received = frames;
        if let Some(identity) = identity {
            received.push_front(identity.clone());
        }
        let reached = chosen.len();
        for (deliveries, read) in chosen {
            let Some(delivery) = read(received.clone()) else {
                continue;
            };
            let delivery = match (delivery, self.coordinator.get()) {
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
            let _ = deliveries.send(delivery);
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
    pub(crate) fn reply(&self, destination: &str, frames: WireMessage) -> Result<(), ZmqError> {
        let mut frames = frames.into_vecdeque();
        let identity = frames.pop_front().unwrap_or_default();
        let frames = WireMessage::try_from(frames)
            .map_err(|_| ZmqError::Wire("a reply needs name and payload frames".into()))?;
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
        record(&mut state, destination, &frames);
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

/// Records what a peer receiving `frames` reads: the payload frame as it was written, the headers
/// the frame decodes to. A message whose frames no peer could read is recorded as its bytes.
fn record(state: &mut State, destination: &str, frames: &WireMessage) {
    let entry = match wire::decode(frames.clone()) {
        Ok((_, headers, payload)) => RawMessage::new(destination, payload).with_headers(headers),
        Err(_) => RawMessage::new(destination, frames.get(2).cloned().unwrap_or_default()),
    };
    state
        .log
        .entry(destination.to_owned())
        .or_default()
        .push(entry);
}

/// Builds the frames a foreign peer sends for `message`.
fn foreign(message: &OutgoingMessage<'_>) -> WireMessage {
    wire::encode_foreign(
        message.name(),
        message.headers(),
        Bytes::copy_from_slice(message.payload()),
    )
}

/// The positions of the subscriptions `pick` reaches, answered from the names alone.
fn positions(subscriptions: &[&str], pick: Pick<'_>) -> Vec<usize> {
    match pick {
        Pick::Nobody | Pick::OneInTurn => Vec::new(),
        Pick::All => (0..subscriptions.len()).collect(),
        Pick::Prefix(name) => subscriptions
            .iter()
            .enumerate()
            .filter(|(_, subscription)| name.starts_with(**subscription))
            .map(|(position, _)| position)
            .collect(),
    }
}

impl<Role: EndpointRole> InProcess for ZmqQueue<Role> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::in_process).await
    }
}

impl<Role: EndpointRole> InProcess for ZmqFanout<Role> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::in_process).await
    }
}

impl InProcess for ZmqRpc {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connect_with(Lifecycle::in_process).await
    }
}

/// The queue's routing: a peer that pushes into an endpoint a subscription binds reaches that
/// subscription, and one that subscriptions dial hands each message to one of them in turn,
/// whatever its name frame says. A publish of this service reaches its own subscription only
/// through the listener that subscription bound.
impl<Role: EndpointRole> TestableBroker for ConnectedZmqQueue<Role> {
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
            bus.send(message.name(), foreign(&message), None, pick);
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
impl<Role: EndpointRole> TestableBroker for ConnectedZmqFanout<Role> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        if let Some(bus) = self.lifecycle().in_process_bus() {
            bus.send(
                message.name(),
                foreign(&message),
                None,
                Pick::Prefix(message.name()),
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
impl TestableBroker for ConnectedZmqRpc {
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
            if bus.send(message.name(), foreign(&message), Some(&identity), pick) == 0 {
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

ruststream::register_testable_broker!(ZmqQueue<Bind>);
ruststream::register_testable_broker!(ZmqQueue<Connect>);
ruststream::register_testable_broker!(ZmqFanout<Bind>);
ruststream::register_testable_broker!(ZmqFanout<Connect>);
ruststream::register_testable_broker!(ZmqRpc);

/// A request of this service over the in-process transport: the request leaves the way the
/// DEALER sends it, reaches the responder this service binds when there is one, and waits for
/// the answer that echoes its correlation id.
pub(crate) async fn request(
    bus: &Bus,
    local: bool,
    name: &str,
    frames: WireMessage,
    correlation: &str,
    timeout: Duration,
) -> Result<ZmqMessage, ZmqError> {
    let identity = peer_identity();
    let mut inbox = bus.open_inbox(identity.clone());
    // Disconnects the DEALER however the request ends, a caller dropping it included.
    let _connected = Connected {
        bus,
        identity: &identity,
    };
    let pick = if local { Pick::All } else { Pick::Nobody };
    let _ = bus.send(name, frames, Some(&identity), pick);
    let exchange = async {
        while let Some(reply) = inbox.recv().await {
            let (name, headers, payload) = wire::decode(reply)?;
            if headers.correlation_id() == Some(correlation) {
                return Ok(ZmqMessage::new(name, headers, payload));
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
        let frames = wire::encode_to(
            "orders",
            "orders",
            &HeaderMap::new(),
            Bytes::from_static(b"x"),
        )
        .expect("frames");
        let pending = request(
            &bus,
            false,
            "orders",
            frames,
            "c-1",
            Duration::from_secs(60),
        )
        .now_or_never();
        assert!(pending.is_none(), "the request was still waiting");
        assert_eq!(bus.peers(), 0);
    }
}
