//! Subscription registry and delivery for the in-process `ZeroMQ` stand-in.
//!
//! One registry of live subscriptions, a per-name log of everything published for assertions,
//! and the part that differs between the crate's patterns: which subscriptions one publish
//! reaches, chosen by the [`Routing`] the publisher hands in. What remains transport behaviour -
//! high-water marks, the slow joiner, delivery guarantees, and whether a peer is connected at all
//! - has no counterpart in a channel and is left to the loopback suite.

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

use crate::rpc::REPLY_PREFIX;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    address: String,
    sender: DeliverySender,
}

/// Which subscriptions one publish reaches: the difference between the crate's three patterns,
/// and the semantics a service writes tests about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Routing {
    /// PUSH/PULL: exactly one of the consumers on the destination, taken in turn, so a job is
    /// worked once however many workers are mounted.
    Competing,
    /// PUB/SUB: every subscription whose name is a prefix of the destination, which is the
    /// protocol's own filter, and none at all when nothing matches.
    Prefix,
    /// Every subscription spelled exactly like the destination. Reply addresses are minted per
    /// request, so this delivers a reply to the one requester waiting on it; harness injection
    /// uses it as the pattern-neutral way in.
    Exact,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
    /// Per-destination cursor behind [`Routing::Competing`], so consecutive publishes rotate
    /// over the consumers instead of always waking the same one.
    next_consumer: HashMap<String, usize>,
}

/// In-memory router over subscription names.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    /// Registers a subscription on `address` and returns the receiving end the subscriber polls,
    /// together with the [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The sending end stays in the registry: only a publish puts a delivery on a subscription,
    /// because `ZeroMQ` has no redelivery for a settled message to ask for.
    pub(crate) fn subscribe(&self, address: String) -> (SubscriptionId, DeliveryReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("zmq test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    address,
                    sender: tx,
                },
            );
        (id, rx)
    }

    /// Removes a subscription. No-op if the id is unknown (double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("zmq test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Delivers `payload` to the subscriptions `routing` selects for `address`, and records it in
    /// the published log whether or not anything was listening.
    ///
    /// Under a harness run every live enqueue is counted with [`Coordinator::enqueued`], so the
    /// harness can drive to quiescence. Reply addresses (the `zmq-reply:` prefix a request mints)
    /// are skipped: their delivery is consumed by the waiting requester rather than by a dispatch
    /// loop, so it carries no coordinator and would never be decremented.
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        routing: Routing,
        coordinator: Option<&Coordinator>,
    ) {
        let snapshot = RawMessage::new(address, payload.clone()).with_headers(headers.clone());
        let mut state = self.state.lock().expect("zmq test router mutex poisoned");
        state
            .log
            .entry(address.to_owned())
            .or_default()
            .push(snapshot);
        let to_notify = select(&mut state, address, routing);
        // The sends below are the subscribers' business, not the registry's.
        drop(state);

        let is_reply = address.starts_with(REPLY_PREFIX);
        let delivery = Delivery { payload, headers };
        for tx in to_notify {
            if tx.send(delivery.clone()).is_ok()
                && !is_reply
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Returns every message recorded for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("zmq test router mutex poisoned")
            .log
            .get(address)
            .cloned()
            .unwrap_or_default()
    }

    /// Drops every subscription and clears the published log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("zmq test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
        state.next_consumer.clear();
    }
}

/// Picks the senders one publish reaches, under the lock its caller already holds.
///
/// Matching subscriptions are ordered by id, so the competing-consumer cursor rotates over a
/// stable sequence instead of over whatever order the map hands back.
fn select(state: &mut RouterState, address: &str, routing: Routing) -> Vec<DeliverySender> {
    let mut matching: Vec<(SubscriptionId, DeliverySender)> = state
        .subscriptions
        .iter()
        .filter(|(_, sub)| match routing {
            Routing::Prefix => address.starts_with(&sub.address),
            Routing::Competing | Routing::Exact => sub.address == address,
        })
        .map(|(id, sub)| (*id, sub.sender.clone()))
        .collect();
    matching.sort_by_key(|(id, _)| id.0);

    match routing {
        Routing::Competing if !matching.is_empty() => {
            let cursor = state.next_consumer.entry(address.to_owned()).or_default();
            let picked = *cursor % matching.len();
            *cursor = cursor.wrapping_add(1);
            vec![matching.swap_remove(picked).1]
        }
        Routing::Competing => Vec::new(),
        Routing::Prefix | Routing::Exact => matching.into_iter().map(|(_, tx)| tx).collect(),
    }
}

impl std::fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("zmq test router mutex poisoned");
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
