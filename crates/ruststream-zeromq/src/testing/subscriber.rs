//! [`ZmqTestSubscriber`] and [`ZmqTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Subscriber,
    testing::Coordinator,
};

use crate::common::BATCH_MAX_WAIT;
use crate::error::ZmqError;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, SubscriptionId};

/// The routed side of an in-process subscription, matching the real transport's one-at-a-time
/// delivery; batches are assembled over it by the wrapper in [`ZmqTestSubscriber`].
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes.
struct TestWire {
    state: Arc<TestState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl Drop for TestWire {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for TestWire {
    type Message = ZmqTestMessage;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| Ok(ZmqTestMessage::new(delivery, coordinator.clone())))
            })
        })
    }
}

/// Subscriber returned by [`ConnectedZmqTestBroker`](crate::testing::ConnectedZmqTestBroker).
pub struct ZmqTestSubscriber {
    inner: BufferedSubscriber<TestWire>,
}

impl std::fmt::Debug for ZmqTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqTestSubscriber").finish_non_exhaustive()
    }
}

impl ZmqTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            inner: BufferedSubscriber::new(TestWire {
                state,
                id,
                rx,
                coordinator,
            })
            .max_wait(BATCH_MAX_WAIT),
        }
    }
}

impl Subscriber for ZmqTestSubscriber {
    type Message = ZmqTestMessage;
    type Error = ZmqError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
    }
}

/// Batches the same way the real subscriber does - on the client, to the size the registration
/// named - so a batch handler that runs in production also compiles and runs in a `TestApp`.
impl BatchSubscriber for ZmqTestSubscriber {
    type Batch = Vec<ZmqTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, ZmqError>> + Send + '_ {
        self.inner.batches(size)
    }
}

/// Message handed to handlers from an [`ZmqTestSubscriber`].
///
/// Settlement is reported exactly as [`ZmqMessage`](crate::ZmqMessage) reports it: `ZeroMQ`
/// acknowledges nothing, so both `ack` and `nack` return [`AckError::Unsupported`] and no
/// redelivery ever happens. A handler that settles by retrying loses its message on this
/// transport, and it loses it here too - a stand-in that redelivered instead would hand the
/// author a passing test for something the deployment cannot do.
pub struct ZmqTestMessage {
    delivery: Delivery,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for ZmqTestMessage {
    /// Counts this delivery consumed exactly once, whether the handler settled it or dropped it:
    /// settling consumes the handle, so every path arrives here.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for ZmqTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqTestMessage").finish_non_exhaustive()
    }
}

impl ZmqTestMessage {
    /// Builds a message carrying a harness coordinator clone: a dispatch-driven delivery.
    pub(crate) fn new(delivery: Delivery, coordinator: Option<Coordinator>) -> Self {
        Self {
            delivery,
            coordinator,
        }
    }

    /// Builds a message with no coordinator: a reply the requester consumes itself, which the
    /// router leaves uncounted for the same reason.
    pub(crate) fn from_reply(delivery: Delivery) -> Self {
        Self::new(delivery, None)
    }
}

impl IncomingMessage for ZmqTestMessage {
    fn payload(&self) -> &[u8] {
        &self.delivery.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.delivery.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}
