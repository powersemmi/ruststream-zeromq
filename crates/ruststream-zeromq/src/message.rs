//! [`ZmqMessage`]: a delivered message.

use std::future::{Future, ready};

use bytes::Bytes;
use ruststream::{AckError, HeaderMap, IncomingMessage};

/// A message delivered by one of the transport's subscribers.
///
/// Delivery is at most once and there is no durability, so acknowledgement is reported as
/// [`AckError::Unsupported`] rather than emulated.
pub struct ZmqMessage {
    pub(crate) name: String,
    pub(crate) headers: HeaderMap,
    pub(crate) payload: Bytes,
}

impl std::fmt::Debug for ZmqMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqMessage")
            .field("name", &self.name)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl ZmqMessage {
    /// The name frame this message carried.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl IncomingMessage for ZmqMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}
