//! [`ZmqMessage`]: a delivered message.

use bytes::Bytes;
use ruststream::{AckError, Headers, IncomingMessage};

/// A message delivered by one of the transport's subscribers.
///
/// Delivery is at most once and there is no durability, so acknowledgement is reported as
/// [`AckError::Unsupported`] rather than emulated.
pub struct ZmqMessage {
    pub(crate) name: String,
    pub(crate) headers: Headers,
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

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }

    async fn nack(self, _requeue: bool) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }
}
