//! [`ZmqMessage`]: a delivered message.

use std::future::{Future, ready};

use bytes::Bytes;
use ruststream::{AckError, HeaderMap, IncomingMessage};

#[cfg(feature = "testing")]
use crate::in_process::Release;

/// A message delivered by one of the transport's subscribers.
///
/// Delivery is at most once and there is no durability, so acknowledgement is reported as
/// [`AckError::Unsupported`] rather than emulated.
pub struct ZmqMessage {
    pub(crate) name: String,
    pub(crate) headers: HeaderMap,
    pub(crate) payload: Bytes,
    /// What the in-process transport hands the test harness back once this delivery is gone.
    #[cfg(feature = "testing")]
    release: Release,
}

// The in-process mode adds nothing to a production delivery: without the `testing` feature the
// message is its three frames and nothing else.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<ZmqMessage>() == size_of::<(String, HeaderMap, Bytes)>());

impl std::fmt::Debug for ZmqMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqMessage")
            .field("name", &self.name)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl ZmqMessage {
    /// A delivery made of the frames a socket received.
    pub(crate) fn new(name: String, headers: HeaderMap, payload: Bytes) -> Self {
        Self {
            name,
            headers,
            payload,
            #[cfg(feature = "testing")]
            release: Release::default(),
        }
    }

    /// Hands the harness's count of this delivery to the delivery itself, which gives it back
    /// once it is settled or dropped.
    #[cfg(feature = "testing")]
    pub(crate) fn counted(mut self, release: Release) -> Self {
        self.release = release;
        self
    }

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
