//! [`ZmqMessage`]: a delivered message.

use std::fmt;
use std::future::{Future, ready};
use std::sync::OnceLock;

use bytes::Bytes;
use ruststream::{AckError, HeaderMap, IncomingMessage, Str};

use crate::envelope::{Envelope, Frames, Opened};
#[cfg(feature = "testing")]
use crate::in_process::Release;
use crate::rpc::{REPLY_TO_HEADER, reply_address};

/// A message delivered by one of the transport's subscribers, read through the envelope `Env`
/// of the broker it arrived on.
///
/// The delivery keeps the frames it was read from. Its payload is located when it is received,
/// and its headers are read out of the envelope on the first call to
/// [`headers`](IncomingMessage::headers), then kept: a handler that reads no header never parses
/// them.
///
/// Delivery is at most once and there is no durability, so acknowledgement is reported as
/// [`AckError::Unsupported`] rather than emulated.
pub struct ZmqMessage<Env: Envelope = Frames> {
    name: String,
    payload: Bytes,
    head: Env::Head,
    headers: OnceLock<HeaderMap>,
    /// What the in-process transport hands the test harness back once this delivery is gone.
    #[cfg(feature = "testing")]
    release: Release,
}

// The in-process mode adds nothing to a production delivery: without the `testing` feature the
// message is its frames and the headers read from them, and nothing else. It moves through the
// delivery channel and the batch buffer by value, so a field more is a copy more per message.
#[cfg(not(feature = "testing"))]
const _: () =
    assert!(size_of::<ZmqMessage>() == size_of::<(String, Bytes, Bytes, OnceLock<HeaderMap>)>());

impl<Env: Envelope> fmt::Debug for ZmqMessage<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqMessage")
            .field("name", &self.name)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> ZmqMessage<Env> {
    /// A delivery made of what the envelope read off the frames a socket received.
    pub(crate) fn new(opened: Opened<Env::Head>) -> Self {
        Self {
            name: opened.name,
            payload: opened.payload,
            head: opened.head,
            headers: OnceLock::new(),
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

    /// The name frame this message carried, empty for a message of one frame, which a plain
    /// `ZeroMQ` peer sends as its payload alone.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The headers, read out of the envelope on the first call and kept; `stamp` adds what the
    /// transport knows of the message beside them.
    fn headers_stamped(&self, stamp: impl FnOnce(&mut HeaderMap)) -> &HeaderMap {
        self.headers.get_or_init(|| {
            let mut headers = self.read_headers();
            stamp(&mut headers);
            headers
        })
    }

    /// Reads the headers out of the envelope. Headers that do not read are logged against the
    /// message, and it has none.
    fn read_headers(&self) -> HeaderMap {
        Env::headers(&self.head).unwrap_or_else(|err| {
            tracing::warn!(
                message = %self.name,
                envelope = Env::NAME,
                error = %err,
                "the headers of a delivery do not read; it is handled without them",
            );
            HeaderMap::new()
        })
    }
}

impl<Env: Envelope> IncomingMessage for ZmqMessage<Env> {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        self.headers.get_or_init(|| self.read_headers())
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}

/// A request a [`ZmqRpc`](crate::ZmqRpc) responder received: the message read through the
/// envelope `Env`, and the identity of the DEALER that sent it.
///
/// Its headers carry a `reply-to` address for that DEALER beside the ones the envelope carries,
/// [`NoOp`](crate::envelope::NoOp) included, so a reply finds the peer that asked. They are read
/// on the first call to [`headers`](IncomingMessage::headers), as a
/// [`ZmqMessage`]'s are.
///
/// Delivery is at most once, so acknowledgement is reported as [`AckError::Unsupported`].
///
/// # Examples
///
/// A responder logs the address of the peer behind each request it is asked:
///
/// ```
/// use ruststream_zeromq::rpc::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Greeting {
///     who: String,
/// }
///
/// #[subscriber("greeter")]
/// async fn listen(request: &Greeting, ctx: &mut Context) -> HandlerOutcome {
///     let reply_to = ctx.headers().reply_to().unwrap_or("nobody");
///     tracing::info!(who = %request.who, reply_to, "asked");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(
///         ZmqRpc::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
///         |b| {
///             b.include(listen).out_retry(Publish).to("greeter.retry");
///         },
///     )
/// }
/// ```
pub struct ZmqRequest<Env: Envelope = Frames> {
    message: ZmqMessage<Env>,
    peer: Bytes,
}

impl<Env: Envelope> fmt::Debug for ZmqRequest<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZmqRequest")
            .field("name", &self.message.name)
            .field("payload_len", &self.message.payload.len())
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> ZmqRequest<Env> {
    /// The request in `message`, sent by the DEALER whose identity is `peer`.
    pub(crate) const fn new(message: ZmqMessage<Env>, peer: Bytes) -> Self {
        Self { message, peer }
    }

    /// Hands the harness's count of this delivery to the request itself.
    #[cfg(feature = "testing")]
    pub(crate) fn counted(self, release: Release) -> Self {
        Self {
            message: self.message.counted(release),
            peer: self.peer,
        }
    }

    /// The name frame this request carried, empty for a request of one frame.
    #[must_use]
    pub fn name(&self) -> &str {
        self.message.name()
    }
}

impl<Env: Envelope> IncomingMessage for ZmqRequest<Env> {
    fn payload(&self) -> &[u8] {
        self.message.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.message.headers_stamped(|headers| {
            headers.insert(Str::from_static(REPLY_TO_HEADER), reply_address(&self.peer));
        })
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ptr;

    use super::*;
    use crate::envelope::Pattern;

    fn delivery(frames: &[&'static [u8]]) -> ZmqMessage {
        let frames: VecDeque<Bytes> = frames.iter().copied().map(Bytes::from_static).collect();
        ZmqMessage::new(Frames.open(Pattern::PushPull, frames).expect("opens"))
    }

    /// The payload is the frame it arrived in, located without reading the header frame: a
    /// handler that never asks for a header never parses one.
    #[test]
    fn reading_the_payload_leaves_the_headers_unparsed() {
        let message = delivery(&[b"orders", b"x-tenant: acme", br#"{"id":7}"#]);
        assert_eq!(message.payload(), br#"{"id":7}"#);
        assert!(
            message.headers.get().is_none(),
            "the header frame was parsed although nothing read a header",
        );
    }

    /// The headers are parsed on the first read and kept, so the second read finds the same map.
    #[test]
    fn the_headers_are_parsed_once_and_kept() {
        let message = delivery(&[b"orders", b"x-tenant: acme", br#"{"id":7}"#]);
        let first: *const HeaderMap = message.headers();
        assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
        assert!(ptr::eq(first, message.headers()), "parsed again");
    }

    /// A header frame that does not read is the payload of a delivery without headers.
    #[test]
    fn a_header_frame_that_does_not_read_leaves_the_delivery_without_headers() {
        let message = delivery(&[b"orders", b"content-type application/json", b"{}"]);
        assert_eq!(message.payload(), b"{}");
        assert_eq!(message.headers(), &HeaderMap::new());
    }

    /// A request carries the address of the peer that sent it, beside the headers its envelope
    /// carries, and is read as lazily as any delivery.
    #[test]
    fn a_request_reads_with_the_address_of_its_peer() {
        let peer = Bytes::from_static(&[0xde, 0xad]);
        let request = ZmqRequest::new(
            ZmqMessage::<Frames>::new(Opened::new(
                "greeter",
                Bytes::from_static(b"x-tenant: acme"),
                Bytes::from_static(b"{}"),
            )),
            peer.clone(),
        );
        assert_eq!(request.payload(), b"{}");
        assert!(request.message.headers.get().is_none(), "parsed unread");
        assert_eq!(
            request.headers().get_str(REPLY_TO_HEADER),
            Some(reply_address(&peer).as_str())
        );
        assert_eq!(request.headers().get_str("x-tenant"), Some("acme"));
    }
}
