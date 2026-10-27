//! [`Framed`]: the name frame, then the headers and the payload as one document of a codec.

#[cfg(any(feature = "cbor", feature = "msgpack"))]
mod binary;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
mod headers;
#[cfg(feature = "json")]
mod json;

use std::collections::VecDeque;
use std::str;

use bytes::Bytes;
use ruststream::HeaderMap;
use ruststream::codec::Codec;

use super::{Envelope, Opened, Pattern, SealError, decode_name};
use crate::error::ZmqError;

/// The name frame, then one frame holding a document of `DocumentCodec`: `headers`, a map of text
/// values, and the payload, the way `CloudEvents` writes an event in its structured mode.
///
/// A peer that reads JSON, CBOR or `MessagePack` reads the whole message with its parser, with no
/// frame layout to learn. The JSON document follows the `CloudEvents` JSON format: a payload that
/// is one JSON text travels as a value in `payload`, byte for byte as it was written, and any
/// other payload as standard base64 in `payload_base64`:
///
/// ```text
/// frame 0: orders
/// frame 1: {"headers":{"event":"created"},"payload":{"id":7}}
/// frame 1: {"headers":{"event":"created"},"payload_base64":"CAc="}
/// ```
///
/// The CBOR and `MessagePack` documents carry the payload as a byte string in `payload`.
///
/// A frame that is not such a document, `headers` and exactly one payload field with nothing
/// beside them, is the payload of a peer that uses no envelope, and reads as it is with no
/// headers; so does a message of one frame. The payload is located when the delivery is read (a
/// JSON value is a slice of the frame, a byte string or base64 a copy), the headers decoded when a
/// handler first asks for one. A publish writes the document as a buffer of its own, after a scan
/// that tells JSON text from other bytes; [`Frames`](super::Frames) and [`NoOp`](super::NoOp)
/// carry the payload in a frame of its own.
///
/// # Examples
///
/// A fan-out whose subscribers are services in other languages, each reading one JSON document
/// per message:
///
/// ```
/// use ruststream::codec::JsonCodec;
/// use ruststream_zeromq::envelope::Framed;
/// use ruststream_zeromq::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders.placed")]
/// struct Placed {
///     id: u64,
/// }
///
/// #[subscriber("orders", reply)]
/// async fn place(order: &Order) -> Placed {
///     Placed { id: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556"))
///         .envelope(Framed::new(JsonCodec))
///         .bindable();
///     let to_events = events.bind(ZmqFanoutPublish);
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(place).out_reply(to_events);
///         })
///         .with_broker(events, |_b| {})
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Framed<DocumentCodec> {
    codec: DocumentCodec,
}

impl<DocumentCodec: FramedCodec> Framed<DocumentCodec> {
    /// The layout whose second frame is a document of `codec`.
    ///
    /// # Examples
    ///
    /// A worker that reads each job as one JSON document behind the name frame:
    ///
    /// ```
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_zeromq::envelope::Framed;
    /// use ruststream_zeromq::queue::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     tracing::info!(job.id, "working on the job");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let jobs = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555"))
    ///         .envelope(Framed::new(JsonCodec));
    ///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(jobs, |b| {
    ///         b.include(work);
    ///     })
    /// }
    /// ```
    pub const fn new(codec: DocumentCodec) -> Self {
        Self { codec }
    }
}

/// A codec [`Framed`] writes its document in: the core's `JsonCodec` (feature `json`),
/// `CborCodec` (feature `cbor`) and `MsgpackCodec` (feature `msgpack`).
///
/// Each one writes the payload the way its format carries bytes, which a document of another
/// codec has no rule for, so the set is closed.
///
/// # Examples
///
/// [`Framed`] shows a broker that names one.
///
/// ```
/// use ruststream::codec::JsonCodec;
/// use ruststream_zeromq::envelope::Framed;
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn work(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.id, "working on the job");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let jobs = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555"))
///         .envelope(Framed::new(JsonCodec));
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(jobs, |b| {
///         b.include(work);
///     })
/// }
/// ```
pub trait FramedCodec: Codec + Clone + 'static + sealed::Sealed {
    /// Writes `headers`, already checked to be text, and `payload` as one document.
    #[doc(hidden)]
    fn seal_document(&self, headers: &HeaderMap, payload: &[u8]) -> Result<Bytes, SealError>;

    /// The payload of `document`, or `None` when it is not a document in this codec's shape.
    #[doc(hidden)]
    fn open_document(&self, document: &Bytes) -> Option<Bytes>;

    /// The headers of a document [`open_document`](Self::open_document) recognised.
    #[doc(hidden)]
    fn document_headers(document: &Bytes) -> Result<HeaderMap, ZmqError>;
}

mod sealed {
    /// Closes [`FramedCodec`](super::FramedCodec) to the codecs this crate knows how to write.
    pub trait Sealed {}
}

impl<DocumentCodec: FramedCodec> Envelope for Framed<DocumentCodec> {
    /// The document, kept to read the headers from; `None` for a frame that was no document.
    type Head = Option<Bytes>;

    const NAME: &'static str = "framed";

    const CONTENT_TYPE: Option<&'static str> = Some(DocumentCodec::CONTENT_TYPE);

    fn open(
        &self,
        _pattern: Pattern,
        mut frames: VecDeque<Bytes>,
    ) -> Result<Opened<Self::Head>, ZmqError> {
        let count = frames.len();
        match (frames.pop_front(), frames.pop_front()) {
            (Some(payload), None) => Ok(Opened::new(String::new(), None, payload)),
            (Some(name), Some(document)) if frames.is_empty() => {
                let name = decode_name(&name)?;
                Ok(match self.codec.open_document(&document) {
                    Some(payload) => Opened::new(name, Some(document), payload),
                    None => Opened::new(name, None, document),
                })
            }
            _ => Err(ZmqError::Wire(format!(
                "a message of {count} frames does not follow the layout of one or two frames \
                 (name, document)"
            ))),
        }
    }

    fn headers(head: &Self::Head) -> Result<HeaderMap, ZmqError> {
        head.as_ref()
            .map_or_else(|| Ok(HeaderMap::new()), DocumentCodec::document_headers)
    }

    fn seal(
        &self,
        _pattern: Pattern,
        name: &str,
        headers: &HeaderMap,
        payload: Bytes,
        frames: &mut Vec<Bytes>,
    ) -> Result<(), SealError> {
        if let Some((header, _)) = headers
            .iter()
            .find(|(_, value)| str::from_utf8(value).is_err())
        {
            return Err(SealError::new(format!(
                "header '{header}' holds a value that is not UTF-8, and the document's headers are \
                 text"
            )));
        }
        let document = self.codec.seal_document(headers, &payload)?;
        frames.push(Bytes::copy_from_slice(name.as_bytes()));
        frames.push(document);
        Ok(())
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use ruststream::codec::JsonCodec;

    use super::*;

    const JSON: Framed<JsonCodec> = Framed::new(JsonCodec);

    fn sealed(headers: &HeaderMap, payload: &'static [u8]) -> Vec<Bytes> {
        let mut frames = Vec::new();
        JSON.seal(
            Pattern::PushPull,
            "orders",
            headers,
            Bytes::from_static(payload),
            &mut frames,
        )
        .expect("seals");
        frames
    }

    /// A peer reads the second frame as one document with its own parser.
    #[test]
    fn the_second_frame_is_one_document_a_peer_parses() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let frames = sealed(&headers, br#"{"id":7}"#);
        assert_eq!(frames[0].as_ref(), b"orders");
        let document: serde_json::Value =
            serde_json::from_slice(&frames[1]).expect("the frame is JSON");
        assert_eq!(
            document,
            serde_json::json!({"headers": {"x-tenant": "acme"}, "payload": {"id": 7}}),
        );
    }

    #[test]
    fn headers_and_payload_round_trip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let opened = JSON
            .open(
                Pattern::PushPull,
                sealed(&headers, b"\xff\x00binary").into(),
            )
            .expect("opens");
        assert_eq!(opened.name, "orders");
        assert_eq!(opened.payload.as_ref(), b"\xff\x00binary");
        let read = Framed::<JsonCodec>::headers(&opened.head).expect("reads");
        assert_eq!(read.get_str("x-tenant"), Some("acme"));
    }

    /// A second frame that is not the document, exactly its two fields, is a peer's own payload.
    #[test]
    fn a_frame_that_is_no_document_is_the_payload() {
        for frame in [
            &br#"{"id":7}"#[..],
            br#"{"headers":{},"payload":"x","extra":1}"#,
            br#"{"payload":"x"}"#,
            b"\xff not json",
        ] {
            let opened = JSON
                .open(
                    Pattern::PubSub,
                    VecDeque::from([Bytes::from_static(b"orders"), Bytes::copy_from_slice(frame)]),
                )
                .expect("opens");
            assert_eq!(opened.payload.as_ref(), frame);
            assert_eq!(
                Framed::<JsonCodec>::headers(&opened.head).expect("reads"),
                HeaderMap::new()
            );
        }
    }

    /// A payload that looks like a document travels inside one, so it reads back as itself.
    #[test]
    fn a_payload_that_looks_like_a_document_reads_back_as_itself() {
        let lookalike = br#"{"headers":{"x-forged":"1"},"payload":"x"}"#;
        let opened = JSON
            .open(
                Pattern::PushPull,
                sealed(&HeaderMap::new(), lookalike).into(),
            )
            .expect("opens");
        assert_eq!(opened.payload.as_ref(), lookalike);
        assert_eq!(
            Framed::<JsonCodec>::headers(&opened.head).expect("reads"),
            HeaderMap::new()
        );
    }

    #[test]
    fn a_single_frame_is_the_payload() {
        let opened = JSON
            .open(
                Pattern::PushPull,
                VecDeque::from([Bytes::from_static(b"raw")]),
            )
            .expect("opens");
        assert_eq!(
            (opened.name.as_str(), opened.payload.as_ref()),
            ("", &b"raw"[..])
        );
    }

    #[test]
    fn a_value_that_is_not_text_is_refused_with_its_destination() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", [0xff, 0xfe].as_slice());
        let err = JSON
            .seal(
                Pattern::PushPull,
                "orders",
                &headers,
                Bytes::from_static(b"{}"),
                &mut Vec::new(),
            )
            .expect_err("a binary value has no text form")
            .against("orders")
            .to_string();
        assert!(err.contains("x-binary") && err.contains("orders"), "{err}");
    }

    /// A binary codec writes a payload that is not text as bytes, and reads it back unchanged.
    #[cfg(any(feature = "cbor", feature = "msgpack"))]
    fn round_trips_through<DocumentCodec: FramedCodec>(codec: DocumentCodec) {
        let layout = Framed::new(codec);
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let mut frames = Vec::new();
        layout
            .seal(
                Pattern::PubSub,
                "orders",
                &headers,
                Bytes::from_static(b"\xff\x00binary"),
                &mut frames,
            )
            .expect("seals");
        let opened = layout.open(Pattern::PubSub, frames.into()).expect("opens");
        assert_eq!(opened.payload.as_ref(), b"\xff\x00binary");
        let read = Framed::<DocumentCodec>::headers(&opened.head).expect("reads");
        assert_eq!(read.get_str("x-tenant"), Some("acme"));
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn a_cbor_document_round_trips() {
        round_trips_through(ruststream::codec::CborCodec);
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn a_msgpack_document_round_trips() {
        round_trips_through(ruststream::codec::MsgpackCodec);
    }

    #[test]
    fn a_message_of_three_frames_is_refused() {
        let err = JSON
            .open(
                Pattern::PushPull,
                VecDeque::from([Bytes::new(), Bytes::new(), Bytes::new()]),
            )
            .expect_err("three frames are not the layout")
            .to_string();
        assert!(err.contains("3 frames"), "{err}");
    }
}
