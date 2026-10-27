//! [`Framed`]: the name frame, then the headers and the payload as one document of a codec.

use std::collections::VecDeque;
use std::fmt;
use std::str;

use bytes::Bytes;
use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Envelope, Opened, Pattern, SealError, decode_name};
use crate::error::ZmqError;

/// The name frame, then one frame holding a document of `Codec` with two fields: `headers`, a
/// map of text values, and `payload`.
///
/// A peer that reads JSON, CBOR or `MessagePack` reads the whole message with its parser, with no
/// frame layout to learn. The payload is written as text when it is UTF-8, as the JSON codec's
/// output always is, and as bytes otherwise:
///
/// ```text
/// frame 0: orders
/// frame 1: {"headers":{"x-tenant":"acme"},"payload":"{\"id\":7}"}
/// ```
///
/// A frame that is not such a document, exactly these two fields, is the payload of a peer that
/// uses no envelope, and reads as it is with no headers; so does a message of one frame. The
/// payload is decoded out of the document when the delivery is read, the headers when a handler
/// first asks for one. The document is a copy of the payload, which a publish allocates and a
/// delivery decodes; [`Frames`](super::Frames) and [`NoOp`](super::NoOp) carry the payload in a
/// frame of its own.
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

impl<DocumentCodec: Codec> Framed<DocumentCodec> {
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

impl<DocumentCodec> Envelope for Framed<DocumentCodec>
where
    DocumentCodec: Codec + Clone + 'static,
{
    /// The document and the codec that reads it; `None` for a frame that was no document.
    type Head = Option<(Bytes, DocumentCodec)>;

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
                Ok(
                    match self.codec.decode::<Shape<IgnoredAny, Payload>>(&document) {
                        Ok(shape) => {
                            Opened::new(name, Some((document, self.codec.clone())), shape.payload.0)
                        }
                        Err(_) => Opened::new(name, None, document),
                    },
                )
            }
            _ => Err(ZmqError::Wire(format!(
                "a message of {count} frames does not follow the layout of one or two frames \
                 (name, document)"
            ))),
        }
    }

    fn headers(head: &Self::Head) -> Result<HeaderMap, ZmqError> {
        let Some((document, codec)) = head else {
            return Ok(HeaderMap::new());
        };
        codec
            .decode::<Shape<Headers, IgnoredAny>>(document)
            .map(|shape| shape.headers.0)
            .map_err(|err| ZmqError::Wire(format!("the document's headers do not read: {err}")))
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
        let document = self
            .codec
            .encode(&Document {
                headers,
                payload: &payload,
            })
            .map_err(|err| SealError::new(format!("the document does not encode: {err}")))?;
        frames.push(Bytes::copy_from_slice(name.as_bytes()));
        frames.push(document.freeze());
        Ok(())
    }
}

/// The document a publish writes.
struct Document<'a> {
    headers: &'a HeaderMap,
    payload: &'a [u8],
}

impl Serialize for Document<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut document = serializer.serialize_map(Some(2))?;
        document.serialize_entry("headers", &HeaderText(self.headers))?;
        match str::from_utf8(self.payload) {
            Ok(text) => document.serialize_entry("payload", text)?,
            Err(_) => document.serialize_entry("payload", &RawBytes(self.payload))?,
        }
        document.end()
    }
}

/// The headers as a map of text values; [`Framed::seal`] refuses a value that is not text first.
struct HeaderText<'a>(&'a HeaderMap);

impl Serialize for HeaderText<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(
            self.0
                .iter()
                .map(|(name, value)| (name, str::from_utf8(value).unwrap_or_default())),
        )
    }
}

struct RawBytes<'a>(&'a [u8]);

impl Serialize for RawBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// What a delivery reads of the document, one field at a time: the other is skipped unread.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Shape<HeadersField, PayloadField> {
    headers: HeadersField,
    payload: PayloadField,
}

/// The payload, from text, bytes, or a sequence of byte values (how JSON writes bytes).
struct Payload(Bytes);

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(PayloadVisitor)
    }
}

struct PayloadVisitor;

impl<'de> Visitor<'de> for PayloadVisitor {
    type Value = Payload;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the payload as text or bytes")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Payload, E> {
        Ok(Payload(Bytes::copy_from_slice(text.as_bytes())))
    }

    fn visit_string<E: de::Error>(self, text: String) -> Result<Payload, E> {
        Ok(Payload(Bytes::from(text)))
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Payload, E> {
        Ok(Payload(Bytes::copy_from_slice(bytes)))
    }

    fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Payload, E> {
        Ok(Payload(Bytes::from(bytes)))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Payload, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or_default());
        while let Some(byte) = seq.next_element::<u8>()? {
            bytes.push(byte);
        }
        Ok(Payload(Bytes::from(bytes)))
    }
}

/// The headers, read straight into the map a delivery hands out.
struct Headers(HeaderMap);

impl<'de> Deserialize<'de> for Headers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(HeadersVisitor)
    }
}

struct HeadersVisitor;

impl<'de> Visitor<'de> for HeadersVisitor {
    type Value = Headers;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map of header names to text values")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Headers, A::Error> {
        let mut headers = HeaderMap::with_capacity(map.size_hint().unwrap_or_default());
        while let Some((name, value)) = map.next_entry::<String, String>()? {
            headers.insert(name, value);
        }
        Ok(Headers(headers))
    }
}

#[cfg(test)]
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
            serde_json::json!({"headers": {"x-tenant": "acme"}, "payload": "{\"id\":7}"}),
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
    fn round_trips_through<DocumentCodec: Codec + Clone + 'static>(codec: DocumentCodec) {
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
