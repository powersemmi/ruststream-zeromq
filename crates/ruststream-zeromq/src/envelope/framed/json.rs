//! The JSON document of [`Framed`], in `CloudEvents`' JSON format.

use std::borrow::Cow;

use base64::Engine;
use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use ruststream::HeaderMap;
use ruststream::codec::JsonCodec;
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use super::FrameCodec;
use super::headers::{HeaderText, HeadersOnly};
use crate::envelope::SealError;
use crate::error::ZmqError;

/// A payload that is one JSON text travels as a value in `payload`, written as it is; any other
/// payload as standard base64 in `payload_base64`.
impl FrameCodec for JsonCodec {
    fn frame(&self, headers: &HeaderMap, payload: &[u8]) -> Result<Bytes, SealError> {
        let document = Written {
            headers: HeaderText(headers),
            payload: json_text(payload),
            bytes: payload,
        };
        serde_json::to_vec(&document)
            .map(Bytes::from)
            .map_err(|err| SealError::new(format!("the document does not encode: {err}")))
    }

    fn frame_body(&self, document: &Bytes) -> Option<Bytes> {
        let read: Read<'_> = serde_json::from_slice(document).ok()?;
        match (read.payload, read.payload_base64) {
            (Some(value), None) => Some(document.slice_ref(value.get().as_bytes())),
            (None, Some(encoded)) => STANDARD.decode(encoded.as_bytes()).ok().map(Bytes::from),
            _ => None,
        }
    }

    fn frame_headers(&self, document: &Bytes) -> Result<HeaderMap, ZmqError> {
        serde_json::from_slice::<HeadersOnly>(document)
            .map(|read| read.headers.0)
            .map_err(|err| ZmqError::Wire(format!("the document's headers do not read: {err}")))
    }
}

/// `payload` as one JSON text, exactly as written, or `None` where it is something else: a
/// value with whitespace around it would not read back as the same bytes.
fn json_text(payload: &[u8]) -> Option<&RawValue> {
    serde_json::from_slice::<&RawValue>(payload)
        .ok()
        .filter(|value| value.get().len() == payload.len())
}

/// The document a publish writes: exactly one of the two payload fields.
struct Written<'a> {
    headers: HeaderText<'a>,
    payload: Option<&'a RawValue>,
    bytes: &'a [u8],
}

impl Serialize for Written<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut document = serializer.serialize_map(Some(2))?;
        document.serialize_entry("headers", &self.headers)?;
        match self.payload {
            Some(value) => document.serialize_entry("payload", value)?,
            // Written straight into the document, with no encoded copy in between.
            None => document.serialize_entry("payload_base64", &Base64(self.bytes))?,
        }
        document.end()
    }
}

struct Base64<'a>(&'a [u8]);

impl Serialize for Base64<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&Base64Display::new(self.0, &STANDARD))
    }
}

/// What a delivery reads of a document: the payload fields, the headers skipped unread.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read<'a> {
    #[allow(dead_code)]
    headers: IgnoredAny,
    #[serde(default, borrow, deserialize_with = "present")]
    payload: Option<&'a RawValue>,
    #[serde(default, borrow)]
    payload_base64: Option<Cow<'a, str>>,
}

/// A `payload` field that is there, `null` included: `null` is a JSON text like any other.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::envelope::{Envelope, Framed, Pattern};

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

    fn document_of(payload: &'static [u8], headers: &HeaderMap) -> Bytes {
        sealed(headers, payload).remove(1)
    }

    fn reopened(document: Bytes) -> (Bytes, HeaderMap) {
        let opened = JSON
            .open(
                Pattern::PushPull,
                VecDeque::from([Bytes::from_static(b"orders"), document]),
            )
            .expect("opens");
        let headers = Framed::<JsonCodec>::headers(&opened.head).expect("reads");
        (opened.payload, headers)
    }

    /// A payload that is JSON text travels as a value of the document, byte for byte.
    #[test]
    fn a_json_payload_is_embedded_as_a_value_and_reads_back_byte_exact() {
        let mut headers = HeaderMap::new();
        headers.insert("event", "created");
        let document = document_of(br#"{"id":7}"#, &headers);
        assert_eq!(
            document.as_ref(),
            br#"{"headers":{"event":"created"},"payload":{"id":7}}"#
        );
        let spaced: &'static [u8] = br#"{ "id" : 7,  "tags":[ "a" ] }"#;
        let (payload, read) = reopened(document_of(spaced, &headers));
        assert_eq!(payload.as_ref(), spaced, "the text is kept as written");
        assert_eq!(read.get_str("event"), Some("created"));
    }

    /// A payload that is not JSON text travels as standard base64 in `payload_base64`.
    #[test]
    fn a_payload_that_is_not_json_travels_as_base64() {
        let mut headers = HeaderMap::new();
        headers.insert("event", "created");
        let document = document_of(b"\x08\x07", &headers);
        assert_eq!(
            document.as_ref(),
            br#"{"headers":{"event":"created"},"payload_base64":"CAc="}"#
        );
        let (payload, read) = reopened(document);
        assert_eq!(payload.as_ref(), b"\x08\x07");
        assert_eq!(read.get_str("event"), Some("created"));
        for text in [&b" 7"[..], b"7 ", b"plain words"] {
            let document = document_of(text, &HeaderMap::new());
            assert!(
                document.windows(14).any(|w| w == b"payload_base64"),
                "{text:?} is not one JSON text as it stands"
            );
            assert_eq!(reopened(document).0.as_ref(), text);
        }
    }

    /// Only the exact shape is a document: anything else is a peer's own record.
    #[test]
    fn a_record_of_another_shape_is_delivered_whole() {
        for record in [
            &br#"{"headers":{},"payload":1,"payload_base64":"AQ=="}"#[..],
            br#"{"headers":{},"payload":1,"id":2}"#,
            br#"{"headers":{}}"#,
            br#"{"payload":1}"#,
            br#"{"headers":{},"payload":[1,2,3],"x":0}"#,
            br#"{"headers":{},"payload_base64":"not base64!"}"#,
        ] {
            let (payload, read) = reopened(Bytes::from_static(record));
            assert_eq!(payload.as_ref(), record);
            assert_eq!(read, HeaderMap::new());
        }
    }

    /// `null` is a JSON text like any other.
    #[test]
    fn a_null_payload_is_a_value_too() {
        let (payload, _) = reopened(document_of(b"null", &HeaderMap::new()));
        assert_eq!(payload.as_ref(), b"null");
    }
}
