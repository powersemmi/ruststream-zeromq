//! The document [`Framed`](super::Framed) writes unless its codec says otherwise: the payload
//! as a byte string.

use std::fmt;

use bytes::Bytes;
use ruststream::codec::Codec;
use ruststream::{BytesMut, HeaderMap};
use serde::de::{self, IgnoredAny, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::headers::{HeaderText, HeadersOnly};
use crate::envelope::SealError;
use crate::error::ZmqError;

/// The document with the payload as a byte string.
pub(super) fn frame(
    codec: &impl Codec,
    headers: &HeaderMap,
    payload: &[u8],
) -> Result<Bytes, SealError> {
    codec
        .encode(&Written {
            headers: HeaderText(headers),
            payload,
        })
        .map(BytesMut::freeze)
        .map_err(|err| SealError::new(format!("the document does not encode: {err}")))
}

/// The payload of a document with a byte-string payload, or `None` for another record.
pub(super) fn body(codec: &impl Codec, document: &[u8]) -> Option<Bytes> {
    codec
        .decode::<Read>(document)
        .ok()
        .map(|read| read.payload.0)
}

/// The headers of a document, whatever its payload field holds.
pub(super) fn headers(codec: &impl Codec, document: &[u8]) -> Result<HeaderMap, ZmqError> {
    codec
        .decode::<HeadersOnly>(document)
        .map(|read| read.headers.0)
        .map_err(|err| ZmqError::Wire(format!("the document's headers do not read: {err}")))
}

struct Written<'a> {
    headers: HeaderText<'a>,
    payload: &'a [u8],
}

impl Serialize for Written<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut document = serializer.serialize_map(Some(2))?;
        document.serialize_entry("headers", &self.headers)?;
        document.serialize_entry("payload", &ByteString(self.payload))?;
        document.end()
    }
}

struct ByteString<'a>(&'a [u8]);

impl Serialize for ByteString<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// What a delivery reads of a document: the payload, the headers skipped unread.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    #[allow(dead_code)]
    headers: IgnoredAny,
    payload: Payload,
}

/// A byte string, and nothing else.
struct Payload(Bytes);

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(PayloadVisitor)
    }
}

struct PayloadVisitor;

impl Visitor<'_> for PayloadVisitor {
    type Value = Payload;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the payload as a byte string")
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Payload, E> {
        Ok(Payload(Bytes::copy_from_slice(bytes)))
    }

    fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Payload, E> {
        Ok(Payload(Bytes::from(bytes)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cbor")]
    use ruststream::codec::CborCodec;
    #[cfg(feature = "msgpack")]
    use ruststream::codec::MsgpackCodec;

    use super::*;
    use crate::envelope::{Envelope, Framed, Pattern};

    /// The binary codecs always write the payload as a byte string in `payload`.
    #[cfg(feature = "cbor")]
    #[test]
    fn cbor_writes_the_payload_as_a_byte_string() {
        let mut frames = Vec::new();
        Framed::new(CborCodec)
            .seal(
                Pattern::PushPull,
                "orders",
                &HeaderMap::new(),
                Bytes::from_static(b"\x08\x07"),
                &mut frames,
            )
            .expect("seals");
        assert_eq!(
            frames[1].as_ref(),
            b"\xa2\x67headers\xa0\x67payload\x42\x08\x07"
        );
        let mut json = Vec::new();
        Framed::new(CborCodec)
            .seal(
                Pattern::PushPull,
                "orders",
                &HeaderMap::new(),
                Bytes::from_static(b"7"),
                &mut json,
            )
            .expect("seals");
        assert_eq!(
            json[1].as_ref(),
            b"\xa2\x67headers\xa0\x67payload\x41\x37",
            "JSON text stays bytes"
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_writes_the_payload_as_a_byte_string() {
        let mut frames = Vec::new();
        Framed::new(MsgpackCodec)
            .seal(
                Pattern::PushPull,
                "orders",
                &HeaderMap::new(),
                Bytes::from_static(b"\x08\x07"),
                &mut frames,
            )
            .expect("seals");
        assert_eq!(
            frames[1].as_ref(),
            b"\x82\xa7headers\x80\xa7payload\xc4\x02\x08\x07"
        );
    }
}

/// A codec of the service's own, with nothing of its own to say about the document.
#[cfg(all(test, feature = "cbor"))]
mod own_codec {
    use ruststream::codec::{CborCodec, CodecError};
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::*;
    use crate::envelope::{Envelope, FrameCodec, Framed, Pattern};

    #[derive(Debug, Clone, Copy)]
    struct Versioned;

    impl Codec for Versioned {
        const CONTENT_TYPE: &'static str = "application/vnd.orders+cbor";

        fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
            CborCodec.encode(value)
        }

        fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
            CborCodec.decode(bytes)
        }
    }

    impl FrameCodec for Versioned {}

    fn sealed(headers: &HeaderMap) -> Result<Vec<Bytes>, SealError> {
        let mut frames = Vec::new();
        Framed::new(Versioned).seal(
            Pattern::PushPull,
            "orders",
            headers,
            Bytes::from_static(b"\x08\x07"),
            &mut frames,
        )?;
        Ok(frames)
    }

    /// A codec that implements the trait with an empty body writes the payload as a byte string,
    /// and reads it and its headers back.
    #[test]
    fn a_codec_of_the_service_writes_the_payload_as_a_byte_string() {
        let mut headers = HeaderMap::new();
        headers.insert("event", "created");
        let frames = sealed(&headers).expect("seals");
        assert_eq!(
            frames[1].as_ref(),
            b"\xa2\x67headers\xa1\x65event\x67created\x67payload\x42\x08\x07"
        );
        let opened = Framed::new(Versioned)
            .open(Pattern::PushPull, frames.into())
            .expect("opens");
        assert_eq!(opened.payload.as_ref(), b"\x08\x07");
        let read = Framed::<Versioned>::headers(&opened.head).expect("reads");
        assert_eq!(read.get_str("event"), Some("created"));
        assert_eq!(
            Framed::<Versioned>::CONTENT_TYPE,
            Some("application/vnd.orders+cbor")
        );
    }

    /// Header values in a document are text, whatever the codec.
    #[test]
    fn a_header_value_that_is_not_text_is_refused_with_its_destination() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", [0xff, 0xfe].as_slice());
        let err = sealed(&headers)
            .expect_err("the value has no text form")
            .against("orders")
            .to_string();
        assert!(err.contains("x-binary") && err.contains("orders"), "{err}");
    }
}
