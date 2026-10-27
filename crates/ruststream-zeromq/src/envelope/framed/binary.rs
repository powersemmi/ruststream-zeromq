//! The CBOR and `MessagePack` documents of [`Framed`]: the payload as a byte string.

use std::fmt;

use bytes::Bytes;
#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
use ruststream::codec::Codec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;
use ruststream::{BytesMut, HeaderMap};
use serde::de::{self, IgnoredAny, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::headers::{HeaderText, HeadersOnly};
use super::{FramedCodec, sealed};
use crate::envelope::SealError;
use crate::error::ZmqError;

/// Writes and reads the document of a binary codec, where the payload is a byte string.
macro_rules! byte_string_document {
    ($codec:ty) => {
        impl sealed::Sealed for $codec {}

        /// The payload is a byte string in `payload`, whatever its bytes are.
        impl FramedCodec for $codec {
            fn seal_document(
                &self,
                headers: &HeaderMap,
                payload: &[u8],
            ) -> Result<Bytes, SealError> {
                seal(self, headers, payload)
            }

            fn open_document(&self, document: &Bytes) -> Option<Bytes> {
                self.decode::<Read>(document)
                    .ok()
                    .map(|read| read.payload.0)
            }

            fn document_headers(document: &Bytes) -> Result<HeaderMap, ZmqError> {
                headers(&Self, document)
            }
        }
    };
}

#[cfg(feature = "cbor")]
byte_string_document!(CborCodec);
#[cfg(feature = "msgpack")]
byte_string_document!(MsgpackCodec);

fn seal(codec: &impl Codec, headers: &HeaderMap, payload: &[u8]) -> Result<Bytes, SealError> {
    codec
        .encode(&Written {
            headers: HeaderText(headers),
            payload,
        })
        .map(BytesMut::freeze)
        .map_err(|err| SealError::new(format!("the document does not encode: {err}")))
}

fn headers(codec: &impl Codec, document: &[u8]) -> Result<HeaderMap, ZmqError> {
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
