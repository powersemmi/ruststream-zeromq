//! The headers of a [`Framed`](super::Framed) document, which every codec writes the same way.

use std::{fmt, str};

use ruststream::HeaderMap;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The headers as a map of text values; [`Framed::seal`](super::Framed) refuses a value that is not text first.
pub(super) struct HeaderText<'a>(pub(super) &'a HeaderMap);

impl Serialize for HeaderText<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(
            self.0
                .iter()
                .map(|(name, value)| (name, str::from_utf8(value).unwrap_or_default())),
        )
    }
}

/// The headers, read straight into the map a delivery hands out.
pub(super) struct Headers(pub(super) HeaderMap);

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

/// The headers alone, whatever else the document holds.
#[derive(Deserialize)]
pub(super) struct HeadersOnly {
    pub(super) headers: Headers,
}
