//! The documented wire layout: the crate's public contract with non-Rust peers.
//!
//! ```text
//! frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
//! frame 1: headers   UTF-8 "name: value" lines separated by '\n'; may be empty
//! frame 2: payload   encoded by the framework's codec
//! ```
//!
//! A Python peer composes a message as
//! `socket.send_multipart([b"orders", b"content-type: application/json", payload])`.
//! Header values are text; a non-UTF-8 value is replaced lossily on encode.

use bytes::Bytes;
use ruststream::Headers;
use zeromq::ZmqMessage as WireMessage;

use crate::error::ZmqError;

/// Encodes headers into the header frame ("name: value" lines).
pub(crate) fn encode_headers(headers: &Headers) -> Bytes {
    if headers.is_empty() {
        return Bytes::new();
    }
    let mut text = String::new();
    for (name, value) in headers.iter() {
        text.push_str(name);
        text.push_str(": ");
        text.push_str(&String::from_utf8_lossy(value));
        text.push('\n');
    }
    text.pop();
    Bytes::from(text)
}

pub(crate) fn decode_headers(frame: &[u8]) -> Headers {
    let mut headers = Headers::new();
    let text = String::from_utf8_lossy(frame);
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
    headers
}

/// Builds the three-frame message.
pub(crate) fn encode(name: &str, headers: &Headers, payload: &[u8]) -> WireMessage {
    let mut message = WireMessage::from(name);
    message.push_back(encode_headers(headers));
    message.push_back(Bytes::copy_from_slice(payload));
    message
}

/// Splits a received message into (name, headers, payload), tolerating a missing header
/// frame (a two-frame message from a minimal peer).
pub(crate) fn decode(message: WireMessage) -> Result<(String, Headers, Bytes), ZmqError> {
    let mut frames = message.into_vecdeque();
    let name = frames
        .pop_front()
        .ok_or_else(|| ZmqError::Wire("a message needs at least a name frame".into()))?;
    let name = std::str::from_utf8(&name)
        .map_err(|_| ZmqError::Wire("the name frame must be UTF-8".into()))?
        .to_owned();
    let (headers, payload) = match (frames.pop_front(), frames.pop_front()) {
        (Some(headers), Some(payload)) => (decode_headers(&headers), payload),
        (Some(payload), None) => (Headers::new(), payload),
        (None, None) => (Headers::new(), Bytes::new()),
        (None, Some(_)) => unreachable!("pop_front cannot skip"),
    };
    Ok((name, headers, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_frames_round_trip() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert("x-tenant", "acme");
        let message = encode("orders", &headers, b"{}");
        let (name, decoded, payload) = decode(message).expect("decodes");
        assert_eq!(name, "orders");
        assert_eq!(decoded.get_str("content-type"), Some("application/json"));
        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert_eq!(payload.as_ref(), b"{}");
    }

    #[test]
    fn a_two_frame_message_reads_as_headerless() {
        let mut message = zeromq::ZmqMessage::from("orders");
        message.push_back(Bytes::from_static(b"raw"));
        let (name, headers, payload) = decode(message).expect("decodes");
        assert_eq!(name, "orders");
        assert!(headers.is_empty());
        assert_eq!(payload.as_ref(), b"raw");
    }

    #[test]
    fn empty_headers_stay_an_empty_frame() {
        let message = encode("orders", &Headers::new(), b"x");
        assert_eq!(message.get(1).map(Bytes::len), Some(0));
    }
}
