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
//!
//! Headers are text in both directions, and neither direction guesses. A header value that is not
//! UTF-8 has no representation in the frame, so publishing it returns an error naming the header
//! and the destination. A header frame that is not UTF-8, or a line with no `:`, returns a wire
//! error instead of dropping the header: a peer that composes the frame by hand hears about it on
//! the first message rather than losing headers silently. Blank lines are skipped, so a peer that
//! ends the frame with a newline interoperates.

use bytes::Bytes;
use ruststream::HeaderMap;
use zeromq::ZmqMessage as WireMessage;

use crate::error::ZmqError;

/// Encodes headers into the header frame ("name: value" lines).
///
/// Returns the reason a value cannot be written, for the caller to report against its
/// destination. The frame is text, so a value that is not UTF-8 has no representation in it.
fn encode_headers(headers: &HeaderMap) -> Result<Bytes, String> {
    if headers.is_empty() {
        return Ok(Bytes::new());
    }
    let mut text = String::new();
    for (name, value) in headers.iter() {
        let value = std::str::from_utf8(value).map_err(|_| {
            format!("header '{name}' holds a value that is not UTF-8, and the header frame is text")
        })?;
        text.push_str(name);
        text.push_str(": ");
        text.push_str(value);
        text.push('\n');
    }
    text.pop();
    Ok(Bytes::from(text))
}

fn decode_headers(frame: &[u8]) -> Result<HeaderMap, ZmqError> {
    let text = std::str::from_utf8(frame)
        .map_err(|_| ZmqError::Wire("the header frame must be UTF-8".into()))?;
    let mut headers = HeaderMap::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| {
            ZmqError::Wire(format!(
                "header line '{line}' has no ':' separating name from value"
            ))
        })?;
        headers.insert(name.trim().to_owned(), value.trim().to_owned());
    }
    Ok(headers)
}

/// Builds the three-frame message.
///
/// The payload becomes the third frame as it is: this crate's publishers declare `Take`, so the
/// buffer the framework wrote arrives here and the frame is made of it.
///
/// Returns the reason a header cannot be written, so the caller reports it against the
/// destination it is publishing to rather than against the name in frame 0, which on a reply is
/// the literal `reply`.
pub(crate) fn encode(
    name: &str,
    headers: &HeaderMap,
    payload: Bytes,
) -> Result<WireMessage, String> {
    let headers = encode_headers(headers)?;
    let mut message = WireMessage::from(name);
    message.push_back(headers);
    message.push_back(payload);
    Ok(message)
}

/// Frames a message for `destination`, reporting a header that cannot be written as a send
/// error naming it.
pub(crate) fn encode_to(
    destination: &str,
    name: &str,
    headers: &HeaderMap,
    payload: Bytes,
) -> Result<WireMessage, ZmqError> {
    encode(name, headers, payload).map_err(|reason| ZmqError::Send {
        name: destination.to_owned(),
        reason,
    })
}

/// Splits a received message into (name, headers, payload), tolerating a missing header
/// frame (a two-frame message from a minimal peer).
pub(crate) fn decode(message: WireMessage) -> Result<(String, HeaderMap, Bytes), ZmqError> {
    let mut frames = message.into_vecdeque();
    let name = frames
        .pop_front()
        .ok_or_else(|| ZmqError::Wire("a message needs at least a name frame".into()))?;
    let name = std::str::from_utf8(&name)
        .map_err(|_| ZmqError::Wire("the name frame must be UTF-8".into()))?
        .to_owned();
    let (headers, payload) = match (frames.pop_front(), frames.pop_front()) {
        (Some(headers), Some(payload)) => (decode_headers(&headers)?, payload),
        (Some(payload), None) => (HeaderMap::new(), payload),
        (None, None) => (HeaderMap::new(), Bytes::new()),
        (None, Some(_)) => unreachable!("pop_front cannot skip"),
    };
    Ok((name, headers, payload))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    /// The frame is made of the buffer the publish wrote, not of a copy of it: this crate takes
    /// the payload, and a frame owns its bytes anyway.
    #[test]
    fn the_payload_frame_is_the_buffer_that_was_handed_in() {
        let body = BytesMut::from(&br#"{"id":7}"#[..]);
        let written_at = body.as_ptr();
        let message = encode("orders", &HeaderMap::new(), body.freeze()).expect("encodes");
        assert_eq!(
            message.get(2).expect("the payload frame").as_ptr(),
            written_at,
            "the frame must carry the buffer the publish wrote, not a copy of it",
        );
    }

    #[test]
    fn three_frames_round_trip() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("x-tenant", "acme");
        let message = encode("orders", &headers, Bytes::from_static(b"{}")).expect("encodes");
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
        let message =
            encode("orders", &HeaderMap::new(), Bytes::from_static(b"x")).expect("encodes");
        assert_eq!(message.get(1).map(Bytes::len), Some(0));
    }

    #[test]
    fn a_value_that_is_not_text_is_refused_with_its_destination() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", [0xff, 0xfe].as_slice());
        let err = encode_to("orders", "reply", &headers, Bytes::from_static(b"{}"))
            .expect_err("a binary value has no frame");
        let message = err.to_string();
        assert!(message.contains("x-binary"), "names the header: {message}");
        assert!(
            message.contains("orders"),
            "names the destination: {message}"
        );
    }

    #[test]
    fn a_header_frame_that_is_not_text_is_refused() {
        let mut message = zeromq::ZmqMessage::from("orders");
        message.push_back(Bytes::from_static(&[0xff, 0xfe]));
        message.push_back(Bytes::from_static(b"{}"));
        assert!(decode(message).is_err());
    }

    /// The line is kept rather than dropped, so a peer composing the frame by hand learns of the
    /// missing separator instead of wondering where its header went.
    #[test]
    fn a_header_line_without_a_separator_is_refused() {
        let mut message = zeromq::ZmqMessage::from("orders");
        message.push_back(Bytes::from_static(b"content-type application/json"));
        message.push_back(Bytes::from_static(b"{}"));
        let err = decode(message).expect_err("a line with no ':' is malformed");
        assert!(err.to_string().contains("content-type application/json"));
    }

    /// A hand-written peer that joins its lines with a trailing newline is not malformed.
    #[test]
    fn blank_header_lines_are_skipped() {
        let mut message = zeromq::ZmqMessage::from("orders");
        message.push_back(Bytes::from_static(b"x-tenant: acme\n"));
        message.push_back(Bytes::from_static(b"{}"));
        let (_, headers, _) = decode(message).expect("decodes");
        assert_eq!(headers.get_str("x-tenant"), Some("acme"));
    }
}
