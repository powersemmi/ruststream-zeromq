//! The documented wire layout: the crate's public contract with non-Rust peers.
//!
//! ```text
//! frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
//! frame 1: headers   UTF-8 "name: value" lines separated by '\n'; may be empty
//! frame 2: payload   encoded by the framework's codec
//! ```
//!
//! Two frames are a name and a payload, with no headers. One frame is a plain `ZeroMQ` message,
//! the frame a SUB matched its prefix against: it is the payload, whole, and the name is empty.
//!
//! A Python peer composes a message as
//! `socket.send_multipart([b"orders", b"content-type: application/json", payload])`.
//!
//! Headers are text in both directions, and neither direction guesses. A header value that is not
//! UTF-8 has no representation in the frame, so publishing it returns an error naming the header
//! and the destination. So does a header that would read back as something else: a line break in
//! its name or value, a `:` in its name, or whitespace at either end of either. A header frame
//! that is not UTF-8, or a line with no `:`, returns a wire error instead of dropping the header:
//! a peer that composes the frame by hand hears about it on the first message rather than losing
//! headers silently. Blank lines are skipped, so a peer that ends the frame with a newline
//! interoperates.

use bytes::Bytes;
use ruststream::HeaderMap;
use zeromq::ZmqMessage as WireMessage;

use crate::error::ZmqError;
use crate::message::ZmqMessage;

/// Why a header cannot be written into the frame as it is, or `None` when the line it makes reads
/// back as the same name and value.
///
/// The reader splits the frame into lines, splits a line at its first `:` and trims both sides,
/// so a line break anywhere, a `:` in the name, or whitespace at either end of the name or the
/// value would reach the receiver as a different header, or as a frame it refuses. Refusing them
/// here keeps the frame format every peer already reads. The check is a scan of bytes the encoder
/// copies anyway, and allocates only to describe a refusal.
fn unwritable(name: &str, value: &str) -> Option<&'static str> {
    let breaks_line = |text: &str| text.bytes().any(|b| b == b'\n' || b == b'\r');
    let padded = |text: &str| text.trim() != text;
    if breaks_line(name) || breaks_line(value) {
        Some("holds a line break, and each header is one line of the frame")
    } else if name.contains(':') {
        Some("has a ':' in its name, and the first ':' of a line ends the name")
    } else if padded(name) || padded(value) {
        Some("starts or ends with whitespace, which the reader trims")
    } else {
        None
    }
}

/// Encodes headers into the header frame ("name: value" lines).
///
/// Returns the reason a header cannot be written, for the caller to report against its
/// destination. The frame is text, so a value that is not UTF-8 has no representation in it, and
/// a header [`unwritable`] describes would read back as something else.
fn encode_headers(headers: &HeaderMap) -> Result<Bytes, String> {
    if headers.is_empty() {
        return Ok(Bytes::new());
    }
    let mut text = String::new();
    for (name, value) in headers.iter() {
        let value = std::str::from_utf8(value).map_err(|_| {
            format!("header '{name}' holds a value that is not UTF-8, and the header frame is text")
        })?;
        if let Some(reason) = unwritable(name, value) {
            return Err(format!("header {name:?} {reason}"));
        }
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

/// Builds the message out of its frames, behind `front` where a pattern addresses it with a
/// frame of its own (the peer identity a ROUTER routes a reply by).
///
/// How many frames there are is known before the first one is written, so the list is built at
/// its final size rather than grown per frame. The payload becomes the last frame as it is: this
/// crate's publishers declare `Take`, so the buffer the framework wrote arrives here and the
/// frame is made of it.
fn frames(
    front: Option<Bytes>,
    name: &str,
    headers: &HeaderMap,
    payload: Bytes,
) -> Result<WireMessage, String> {
    let headers = encode_headers(headers)?;
    let name = Bytes::copy_from_slice(name.as_bytes());
    let frames = match front {
        Some(front) => vec![front, name, headers, payload],
        None => vec![name, headers, payload],
    };
    Ok(WireMessage::try_from(frames).expect("a list built from named frames is never empty"))
}

/// Builds the three-frame message.
///
/// Returns the reason a header cannot be written, so the caller reports it against the
/// destination it is publishing to rather than against the name in frame 0, which on a reply is
/// the literal `reply`.
pub(crate) fn encode(
    name: &str,
    headers: &HeaderMap,
    payload: Bytes,
) -> Result<WireMessage, String> {
    frames(None, name, headers, payload)
}

/// Frames a reply behind the peer identity that routes it, reporting a header that cannot be
/// written as a send error naming the destination.
pub(crate) fn encode_addressed_to(
    destination: &str,
    identity: Bytes,
    name: &str,
    headers: &HeaderMap,
    payload: Bytes,
) -> Result<WireMessage, ZmqError> {
    frames(Some(identity), name, headers, payload).map_err(|reason| ZmqError::Send {
        name: destination.to_owned(),
        reason,
    })
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

/// The frames a peer that is not this crate sends: the header lines written as the bytes they
/// hold, with none of the checks this crate's own publishers make, so a value that is not UTF-8
/// reaches the receiving side as the frame [`decode`] refuses.
#[cfg(feature = "testing")]
pub(crate) fn encode_foreign(name: &str, headers: &HeaderMap, payload: Bytes) -> WireMessage {
    let mut text = Vec::new();
    for (name, value) in headers.iter() {
        if !text.is_empty() {
            text.push(b'\n');
        }
        text.extend_from_slice(name.as_bytes());
        text.extend_from_slice(b": ");
        text.extend_from_slice(value);
    }
    let frames = vec![
        Bytes::copy_from_slice(name.as_bytes()),
        Bytes::from(text),
        payload,
    ];
    WireMessage::try_from(frames).expect("a list built from named frames is never empty")
}

/// A delivery of a one-way pattern, read off the frames its socket received.
///
/// The PULL and SUB drivers read every message through here, and so does the in-process
/// transport, so a delivery reads the same whichever carried it. `None` is a message the reader
/// skips; a one-way socket skips none.
// The `Option` is the shape every subscription's reader shares: the ROUTER's reader skips a
// message with no identity frame, and the in-process transport holds either reader by one type.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn read_delivery(message: WireMessage) -> Option<Result<ZmqMessage, ZmqError>> {
    Some(decode(message).map(|(name, headers, payload)| ZmqMessage::new(name, headers, payload)))
}

/// Splits a received message into (name, headers, payload).
///
/// Three frames are the layout; two are a name and a payload with no headers. One frame is the
/// message a plain `ZeroMQ` peer sends, and the frame a SUB matched its prefix against: it is the
/// payload, whole, with an empty name and no headers.
pub(crate) fn decode(message: WireMessage) -> Result<(String, HeaderMap, Bytes), ZmqError> {
    let mut frames = message.into_vecdeque();
    match (frames.pop_front(), frames.pop_front(), frames.pop_front()) {
        (Some(payload), None, None) => Ok((String::new(), HeaderMap::new(), payload)),
        (Some(name), Some(payload), None) => Ok((decode_name(&name)?, HeaderMap::new(), payload)),
        (Some(name), Some(headers), Some(payload)) => {
            Ok((decode_name(&name)?, decode_headers(&headers)?, payload))
        }
        _ => Err(ZmqError::Wire("a message needs at least one frame".into())),
    }
}

fn decode_name(frame: &[u8]) -> Result<String, ZmqError> {
    std::str::from_utf8(frame)
        .map(str::to_owned)
        .map_err(|_| ZmqError::Wire("the name frame must be UTF-8".into()))
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

    /// The list holds exactly the frames there are: how many a message has is known before the
    /// first one is written, so building it is one allocation rather than one per frame.
    #[test]
    fn the_frame_list_is_built_at_its_final_size() {
        let message =
            encode("orders", &HeaderMap::new(), Bytes::from_static(b"{}")).expect("encodes");
        let frames = message.into_vecdeque();
        assert_eq!(frames.len(), 3, "name, headers, payload");
        assert_eq!(
            frames.capacity(),
            frames.len(),
            "a list grown frame by frame ends up larger than the message it holds",
        );
    }

    /// The same for a reply, which carries the peer identity in front of the three.
    #[test]
    fn a_reply_is_framed_behind_its_identity_at_its_final_size() {
        let message = encode_addressed_to(
            "orders",
            Bytes::from_static(&[0xde, 0xad]),
            "reply",
            &HeaderMap::new(),
            Bytes::from_static(b"{}"),
        )
        .expect("encodes");
        let frames = message.into_vecdeque();
        assert_eq!(frames.len(), 4, "identity, name, headers, payload");
        assert_eq!(
            frames.capacity(),
            frames.len(),
            "a list grown frame by frame ends up larger than the message it holds",
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
        assert_eq!(headers, HeaderMap::new());
        assert_eq!(payload.as_ref(), b"raw");
    }

    /// A plain `ZeroMQ` peer sends its message as one frame, and a SUB matches its prefix against
    /// that frame. The frame is the payload, whole, as a plain receiver reads it; there is no name
    /// frame, so the name is empty, and the bytes need not be text.
    #[test]
    fn a_single_frame_is_the_payload() {
        for frame in [&b"orders{\"id\":7}"[..], &[0xff, 0x00, 0xfe][..]] {
            let message = zeromq::ZmqMessage::from(Bytes::copy_from_slice(frame));
            let (name, headers, payload) = decode(message).expect("a single frame decodes");
            assert_eq!(name, "", "a single frame carries no name frame");
            assert_eq!(headers, HeaderMap::new());
            assert_eq!(payload.as_ref(), frame, "the single frame is the payload");
        }
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

    /// A header that would read back as a different header, or as a frame the receiver refuses,
    /// is refused before the socket is touched, naming the header and the destination.
    #[test]
    fn a_header_the_lines_cannot_hold_is_refused_with_its_destination() {
        let cases = [
            ("x-note", "first\nsecond"),
            ("x-note", "first\rsecond"),
            ("x-tenant", "acme\nx-ruststream-retry-count: 9"),
            ("x-note", " padded"),
            ("x-note", "padded\t"),
            ("x-a:b", "value"),
            ("x-a\nb", "value"),
            ("x-note ", "value"),
        ];
        for (name, value) in cases {
            let mut headers = HeaderMap::new();
            headers.insert(name, value);
            let err = encode_to("orders", "orders", &headers, Bytes::from_static(b"{}"))
                .expect_err("the frame cannot carry this header as written")
                .to_string();
            assert!(
                err.contains(&format!("{name:?}")) && err.contains("orders"),
                "the refusal must name the header and the destination, got: {err}",
            );
        }
    }

    /// Only the ends of a value are trimmed by the reader, so inner whitespace and a `:` inside a
    /// value round-trip.
    #[test]
    fn inner_spaces_and_separators_of_a_value_round_trip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-note", "a: b  c");
        let message = encode("orders", &headers, Bytes::from_static(b"{}")).expect("encodes");
        let (_, decoded, _) = decode(message).expect("decodes");
        assert_eq!(decoded.get_str("x-note"), Some("a: b  c"));
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
