//! [`Frames`]: the documented wire layout, the crate's contract with peers in other languages.
//!
//! ```text
//! frame 0: name      UTF-8; also the subscription prefix for the fan-out pattern
//! frame 1: headers   UTF-8 "name: value" lines separated by '\n'; may be empty
//! frame 2: payload   encoded by the framework's codec
//! ```
//!
//! Two frames are a name and a payload, with no headers. One frame is a plain `ZeroMQ` message,
//! the frame a SUB matched its prefix against: it is the payload, whole, and the name is empty.
//! A message of more than three frames is refused with a wire error.
//!
//! A Python peer composes a message as
//! `socket.send_multipart([b"orders", b"content-type: application/json", payload])`.
//!
//! Headers are text in both directions, and neither direction guesses. A header value that is not
//! UTF-8 has no representation in the frame, so publishing it returns an error naming the header
//! and the destination. So does a header that would read back as something else: a line break in
//! its name or value, a `:` in its name, or whitespace at either end of either. A delivery reads
//! its header frame when a handler first asks for a header; a frame that is not UTF-8, or a line
//! with no `:`, is logged against the message and reads as no headers. Blank lines are skipped,
//! so a peer that ends the frame with a newline interoperates.

use std::collections::VecDeque;

use bytes::Bytes;
use ruststream::HeaderMap;

use super::{Envelope, Opened, Pattern, SealError, decode_name};
use crate::error::ZmqError;

/// The three-frame layout, name, headers and payload: the default of every broker, and the one
/// a peer in another language composes with `send_multipart`.
///
/// A message of two frames reads as a name and a payload without headers; one of a single frame,
/// what a plain `ZeroMQ` peer sends, reads as the payload, whole, with an empty name. The header
/// frame is `name: value` lines of UTF-8 text, read when a handler first asks for a header.
///
/// # Examples
///
/// A worker reads the jobs a Python peer pushes as
/// `send_multipart([b"jobs", b"x-tenant: acme", payload])`. `Frames` is what a broker reads and
/// writes when it names no envelope; naming it spells the default out:
///
/// ```
/// use ruststream_zeromq::envelope::Frames;
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn work(job: &Job, ctx: &mut Context) -> HandlerOutcome {
///     let tenant = ctx.headers().get_str("x-tenant").unwrap_or("none");
///     tracing::info!(job.id, tenant, "working on the job");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let jobs = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")).envelope(Frames);
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(jobs, |b| {
///         b.include(work);
///     })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Frames;

impl Envelope for Frames {
    /// The header frame as it was received; empty when the message had none.
    type Head = Bytes;

    const NAME: &'static str = "frames";

    fn open(
        &self,
        _pattern: Pattern,
        mut frames: VecDeque<Bytes>,
    ) -> Result<Opened<Bytes>, ZmqError> {
        let count = frames.len();
        match (frames.pop_front(), frames.pop_front(), frames.pop_front()) {
            (Some(payload), None, None) => Ok(Opened::new(String::new(), Bytes::new(), payload)),
            (Some(name), Some(payload), None) => {
                Ok(Opened::new(decode_name(&name)?, Bytes::new(), payload))
            }
            (Some(name), Some(headers), Some(payload)) if frames.is_empty() => {
                Ok(Opened::new(decode_name(&name)?, headers, payload))
            }
            _ => Err(ZmqError::Wire(format!(
                "a message of {count} frames does not follow the layout of one to three frames \
                 (name, headers, payload)"
            ))),
        }
    }

    fn headers(head: &Bytes) -> Result<HeaderMap, ZmqError> {
        decode_headers(head)
    }

    fn seal(
        &self,
        _pattern: Pattern,
        name: &str,
        headers: &HeaderMap,
        payload: Bytes,
        frames: &mut Vec<Bytes>,
    ) -> Result<(), SealError> {
        let headers = encode_headers(headers).map_err(SealError::new)?;
        frames.push(Bytes::copy_from_slice(name.as_bytes()));
        frames.push(headers);
        frames.push(payload);
        Ok(())
    }
}

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

/// The frames a peer that is not this crate sends: the header lines written as the bytes they
/// hold, with none of the checks this crate's own publishers make, so a value that is not UTF-8
/// reaches the receiving side as a header frame that does not read.
#[cfg(feature = "testing")]
pub(crate) fn foreign(name: &str, headers: &HeaderMap, payload: Bytes) -> Vec<Bytes> {
    let mut text = Vec::new();
    for (name, value) in headers.iter() {
        if !text.is_empty() {
            text.push(b'\n');
        }
        text.extend_from_slice(name.as_bytes());
        text.extend_from_slice(b": ");
        text.extend_from_slice(value);
    }
    vec![
        Bytes::copy_from_slice(name.as_bytes()),
        Bytes::from(text),
        payload,
    ]
}

/// Reads the header frame. A value is a slice of the frame rather than a copy of it.
fn decode_headers(frame: &Bytes) -> Result<HeaderMap, ZmqError> {
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
        headers.insert(
            name.trim().to_owned(),
            frame.slice_ref(value.trim().as_bytes()),
        );
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    fn sealed(name: &str, headers: &HeaderMap, payload: Bytes) -> Result<Vec<Bytes>, SealError> {
        let mut frames = Vec::new();
        Frames.seal(Pattern::PushPull, name, headers, payload, &mut frames)?;
        Ok(frames)
    }

    fn opened(frames: Vec<Bytes>) -> Result<(String, HeaderMap, Bytes), ZmqError> {
        let opened = Frames.open(Pattern::PushPull, frames.into())?;
        Ok((opened.name, Frames::headers(&opened.head)?, opened.payload))
    }

    /// The frame is made of the buffer the publish wrote, not of a copy of it: this crate takes
    /// the payload, and a frame owns its bytes anyway.
    #[test]
    fn the_payload_frame_is_the_buffer_that_was_handed_in() {
        let body = BytesMut::from(&br#"{"id":7}"#[..]);
        let written_at = body.as_ptr();
        let frames = sealed("orders", &HeaderMap::new(), body.freeze()).expect("seals");
        assert_eq!(
            frames[2].as_ptr(),
            written_at,
            "the frame must carry the buffer the publish wrote, not a copy of it",
        );
    }

    /// A header value is a slice of the frame it arrived in, so reading the headers copies no
    /// value.
    #[test]
    fn a_header_value_is_a_slice_of_the_frame() {
        let frame = Bytes::from_static(b"x-tenant: acme");
        let headers = Frames::headers(&frame).expect("reads");
        let value = headers.get_shared("x-tenant").expect("the header");
        assert_eq!(value, "acme");
        assert_eq!(
            value.as_ptr(),
            frame[10..].as_ptr(),
            "the value is not copied"
        );
    }

    #[test]
    fn three_frames_round_trip() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("x-tenant", "acme");
        let frames = sealed("orders", &headers, Bytes::from_static(b"{}")).expect("seals");
        let (name, decoded, payload) = opened(frames).expect("opens");
        assert_eq!(name, "orders");
        assert_eq!(decoded.get_str("content-type"), Some("application/json"));
        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert_eq!(payload.as_ref(), b"{}");
    }

    #[test]
    fn a_two_frame_message_reads_as_headerless() {
        let (name, headers, payload) = opened(vec![
            Bytes::from_static(b"orders"),
            Bytes::from_static(b"raw"),
        ])
        .expect("opens");
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
            let (name, headers, payload) =
                opened(vec![Bytes::copy_from_slice(frame)]).expect("a single frame opens");
            assert_eq!(name, "", "a single frame carries no name frame");
            assert_eq!(headers, HeaderMap::new());
            assert_eq!(payload.as_ref(), frame, "the single frame is the payload");
        }
    }

    /// The layout holds one payload frame, so a frame past it has nowhere to go: the message is
    /// refused, naming how many frames it had, rather than read with its tail cut off.
    #[test]
    fn a_frame_past_the_payload_is_refused() {
        let err = opened(vec![
            Bytes::from_static(b"orders"),
            Bytes::new(),
            Bytes::from_static(b"part one"),
            Bytes::from_static(b"part two"),
        ])
        .expect_err("a fourth frame is not part of the layout")
        .to_string();
        assert!(
            err.contains("4 frames"),
            "the error names the count, got: {err}"
        );
    }

    #[test]
    fn empty_headers_stay_an_empty_frame() {
        let frames = sealed("orders", &HeaderMap::new(), Bytes::from_static(b"x")).expect("seals");
        assert_eq!(frames[1].len(), 0);
    }

    #[test]
    fn a_value_that_is_not_text_is_refused_with_its_destination() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", [0xff, 0xfe].as_slice());
        let err = sealed("reply", &headers, Bytes::from_static(b"{}"))
            .expect_err("a binary value has no frame")
            .against("orders");
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
            let err = sealed("orders", &headers, Bytes::from_static(b"{}"))
                .expect_err("the frame cannot carry this header as written")
                .against("orders")
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
        let frames = sealed("orders", &headers, Bytes::from_static(b"{}")).expect("seals");
        let (_, decoded, _) = opened(frames).expect("opens");
        assert_eq!(decoded.get_str("x-note"), Some("a: b  c"));
    }

    /// The header frame is read only when the headers are, so a frame that is not text opens and
    /// fails only on that read.
    #[test]
    fn a_header_frame_that_is_not_text_fails_only_its_read() {
        let opened = Frames
            .open(
                Pattern::PushPull,
                vec![
                    Bytes::from_static(b"orders"),
                    Bytes::from_static(&[0xff, 0xfe]),
                    Bytes::from_static(b"{}"),
                ]
                .into(),
            )
            .expect("the message opens without reading its headers");
        assert_eq!(opened.payload.as_ref(), b"{}");
        assert!(Frames::headers(&opened.head).is_err());
    }

    /// The line is reported rather than dropped, so a peer composing the frame by hand finds the
    /// missing separator in the log.
    #[test]
    fn a_header_line_without_a_separator_is_refused() {
        let err = Frames::headers(&Bytes::from_static(b"content-type application/json"))
            .expect_err("a line with no ':' is malformed");
        assert!(err.to_string().contains("content-type application/json"));
    }

    /// A hand-written peer that joins its lines with a trailing newline is not malformed.
    #[test]
    fn blank_header_lines_are_skipped() {
        let headers = Frames::headers(&Bytes::from_static(b"x-tenant: acme\n")).expect("reads");
        assert_eq!(headers.get_str("x-tenant"), Some("acme"));
    }
}
