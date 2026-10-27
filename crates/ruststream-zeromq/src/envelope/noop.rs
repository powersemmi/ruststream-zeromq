//! [`NoOp`]: the payload alone, with no envelope at all.

use std::collections::VecDeque;

use bytes::Bytes;
use ruststream::HeaderMap;

use super::{Envelope, Opened, Pattern, SealError, decode_name};
use crate::error::ZmqError;

/// The payload alone: one frame, behind the name frame on the fan-out, where a SUB filters on it.
///
/// It is the layout a plain `ZeroMQ` peer reads and writes: whatever arrives is the payload, and
/// a delivery has no headers. A publish that carries a header fails, naming the header and the
/// destination, since the layout has no place for it. On DEALER/ROUTER a request reads with the
/// `reply-to` address the ROUTER gives it, and the requester takes the first answer on its socket
/// as the answer, without a `correlation-id`.
///
/// # Examples
///
/// A worker takes the jobs a plain PUSH peer sends as one frame each, `send(payload)`:
///
/// ```
/// use ruststream_zeromq::envelope::NoOp;
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
///     let jobs = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")).envelope(NoOp);
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(jobs, |b| {
///         b.include(work);
///     })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NoOp;

impl Envelope for NoOp {
    type Head = ();

    const NAME: &'static str = "none";

    const CARRIES_HEADERS: bool = false;

    fn open(&self, pattern: Pattern, mut frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
        let count = frames.len();
        match (pattern, frames.pop_front(), frames.pop_front()) {
            (_, Some(payload), None) => Ok(Opened::new(String::new(), (), payload)),
            (Pattern::PubSub, Some(name), Some(payload)) if frames.is_empty() => {
                Ok(Opened::new(decode_name(&name)?, (), payload))
            }
            (Pattern::PubSub, ..) => Err(ZmqError::Wire(format!(
                "a message of {count} frames is not a payload behind its name frame"
            ))),
            _ => Err(ZmqError::Wire(format!(
                "a message of {count} frames is not a payload of one frame"
            ))),
        }
    }

    fn headers((): &()) -> Result<HeaderMap, ZmqError> {
        Ok(HeaderMap::new())
    }

    fn seal(
        &self,
        pattern: Pattern,
        name: &str,
        headers: &HeaderMap,
        payload: Bytes,
        frames: &mut Vec<Bytes>,
    ) -> Result<(), SealError> {
        if let Some((header, _)) = headers.iter().next() {
            return Err(SealError::new(format!(
                "header {header:?} has no place in a message without an envelope, which carries \
                 its payload alone"
            )));
        }
        if pattern == Pattern::PubSub {
            frames.push(Bytes::copy_from_slice(name.as_bytes()));
        }
        frames.push(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed(
        pattern: Pattern,
        headers: &HeaderMap,
        payload: &'static [u8],
    ) -> Result<Vec<Bytes>, SealError> {
        let mut frames = Vec::new();
        NoOp.seal(
            pattern,
            "orders",
            headers,
            Bytes::from_static(payload),
            &mut frames,
        )?;
        Ok(frames)
    }

    /// The queue and the exchange route without a name, so the payload is the whole message.
    #[test]
    fn the_payload_is_the_only_frame_where_nothing_filters_on_a_name() {
        for pattern in [Pattern::PushPull, Pattern::DealerRouter] {
            let frames = sealed(pattern, &HeaderMap::new(), b"{}").expect("seals");
            assert_eq!(frames, [Bytes::from_static(b"{}")], "{pattern:?}");
            let opened = NoOp.open(pattern, frames.into()).expect("opens");
            assert_eq!(
                (opened.name.as_str(), opened.payload.as_ref()),
                ("", &b"{}"[..])
            );
        }
    }

    /// A SUB matches its prefix against the first frame, so the fan-out keeps the name there.
    #[test]
    fn the_fan_out_keeps_the_name_frame_its_subscribers_filter_on() {
        let frames = sealed(Pattern::PubSub, &HeaderMap::new(), b"{}").expect("seals");
        assert_eq!(
            frames,
            [Bytes::from_static(b"orders"), Bytes::from_static(b"{}")]
        );
        let opened = NoOp.open(Pattern::PubSub, frames.into()).expect("opens");
        assert_eq!(
            (opened.name.as_str(), opened.payload.as_ref()),
            ("orders", &b"{}"[..])
        );
    }

    /// The layout has no place for a header, so dropping it would lose it silently: the publish
    /// fails instead, naming the header and the destination.
    #[test]
    fn a_header_is_refused_with_its_name_and_destination() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        for pattern in [Pattern::PushPull, Pattern::PubSub, Pattern::DealerRouter] {
            let err = sealed(pattern, &headers, b"{}")
                .expect_err("the layout carries no header")
                .against("orders")
                .to_string();
            assert!(
                err.contains("x-tenant") && err.contains("orders"),
                "the refusal names the header and the destination, got: {err}",
            );
        }
    }

    /// A frame the layout has no place for is refused rather than dropped.
    #[test]
    fn a_message_of_more_frames_than_the_layout_is_refused() {
        let three = || {
            VecDeque::from([
                Bytes::from_static(b"orders"),
                Bytes::new(),
                Bytes::from_static(b"{}"),
            ])
        };
        for pattern in [Pattern::PushPull, Pattern::PubSub, Pattern::DealerRouter] {
            let err = NoOp
                .open(pattern, three())
                .expect_err("three frames are not a bare payload")
                .to_string();
            assert!(err.contains("3 frames"), "names the count, got: {err}");
        }
    }

    #[test]
    fn a_delivery_has_no_headers() {
        assert_eq!(NoOp::headers(&()).expect("reads"), HeaderMap::new());
    }
}
