//! [`Envelope`]: how a message is laid out in frames, with [`Frames`] as the default and
//! [`Framed`] and [`NoOp`] beside it.
//!
//! - [Envelopes][envelopes]: the three built-in layouts, how a broker and a publish policy choose
//!   one, and an envelope of a service's own.
//! - [Wire format][wire]: the [`Frames`] layout, the crate's contract with peers in other
//!   languages.
//!
//! [envelopes]: https://powersemmi.github.io/ruststream-zeromq/latest/components/wire/envelopes/
//! [wire]: https://powersemmi.github.io/ruststream-zeromq/latest/components/wire/

mod framed;
pub(crate) mod frames;
mod noop;

use std::collections::VecDeque;

use bytes::Bytes;
use ruststream::HeaderMap;

use zeromq::ZmqMessage as WireMessage;

use crate::error::ZmqError;
use crate::message::ZmqMessage;

pub use self::framed::Framed;
pub use self::frames::Frames;
pub use self::noop::NoOp;

/// The socket pattern a message travels over, which decides the frames a layout needs.
///
/// A PUB/SUB message needs a name frame in front, because a SUB socket matches its prefix
/// against the first frame. The other patterns route without it.
///
/// # Examples
///
/// An envelope of a service's own writes the name frame only where a SUB filters on it:
///
/// ```
/// use std::collections::VecDeque;
///
/// use ruststream::{Bytes, HeaderMap};
/// use ruststream_zeromq::envelope::{Envelope, Opened, Pattern, SealError};
/// use ruststream_zeromq::queue::prelude::*;
/// use ruststream_zeromq::ZmqError;
/// use serde::Deserialize;
///
/// /// The payload alone, behind the name frame where a SUB filters on it.
/// #[derive(Debug, Clone, Copy)]
/// struct Bare;
///
/// impl Envelope for Bare {
///     type Head = ();
///
///     const NAME: &'static str = "bare";
///
///     const CARRIES_HEADERS: bool = false;
///
///     fn open(&self, pattern: Pattern, mut frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
///         let name = match (pattern, frames.len()) {
///             (Pattern::PubSub, 2) => frames.pop_front().unwrap_or_default(),
///             _ => Bytes::new(),
///         };
///         let name = String::from_utf8(name.to_vec())
///             .map_err(|_| ZmqError::Wire("the name frame must be UTF-8".into()))?;
///         let payload = frames.pop_back().unwrap_or_default();
///         Ok(Opened::new(name, (), payload))
///     }
///
///     fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
///         Ok(HeaderMap::new())
///     }
///
///     fn seal(
///         &self,
///         pattern: Pattern,
///         name: &str,
///         headers: &HeaderMap,
///         payload: Bytes,
///         frames: &mut Vec<Bytes>,
///     ) -> Result<(), SealError> {
///         if let Some((header, _)) = headers.iter().next() {
///             return Err(SealError::new(format!("header {header:?} has no frame in this layout")));
///         }
///         if pattern == Pattern::PubSub {
///             frames.push(Bytes::copy_from_slice(name.as_bytes()));
///         }
///         frames.push(payload);
///         Ok(())
///     }
/// }
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
///     let jobs = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")).envelope(Bare);
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(jobs, |b| {
///         b.include(work);
///     })
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Pattern {
    /// PUSH/PULL, the [`ZmqQueue`](crate::ZmqQueue).
    PushPull,
    /// PUB/SUB, the [`ZmqFanout`](crate::ZmqFanout): the first frame is what a SUB filters on.
    PubSub,
    /// DEALER/ROUTER, the [`ZmqRpc`](crate::ZmqRpc), behind the identity frame the ROUTER adds
    /// and takes off.
    DealerRouter,
}

/// What an [`Envelope`] read off the frames of a received message: the name, the payload, and
/// what the delivery keeps to read its headers from when a handler asks for them.
///
/// # Examples
///
/// An envelope that reads a one-frame `name payload` message, the layout of many hand-written
/// PUB sockets, opens it into the name before the space and the payload after it:
///
/// ```
/// # use std::collections::VecDeque;
/// #
/// # use ruststream::{Bytes, HeaderMap};
/// # use ruststream_zeromq::envelope::{Envelope, Opened, Pattern, SealError};
/// # use ruststream_zeromq::fanout::prelude::*;
/// # use ruststream_zeromq::ZmqError;
/// # use serde::Deserialize;
/// #
/// # /// One frame: the name, a space, and the payload.
/// # #[derive(Debug, Clone, Copy)]
/// # struct Spaced;
/// #
/// # impl Envelope for Spaced {
/// #     type Head = ();
/// #
/// #     const NAME: &'static str = "spaced";
/// #
/// #     const CARRIES_HEADERS: bool = false;
///     fn open(&self, _pattern: Pattern, frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
///         let frame = frames.into_iter().next().unwrap_or_default();
///         let Some(at) = frame.iter().position(|&byte| byte == b' ') else {
///             // A frame without a space is a peer's payload, with no name.
///             return Ok(Opened::new("", (), frame));
///         };
///         let name = std::str::from_utf8(&frame[..at])
///             .map_err(|_| ZmqError::Wire("the name must be UTF-8".into()))?;
///         Ok(Opened::new(name, (), frame.slice(at + 1..)))
///     }
/// #     fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
/// #         Ok(HeaderMap::new())
/// #     }
/// #     fn seal(
/// #         &self,
/// #         _pattern: Pattern,
/// #         name: &str,
/// #         headers: &HeaderMap,
/// #         payload: Bytes,
/// #         frames: &mut Vec<Bytes>,
/// #     ) -> Result<(), SealError> {
/// #         if let Some((header, _)) = headers.iter().next() {
/// #             return Err(SealError::new(format!("header {header:?} has no place in the frame")));
/// #         }
/// #         frames.push([name.as_bytes(), b" ", &payload].concat().into());
/// #         Ok(())
/// #     }
/// # }
/// # #[derive(Deserialize)]
/// # struct Tick {
/// #     price: u64,
/// # }
/// #
/// # #[subscriber("ticks")]
/// # async fn record(tick: &Tick) -> HandlerOutcome {
/// #     tracing::info!(tick.price, "recorded");
/// #     HandlerOutcome::ack()
/// # }
/// #
/// # #[ruststream::app]
/// # fn app() -> impl App {
/// #     let ticks = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).envelope(Spaced);
/// #     RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(ticks, |b| {
/// #         b.include(record);
/// #     })
/// # }
/// ```
#[derive(Debug)]
pub struct Opened<Head> {
    pub(crate) name: String,
    pub(crate) head: Head,
    pub(crate) payload: Bytes,
}

impl<Head> Opened<Head> {
    /// A received message: its name (empty where the layout carries none), what its headers are
    /// read from later, and the payload the handler decodes.
    ///
    /// # Examples
    ///
    /// An envelope that reads a one-frame `name payload` message opens it into the name before
    /// the space and the payload after it:
    ///
    /// ```
    /// # use std::collections::VecDeque;
    /// #
    /// # use ruststream::{Bytes, HeaderMap};
    /// # use ruststream_zeromq::envelope::{Envelope, Opened, Pattern, SealError};
    /// # use ruststream_zeromq::fanout::prelude::*;
    /// # use ruststream_zeromq::ZmqError;
    /// # use serde::Deserialize;
    /// #
    /// # /// One frame: the name, a space, and the payload.
    /// # #[derive(Debug, Clone, Copy)]
    /// # struct Spaced;
    /// #
    /// # impl Envelope for Spaced {
    /// #     type Head = ();
    /// #
    /// #     const NAME: &'static str = "spaced";
    /// #
    /// #     const CARRIES_HEADERS: bool = false;
    ///     fn open(&self, _pattern: Pattern, frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
    ///         let frame = frames.into_iter().next().unwrap_or_default();
    ///         let Some(at) = frame.iter().position(|&byte| byte == b' ') else {
    ///             // A frame without a space is a peer's payload, with no name.
    ///             return Ok(Opened::new("", (), frame));
    ///         };
    ///         let name = std::str::from_utf8(&frame[..at])
    ///             .map_err(|_| ZmqError::Wire("the name must be UTF-8".into()))?;
    ///         Ok(Opened::new(name, (), frame.slice(at + 1..)))
    ///     }
    /// #     fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
    /// #         Ok(HeaderMap::new())
    /// #     }
    /// #     fn seal(
    /// #         &self,
    /// #         _pattern: Pattern,
    /// #         name: &str,
    /// #         headers: &HeaderMap,
    /// #         payload: Bytes,
    /// #         frames: &mut Vec<Bytes>,
    /// #     ) -> Result<(), SealError> {
    /// #         if let Some((header, _)) = headers.iter().next() {
    /// #             return Err(SealError::new(format!("header {header:?} has no place in the frame")));
    /// #         }
    /// #         frames.push([name.as_bytes(), b" ", &payload].concat().into());
    /// #         Ok(())
    /// #     }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Tick {
    /// #     price: u64,
    /// # }
    /// #
    /// # #[subscriber("ticks")]
    /// # async fn record(tick: &Tick) -> HandlerOutcome {
    /// #     tracing::info!(tick.price, "recorded");
    /// #     HandlerOutcome::ack()
    /// # }
    /// #
    /// # #[ruststream::app]
    /// # fn app() -> impl App {
    /// #     let ticks = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).envelope(Spaced);
    /// #     RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(ticks, |b| {
    /// #         b.include(record);
    /// #     })
    /// # }
    /// ```
    #[must_use]
    pub fn new(name: impl Into<String>, head: Head, payload: Bytes) -> Self {
        Self {
            name: name.into(),
            head,
            payload,
        }
    }
}

/// Why an [`Envelope`] cannot write a message as it is: a header its layout has no place for,
/// or one it would carry as something else.
///
/// A publish reports it as [`ZmqError::Send`], naming the destination.
///
/// # Examples
///
/// An envelope that writes a message as one `name payload` frame has no place for a header,
/// and refuses one rather than drop it:
///
/// ```
/// # use std::collections::VecDeque;
/// #
/// # use ruststream::{Bytes, HeaderMap};
/// # use ruststream_zeromq::envelope::{Envelope, Opened, Pattern, SealError};
/// # use ruststream_zeromq::fanout::prelude::*;
/// # use ruststream_zeromq::ZmqError;
/// # use serde::Deserialize;
/// #
/// # /// One frame: the name, a space, and the payload.
/// # #[derive(Debug, Clone, Copy)]
/// # struct Spaced;
/// #
/// # impl Envelope for Spaced {
/// #     type Head = ();
/// #
/// #     const NAME: &'static str = "spaced";
/// #
/// #     const CARRIES_HEADERS: bool = false;
/// #     fn open(&self, _pattern: Pattern, frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
/// #         let frame = frames.into_iter().next().unwrap_or_default();
/// #         let Some(at) = frame.iter().position(|&byte| byte == b' ') else {
/// #             // A frame without a space is a peer's payload, with no name.
/// #             return Ok(Opened::new("", (), frame));
/// #         };
/// #         let name = std::str::from_utf8(&frame[..at])
/// #             .map_err(|_| ZmqError::Wire("the name must be UTF-8".into()))?;
/// #         Ok(Opened::new(name, (), frame.slice(at + 1..)))
/// #     }
/// #     fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
/// #         Ok(HeaderMap::new())
/// #     }
///     fn seal(
///         &self,
///         _pattern: Pattern,
///         name: &str,
///         headers: &HeaderMap,
///         payload: Bytes,
///         frames: &mut Vec<Bytes>,
///     ) -> Result<(), SealError> {
///         if let Some((header, _)) = headers.iter().next() {
///             return Err(SealError::new(format!("header {header:?} has no place in the frame")));
///         }
///         frames.push([name.as_bytes(), b" ", &payload].concat().into());
///         Ok(())
///     }
/// }
/// # #[derive(Deserialize)]
/// # struct Tick {
/// #     price: u64,
/// # }
/// #
/// # #[subscriber("ticks")]
/// # async fn record(tick: &Tick) -> HandlerOutcome {
/// #     tracing::info!(tick.price, "recorded");
/// #     HandlerOutcome::ack()
/// # }
/// #
/// # #[ruststream::app]
/// # fn app() -> impl App {
/// #     let ticks = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).envelope(Spaced);
/// #     RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(ticks, |b| {
/// #         b.include(record);
/// #     })
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SealError(String);

impl SealError {
    /// A refusal with the reason a publish reports against its destination.
    ///
    /// # Examples
    ///
    /// An envelope that writes a message as one `name payload` frame refuses a header:
    ///
    /// ```
    /// # use std::collections::VecDeque;
    /// #
    /// # use ruststream::{Bytes, HeaderMap};
    /// # use ruststream_zeromq::envelope::{Envelope, Opened, Pattern, SealError};
    /// # use ruststream_zeromq::fanout::prelude::*;
    /// # use ruststream_zeromq::ZmqError;
    /// # use serde::Deserialize;
    /// #
    /// # /// One frame: the name, a space, and the payload.
    /// # #[derive(Debug, Clone, Copy)]
    /// # struct Spaced;
    /// #
    /// # impl Envelope for Spaced {
    /// #     type Head = ();
    /// #
    /// #     const NAME: &'static str = "spaced";
    /// #
    /// #     const CARRIES_HEADERS: bool = false;
    /// #     fn open(&self, _pattern: Pattern, frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
    /// #         let frame = frames.into_iter().next().unwrap_or_default();
    /// #         let Some(at) = frame.iter().position(|&byte| byte == b' ') else {
    /// #             // A frame without a space is a peer's payload, with no name.
    /// #             return Ok(Opened::new("", (), frame));
    /// #         };
    /// #         let name = std::str::from_utf8(&frame[..at])
    /// #             .map_err(|_| ZmqError::Wire("the name must be UTF-8".into()))?;
    /// #         Ok(Opened::new(name, (), frame.slice(at + 1..)))
    /// #     }
    /// #     fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
    /// #         Ok(HeaderMap::new())
    /// #     }
    ///     fn seal(
    ///         &self,
    ///         _pattern: Pattern,
    ///         name: &str,
    ///         headers: &HeaderMap,
    ///         payload: Bytes,
    ///         frames: &mut Vec<Bytes>,
    ///     ) -> Result<(), SealError> {
    ///         if let Some((header, _)) = headers.iter().next() {
    ///             return Err(SealError::new(format!("header {header:?} has no place in the frame")));
    ///         }
    ///         frames.push([name.as_bytes(), b" ", &payload].concat().into());
    ///         Ok(())
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Tick {
    /// #     price: u64,
    /// # }
    /// #
    /// # #[subscriber("ticks")]
    /// # async fn record(tick: &Tick) -> HandlerOutcome {
    /// #     tracing::info!(tick.price, "recorded");
    /// #     HandlerOutcome::ack()
    /// # }
    /// #
    /// # #[ruststream::app]
    /// # fn app() -> impl App {
    /// #     let ticks = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).envelope(Spaced);
    /// #     RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(ticks, |b| {
    /// #         b.include(record);
    /// #     })
    /// # }
    /// ```
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }

    /// The refusal as the error a publish to `destination` returns.
    pub(crate) fn against(self, destination: &str) -> ZmqError {
        ZmqError::Send {
            name: destination.to_owned(),
            reason: self.0,
        }
    }
}

/// How a message is laid out in `ZeroMQ` frames: what a publish writes and what a subscription
/// reads.
///
/// A broker reads and writes through [`Frames`] unless it names another with `.envelope(..)`,
/// and a publish policy writes through its broker's envelope unless it names one of its own the
/// same way. The envelope is a type parameter of the broker and the publisher, so the choice
/// costs nothing per message.
///
/// A delivery keeps the [`Head`](Self::Head) [`open`](Self::open) returned and reads its headers
/// from it on the first call to `headers()`. A handler that reads no header never parses them.
///
/// The test harness runs a broker in process by its type, so a test of a service on an envelope
/// of its own registers the broker type once, beside the test:
/// `ruststream::register_testable_broker!(ZmqQueue<Bind, MyEnvelope>);`.
///
/// # Examples
///
/// A recorder reads the ticks a feed sends as one `name payload` frame, the layout of many
/// hand-written PUB sockets, which has no place for headers:
///
/// ```
/// use std::collections::VecDeque;
///
/// use ruststream::{Bytes, HeaderMap};
/// use ruststream_zeromq::envelope::{Envelope, Opened, Pattern, SealError};
/// use ruststream_zeromq::fanout::prelude::*;
/// use ruststream_zeromq::ZmqError;
/// use serde::Deserialize;
///
/// /// One frame: the name, a space, and the payload.
/// #[derive(Debug, Clone, Copy)]
/// struct Spaced;
///
/// impl Envelope for Spaced {
///     type Head = ();
///
///     const NAME: &'static str = "spaced";
///
///     const CARRIES_HEADERS: bool = false;
///     fn open(&self, _pattern: Pattern, frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
///         let frame = frames.into_iter().next().unwrap_or_default();
///         let Some(at) = frame.iter().position(|&byte| byte == b' ') else {
///             // A frame without a space is a peer's payload, with no name.
///             return Ok(Opened::new("", (), frame));
///         };
///         let name = std::str::from_utf8(&frame[..at])
///             .map_err(|_| ZmqError::Wire("the name must be UTF-8".into()))?;
///         Ok(Opened::new(name, (), frame.slice(at + 1..)))
///     }
///     fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
///         Ok(HeaderMap::new())
///     }
///     fn seal(
///         &self,
///         _pattern: Pattern,
///         name: &str,
///         headers: &HeaderMap,
///         payload: Bytes,
///         frames: &mut Vec<Bytes>,
///     ) -> Result<(), SealError> {
///         if let Some((header, _)) = headers.iter().next() {
///             return Err(SealError::new(format!("header {header:?} has no place in the frame")));
///         }
///         frames.push([name.as_bytes(), b" ", &payload].concat().into());
///         Ok(())
///     }
/// }
/// #[derive(Deserialize)]
/// struct Tick {
///     price: u64,
/// }
///
/// #[subscriber("ticks")]
/// async fn record(tick: &Tick) -> HandlerOutcome {
///     tracing::info!(tick.price, "recorded");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let ticks = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).envelope(Spaced);
///     RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(ticks, |b| {
///         b.include(record);
///     })
/// }
/// ```
pub trait Envelope: Clone + Send + Sync + 'static {
    /// What a delivery keeps of its frames to read its headers from when they are asked for:
    /// the header frame of [`Frames`], `()` for a layout without headers.
    type Head: Send + Sync + 'static;

    /// The name the generated `AsyncAPI` document gives this layout.
    const NAME: &'static str;

    /// The media type of the frame this layout writes the headers and the payload into, which the
    /// generated document reports beside the layout's name; `None` where the payload travels in
    /// a frame of its own.
    const CONTENT_TYPE: Option<&'static str> = None;

    /// Whether this layout carries headers. A request through one that does not is matched to its
    /// answer by its socket alone, without a `correlation-id`.
    const CARRIES_HEADERS: bool = true;

    /// Reads a received message, everything but the identity frame a ROUTER puts in front of a
    /// request.
    ///
    /// It locates the name and the payload and keeps what the headers are read from, without
    /// reading them: [`headers`](Self::headers) does that, once, when a handler asks.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::Wire`] when the frames cannot be a message of this layout, and the
    /// subscription reports it instead of a delivery.
    fn open(
        &self,
        pattern: Pattern,
        frames: VecDeque<Bytes>,
    ) -> Result<Opened<Self::Head>, ZmqError>;

    /// Reads the headers out of what [`open`](Self::open) kept.
    ///
    /// # Errors
    ///
    /// Returns [`ZmqError::Wire`] when the headers cannot be read. The delivery then has no
    /// headers, and the failure is logged against its name.
    fn headers(head: &Self::Head) -> Result<HeaderMap, ZmqError>;

    /// Writes a message into `frames`, after any frame the pattern puts in front of it.
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] when a header has no place in the layout, or would read back as
    /// something else; the publish fails with it, naming the destination.
    fn seal(
        &self,
        pattern: Pattern,
        name: &str,
        headers: &HeaderMap,
        payload: Bytes,
        frames: &mut Vec<Bytes>,
    ) -> Result<(), SealError>;
}

/// The name frame as text.
pub(crate) fn decode_name(frame: &[u8]) -> Result<String, ZmqError> {
    std::str::from_utf8(frame)
        .map(str::to_owned)
        .map_err(|_| ZmqError::Wire("the name frame must be UTF-8".into()))
}

/// Reads a message a socket received through `envelope`.
pub(crate) fn read<Env: Envelope>(
    envelope: &Env,
    pattern: Pattern,
    frames: VecDeque<Bytes>,
) -> Result<ZmqMessage<Env>, ZmqError> {
    envelope.open(pattern, frames).map(ZmqMessage::new)
}

/// Frames a message for `destination` through `envelope`, behind `front` where a pattern
/// addresses it with a frame of its own (the peer identity a ROUTER routes a reply by).
///
/// The list is reserved for the largest built-in layout once, so a message is one allocation for
/// its frames rather than one per frame. The payload becomes a frame as it is: this crate's
/// publishers declare `Take`, so the buffer the framework wrote arrives here and the frame is
/// made of it.
pub(crate) fn write<Env: Envelope>(
    envelope: &Env,
    pattern: Pattern,
    destination: &str,
    front: Option<Bytes>,
    message: (&str, &HeaderMap, Bytes),
) -> Result<WireMessage, ZmqError> {
    let (name, headers, payload) = message;
    let mut frames = Vec::with_capacity(LARGEST_LAYOUT + usize::from(front.is_some()));
    frames.extend(front);
    envelope
        .seal(pattern, name, headers, payload, &mut frames)
        .map_err(|refused| refused.against(destination))?;
    WireMessage::try_from(frames).map_err(|_| ZmqError::Send {
        name: destination.to_owned(),
        reason: format!("the {} envelope wrote no frame", Env::NAME),
    })
}

/// The frames of [`Frames`], the largest built-in layout.
const LARGEST_LAYOUT: usize = 3;

/// A publish policy that writes through the envelope `Env` rather than its broker's: what
/// `.envelope(..)` on [`ZmqQueuePublish`](crate::ZmqQueuePublish),
/// [`ZmqFanoutPublish`](crate::ZmqFanoutPublish) or [`ZmqRpcPublish`](crate::ZmqRpcPublish)
/// returns.
///
/// # Examples
///
/// A worker reads its jobs in the default layout and pushes each result to a sink that reads
/// bare payloads:
///
/// ```
/// use ruststream_zeromq::envelope::NoOp;
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", reply)]
/// async fn work(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let results = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
///     let to_results = results.bind(Publish.envelope(NoOp));
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(work).out_reply(to_results);
///         })
///         .with_broker(results, |_b| {})
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[must_use]
pub struct Enveloped<Policy, Env> {
    pub(crate) policy: Policy,
    pub(crate) envelope: Env,
}
