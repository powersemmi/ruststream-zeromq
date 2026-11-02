//! The window that holds a sending socket back when its peer does not keep up.
//!
//! The client accepts a send as soon as the message is in a queue of 100,000 messages per peer,
//! and that queue is not configurable, so on its own it bounds a sender by a count that no payload
//! size makes sensible. [`SendWindow`] bounds it by what the publisher sent instead: it marks one
//! message per window, and a publisher that has filled a second window waits until the mark of the
//! first has left the client's queue. The messages leave in order, so by then every message before
//! the mark has left too.
//!
//! The mark is the last frame of a message, wrapped once per window in an owner that wakes the
//! publisher when the client drops it, which the client does once it has written the message or
//! lost its peer. A message that is not a mark pays two additions and two comparisons, and a mark
//! pays two allocations and an atomic, once per window.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;
use zeromq::ZmqMessage;

/// Messages in a window.
const WINDOW_MESSAGES: u32 = 256;

/// Payload bytes in a window, so that large messages close a window after a few.
const WINDOW_BYTES: usize = 1 << 20;

/// The last frame of a marked message: the frame itself, and the signal to give when the client is
/// done with it.
struct Mark {
    frame: Bytes,
    left: Arc<Notify>,
}

impl AsRef<[u8]> for Mark {
    fn as_ref(&self) -> &[u8] {
        &self.frame
    }
}

impl Drop for Mark {
    fn drop(&mut self) {
        self.left.notify_one();
    }
}

/// What a sending socket has sent since its last mark, and the mark still in the client's queue.
#[derive(Default)]
pub(crate) struct SendWindow {
    messages: u32,
    bytes: usize,
    /// Signalled when the message marked at the end of the previous window has left the client.
    previous: Option<Arc<Notify>>,
}

impl SendWindow {
    /// Counts a message of `size` payload bytes, and at the end of a window waits for the mark of
    /// the window before it and marks `frames`. Returns the frames to send.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe. Dropped while it waits, nothing has been counted and nothing is sent.
    pub(crate) async fn admit(&mut self, frames: ZmqMessage, size: usize) -> ZmqMessage {
        let bytes = self.bytes.saturating_add(size);
        if self.messages + 1 < WINDOW_MESSAGES && bytes < WINDOW_BYTES {
            self.messages += 1;
            self.bytes = bytes;
            return frames;
        }
        if let Some(previous) = &self.previous {
            previous.notified().await;
        }
        self.messages = 0;
        self.bytes = 0;
        let left = Arc::new(Notify::new());
        self.previous = Some(Arc::clone(&left));
        mark(frames, left)
    }
}

/// Wraps the last frame of `frames` so that `left` is signalled when the client drops it.
fn mark(frames: ZmqMessage, left: Arc<Notify>) -> ZmqMessage {
    let mut frames = frames.into_vec();
    // A message always has a frame: `ZmqMessage` cannot be empty.
    let frame = frames.pop().unwrap_or_default();
    frames.push(Bytes::from_owner(Mark { frame, left }));
    ZmqMessage::try_from(frames).expect("a message with a frame still has one")
}
