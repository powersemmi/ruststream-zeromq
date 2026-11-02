//! The window that holds a sending socket back when its peer does not keep up.
//!
//! The client accepts a send as soon as the message is in a queue of 100,000 messages per peer,
//! and that queue is not configurable, so on its own it bounds a sender by a count that no payload
//! size makes sensible. [`SendWindow`] bounds it by what the publisher sent instead: it marks one
//! message per window, and a publisher that has filled a second window waits until the mark of the
//! first has left the client. The messages leave in order, so by then every message before the
//! mark has left too.
//!
//! A mark is a frame of the message that the publisher keeps a handle to, the anchor. The client
//! drops the message once it has written it or lost its peer, which leaves the anchor the only
//! holder of its buffer, and the publisher reads that off the buffer's reference count. The
//! anchor is the first small frame of the message (the name, in the default layout) and is kept
//! from window to window while that frame stays the same, so a mark allocates and copies nothing.
//! A message without a small frame is marked by its last frame, which costs the one allocation
//! that sharing a buffer for the first time takes.
//!
//! The reference count has no wake-up, so the wait checks it. It checks without pause for the
//! first millisecond, which is what a peer that keeps up needs, and then every millisecond, so a
//! peer that holds the mark for longer costs a timer tick of latency and no spinning.

use std::time::{Duration, Instant};

use bytes::Bytes;
use zeromq::ZmqMessage;

/// Messages in a window.
const WINDOW_MESSAGES: u32 = 256;

/// Payload bytes in a window, so that large messages close a window after a few.
const WINDOW_BYTES: usize = 1 << 20;

/// The longest frame that stays the anchor from one window to the next.
const KEPT_FRAME: usize = 256;

/// How long the wait checks the anchor again and again, each time handing the thread back, before
/// it turns into sleeping. A peer that keeps up releases a mark within this, and a sleep would
/// cost it a timer tick.
const SPIN_FOR: Duration = Duration::from_millis(1);

/// The sleep between checks once the peer has held the mark for longer than [`SPIN_FOR`].
const NAP: Duration = Duration::from_millis(1);

/// What a sending socket has sent since its last mark, and the anchor of that mark.
#[derive(Default)]
pub(crate) struct SendWindow {
    messages: u32,
    bytes: usize,
    /// A handle to a frame of the last marked message; the message's own handle is the other.
    anchor: Option<Bytes>,
    /// Whether the last mark is still to be waited for.
    outstanding: bool,
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
        if self.outstanding {
            if let Some(anchor) = &self.anchor {
                released(anchor).await;
                // A large frame is not kept: the publisher would hold its buffer until the next mark.
                if anchor.len() > KEPT_FRAME {
                    self.anchor = None;
                }
            }
            self.outstanding = false;
        }
        self.messages = 0;
        self.bytes = 0;
        let (frames, marked) = self.mark(frames);
        self.outstanding = marked;
        frames
    }

    /// Makes one frame of `frames` the anchor, and says whether there was one to make.
    fn mark(&mut self, frames: ZmqMessage) -> (ZmqMessage, bool) {
        let mut frames = frames.into_vec();
        let small = frames
            .iter()
            .position(|frame| !frame.is_empty() && frame.len() <= KEPT_FRAME);
        let last = frames.len() - 1;
        let index = small.unwrap_or(last);
        let marked = match &self.anchor {
            // The same frame as the last window's: its handle goes into the message, and the
            // frame this message made is dropped.
            Some(anchor) if small.is_some() && *anchor == frames[index] => {
                frames[index] = anchor.clone();
                true
            }
            // Only a frame this publisher holds alone can be told from its count: a static or
            // owned one never reads as released.
            _ if frames[index].is_unique() => {
                self.anchor = Some(frames[index].clone());
                true
            }
            _ => {
                self.anchor = None;
                false
            }
        };
        (
            ZmqMessage::try_from(frames).expect("a message with a frame still has one"),
            marked,
        )
    }
}

/// Resolves once the client has dropped the message the anchor was cloned into.
async fn released(anchor: &Bytes) {
    let started = Instant::now();
    while !anchor.is_unique() {
        if started.elapsed() < SPIN_FOR {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(NAP).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;

    use super::*;

    fn message(name: &'static [u8]) -> ZmqMessage {
        ZmqMessage::try_from(vec![
            Bytes::copy_from_slice(name),
            Bytes::copy_from_slice(b"{}"),
            Bytes::copy_from_slice(b"payload"),
        ])
        .expect("a message with frames")
    }

    /// Sends messages until the window marks one, and returns that message.
    async fn until_marked(window: &mut SendWindow, name: &'static [u8]) -> ZmqMessage {
        for _ in 1..WINDOW_MESSAGES {
            window.admit(message(name), 8).await;
        }
        window.admit(message(name), 8).await
    }

    /// The frame that marks a window is the one the last window marked, so a window after the
    /// first allocates and copies nothing.
    #[tokio::test]
    async fn the_name_frame_marks_every_window() {
        let mut window = SendWindow::default();
        let first = until_marked(&mut window, b"jobs").await;
        let first_name = first.get(0).expect("a name frame").as_ptr();
        drop(first);
        let second = until_marked(&mut window, b"jobs").await;
        assert_eq!(
            second.get(0).expect("a name frame").as_ptr(),
            first_name,
            "the second window reuses the first window's mark"
        );
    }

    /// A name that changes makes the next mark of the new name.
    #[tokio::test]
    async fn a_new_name_makes_a_new_mark() {
        let mut window = SendWindow::default();
        drop(until_marked(&mut window, b"jobs").await);
        let renamed = until_marked(&mut window, b"orders").await;
        assert_eq!(renamed.get(0).map(Bytes::as_ref), Some(&b"orders"[..]));
        assert!(
            window
                .anchor
                .as_ref()
                .is_some_and(|anchor| *anchor == renamed.get(0).expect("a name")),
            "the anchor follows the name"
        );
    }

    /// The second window after a mark waits until the client has dropped the marked message, and
    /// goes on the moment it has.
    #[tokio::test]
    async fn a_window_waits_for_the_mark_before_it() {
        let mut window = SendWindow::default();
        let held = until_marked(&mut window, b"jobs").await;
        for _ in 1..WINDOW_MESSAGES {
            window.admit(message(b"jobs"), 8).await;
        }
        let mut admit = Box::pin(window.admit(message(b"jobs"), 8));
        assert!(poll!(admit.as_mut()).is_pending(), "the mark is still held");
        drop(held);
        let _ = admit.await;
    }

    /// One message of a window's worth of bytes closes the window on its own.
    #[tokio::test]
    async fn a_large_message_closes_a_window_by_its_size() {
        let mut window = SendWindow::default();
        let marked = window.admit(message(b"jobs"), WINDOW_BYTES).await;
        assert!(window.outstanding, "one large message is a whole window");
        drop(marked);
    }
}
