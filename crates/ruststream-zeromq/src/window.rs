//! The window that holds a sending socket back when its peer does not keep up.
//!
//! The client accepts a send as soon as the message is in a queue of 100,000 messages per peer,
//! and that queue is not configurable. A PUSH socket waits once a peer's queue is full, so on its
//! own it bounds a sender by a count that no payload size makes sensible. A PUB socket drops what a
//! subscriber's full queue cannot take, so a subscriber that does not keep up loses messages
//! instead of holding the publisher back. [`SendWindow`] bounds a sender by what it sent instead:
//! it marks one message per window, and a publisher that has filled a second window waits until
//! the mark of the first has left the client. The messages leave in order, so by then every
//! message before the mark has left too.
//!
//! A mark is a frame of the message that the publisher keeps a handle to, the anchor. The client
//! drops the message once it has written it or lost its peer, which leaves the anchor the only
//! holder of its buffer, and the publisher reads that off the buffer's reference count. A PUB
//! socket hands each subscriber a copy that shares the frames, so the anchor is released once the
//! last subscriber's copy has left. A mark reaches only the peers that receive its message: a
//! PUSH socket deals messages to its peers in turn and a PUB socket sends each to the subscribers
//! whose filters match, so with several peers a slow one that the marks miss is held only by the
//! client's own queue. The
//! anchor is the first small frame of the message (the name, in the default layout) and is kept
//! from window to window while that frame stays the same, so a mark allocates and copies nothing.
//! A message without a small frame is marked by its last frame, which costs the one allocation
//! that sharing a buffer for the first time takes. A message of nothing but static or owned
//! frames, which no count can tell released, is marked by wrapping its last frame in an owner
//! that gives a signal when the client drops it: one allocation a window, on that path alone.
//!
//! The reference count has no wake-up, so the wait checks it. It checks without pause for the
//! first millisecond, which is what a peer that keeps up needs, and then every millisecond, so a
//! peer that holds the mark for longer costs a timer tick of latency and no spinning.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Notify;
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

/// The last frame of a message marked by its drop: the frame itself, and the signal to give when
/// the client is done with it.
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

/// What the last window's mark is waited for by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum Held {
    /// No mark is in the client's queue.
    #[default]
    Nothing,
    /// The anchor is the only holder of its buffer once the client has dropped the message.
    Anchor,
    /// The signal is given when the client drops the message's last frame.
    Signal,
}

/// What a sending socket has sent since its last mark, and how that mark is waited for.
#[derive(Default)]
pub(crate) struct SendWindow {
    messages: u32,
    bytes: usize,
    /// A handle to a frame of the last marked message; the message's own handle is the other.
    anchor: Option<Bytes>,
    /// Given by a [`Mark`]; one is in the queue at a time, so one signal serves them all. Made
    /// with the first mark that needs it.
    left: Option<Arc<Notify>>,
    /// How the last mark is still to be waited for.
    held: Held,
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
        match self.held {
            Held::Nothing => {}
            Held::Anchor => {
                if let Some(anchor) = &self.anchor {
                    released(anchor).await;
                    // A large frame is not kept: the publisher would hold its buffer until the
                    // next mark.
                    if anchor.len() > KEPT_FRAME {
                        self.anchor = None;
                    }
                }
            }
            Held::Signal => {
                if let Some(left) = &self.left {
                    left.notified().await;
                }
            }
        }
        self.messages = 0;
        self.bytes = 0;
        let (frames, held) = self.mark(frames);
        self.held = held;
        frames
    }

    /// Marks `frames`, in the cheapest way the message allows, and says how the mark is waited
    /// for.
    ///
    /// The anchor of the last window is reused when the message still has its frame. Otherwise
    /// any frame the publisher holds alone becomes the anchor, since a static or owned frame
    /// never reads as released. A message of nothing but such frames is marked by its last frame,
    /// which costs an allocation.
    fn mark(&mut self, frames: ZmqMessage) -> (ZmqMessage, Held) {
        let mut frames = frames.into_vec();
        let small = frames
            .iter()
            .position(|frame| !frame.is_empty() && frame.len() <= KEPT_FRAME);
        let held = match (&self.anchor, small) {
            // The same frame as the last window's: its handle goes into the message, and the
            // frame this message made is dropped.
            (Some(anchor), Some(index)) if *anchor == frames[index] => {
                frames[index] = anchor.clone();
                Held::Anchor
            }
            _ => {
                let index = small
                    .filter(|&index| frames[index].is_unique())
                    .or_else(|| frames.iter().rposition(Bytes::is_unique));
                if let Some(index) = index {
                    self.anchor = Some(frames[index].clone());
                    Held::Anchor
                } else {
                    self.anchor = None;
                    let left = Arc::clone(self.left.get_or_insert_with(|| Arc::new(Notify::new())));
                    // A message always has a frame: `ZmqMessage` cannot be empty.
                    let frame = frames.pop().unwrap_or_default();
                    frames.push(Bytes::from_owner(Mark { frame, left }));
                    Held::Signal
                }
            }
        };
        (
            ZmqMessage::try_from(frames).expect("a message with a frame still has one"),
            held,
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

    /// A message of static frames cannot be told released by a count, and the window holds back
    /// all the same.
    #[tokio::test]
    async fn a_message_of_static_frames_is_waited_for() {
        let fixed = || {
            ZmqMessage::try_from(vec![Bytes::from_static(b"jobs"), Bytes::from_static(b"{}")])
                .expect("a message with frames")
        };
        let mut window = SendWindow::default();
        for _ in 1..WINDOW_MESSAGES {
            window.admit(fixed(), 8).await;
        }
        let held = window.admit(fixed(), 8).await;
        for _ in 1..WINDOW_MESSAGES {
            window.admit(fixed(), 8).await;
        }
        let mut admit = Box::pin(window.admit(fixed(), 8));
        assert!(poll!(admit.as_mut()).is_pending(), "the mark is still held");
        drop(held);
        let _ = admit.await;
    }

    /// One message of a window's worth of bytes closes the window on its own.
    #[tokio::test]
    async fn a_large_message_closes_a_window_by_its_size() {
        let mut window = SendWindow::default();
        let marked = window.admit(message(b"jobs"), WINDOW_BYTES).await;
        assert_eq!(
            window.held,
            Held::Anchor,
            "one large message is a whole window"
        );
        drop(marked);
    }
}
