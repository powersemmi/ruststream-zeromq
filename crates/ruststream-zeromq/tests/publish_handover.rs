//! What this crate does with the buffer the framework hands it on the publish path.
//!
//! Every publisher here declares `Take`, so the payload arrives as the buffer the codec wrote and
//! the payload frame is made of it rather than from a copy of it. Content equality cannot tell the
//! two apart, so this compares the address the buffer was written at, read off the in-process
//! transport, which carries the frames the publisher built.

#![cfg(feature = "testing")]

use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{BytesMut, HeaderMap, OutgoingMessage, Publisher};
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue};

/// The payload a peer receives is the buffer the publish wrote, not a copy of it.
#[tokio::test]
async fn the_payload_frame_is_the_buffer_the_publish_wrote() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind("tcp://127.0.0.1:0"))
        .connect_in_process()
        .await
        .expect("connects in process");
    let publisher = connected.publisher();

    let body = BytesMut::from(&br#"{"id":7}"#[..]);
    let written_at = body.as_ptr();
    publisher
        .publish(
            OutgoingMessage::produced("orders", body).with_headers(HeaderMap::new()),
            None,
        )
        .await
        .expect("the publish leaves");

    let recorded = connected.published("orders");
    assert_eq!(
        recorded
            .first()
            .expect("the publish is logged")
            .payload()
            .as_ptr(),
        written_at,
        "the frame must carry the buffer the publish wrote, not a copy of it",
    );
}
