//! What this crate does with the buffer the framework hands it on the publish path.
//!
//! Both publishers here declare `Take`, so the payload arrives as the buffer the codec wrote and
//! the frame is made of it rather than from a copy of it. Content equality cannot tell the two
//! apart, so these tests compare the address the buffer was written at.

#![cfg(feature = "testing")]

use ruststream::testing::TestableBroker;
use ruststream::{Broker, BytesMut, HeaderMap, OutgoingMessage, Publisher};
use ruststream_zeromq::testing::ZmqTestBroker;

/// The delivery the stand records is the buffer the publish wrote, not a copy of it.
#[tokio::test]
async fn the_stand_records_the_buffer_it_was_handed() {
    let connected = ZmqTestBroker::queue().connect().await.expect("connects");
    let publisher = connected.publisher();

    let body = BytesMut::from(&br#"{"id":7}"#[..]);
    let written_at = body.as_ptr();
    publisher
        .publish(
            OutgoingMessage::produced("orders", body).with_headers(HeaderMap::new()),
            None,
        )
        .await
        .expect("the stand accepts the publish");

    let recorded = connected.published("orders");
    assert_eq!(
        recorded
            .first()
            .expect("the publish is logged")
            .payload()
            .as_ptr(),
        written_at,
        "the delivery must carry the buffer the publish wrote, not a copy of it",
    );
}
