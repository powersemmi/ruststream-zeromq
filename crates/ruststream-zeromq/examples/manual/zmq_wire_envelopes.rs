//! The service of `examples/zmq_wire_envelopes.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_wire_envelopes --features testing,json
//! ```

use std::collections::VecDeque;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::codec::JsonCodec;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream::{Bytes, HeaderMap};
use serde::{Deserialize, Serialize};

use ruststream_zeromq::ZmqError;
use ruststream_zeromq::envelope::{Envelope, Framed, NoOp, Opened, Pattern, SealError};
use ruststream_zeromq::prelude::*;

// --8<-- [start:envelope]
/// One frame: the name, a space, and the payload, the layout of many hand-written PUB sockets.
#[derive(Debug, Clone, Copy)]
struct Spaced;

impl Envelope for Spaced {
    type Head = ();

    const NAME: &'static str = "spaced";

    const CARRIES_HEADERS: bool = false;

    fn open(&self, _pattern: Pattern, frames: VecDeque<Bytes>) -> Result<Opened<()>, ZmqError> {
        let frame = frames.into_iter().next().unwrap_or_default();
        let Some(at) = frame.iter().position(|&byte| byte == b' ') else {
            // A frame without a space is a peer's payload, with no name.
            return Ok(Opened::new("", (), frame));
        };
        let name = std::str::from_utf8(&frame[..at])
            .map_err(|_| ZmqError::Wire("the name must be UTF-8".into()))?;
        Ok(Opened::new(name, (), frame.slice(at + 1..)))
    }

    fn headers(_head: &()) -> Result<HeaderMap, ZmqError> {
        Ok(HeaderMap::new())
    }

    fn seal(
        &self,
        _pattern: Pattern,
        name: &str,
        headers: &HeaderMap,
        payload: Bytes,
        frames: &mut Vec<Bytes>,
    ) -> Result<(), SealError> {
        if let Some((header, _)) = headers.iter().next() {
            return Err(SealError::new(format!(
                "header {header:?} has no place in the frame"
            )));
        }
        frames.push([name.as_bytes(), b" ", &payload].concat().into());
        Ok(())
    }
}
// --8<-- [end:envelope]

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Tick {
    symbol: String,
    price: u64,
}

// `#[outgoing(name = "ticks")]` by hand.
impl OutgoingDestination for Tick {
    type Form = FixedName;
    const DESTINATION: &'static str = "ticks";
}

impl MessageHeaders for Tick {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Price {
    symbol: String,
    price: u64,
}

// `#[outgoing(name = "prices")]` by hand.
impl OutgoingDestination for Price {
    type Form = FixedName;
    const DESTINATION: &'static str = "prices";
}

impl MessageHeaders for Price {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Audited {
    order: u64,
}

// `#[outgoing(name = "audit")]` by hand.
impl OutgoingDestination for Audited {
    type Form = FixedName;
    const DESTINATION: &'static str = "audit";
}

impl MessageHeaders for Audited {
    type Contract = NoHeaders;
}

struct Relay;

impl Handle<Tick, Price> for Relay {
    fn handle(
        &self,
        tick: &Tick,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Price, HandlerOutcome>> {
        ready(Ok(Price {
            symbol: tick.symbol.clone(),
            price: tick.price,
        }))
    }
}

struct Audit;

impl Handle<Order, Audited> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Audited, HandlerOutcome>> {
        ready(Ok(Audited { order: order.id }))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    // The feed's PUB socket dials in and writes its own layout, read through the envelope above.
    let feed = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5556")).envelope(Spaced);
    // Every price leaves as one JSON document for subscribers in other languages.
    let prices = ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557"))
        .envelope(Framed::new(JsonCodec))
        .bindable();
    let to_prices = prices.bind(ZmqFanoutPublish);
    // The sink reads bare payloads: this policy writes them, whatever its broker reads.
    let sink = ZmqQueue::new(ZmqEndpoint::connect("tcp://sink:5558")).bindable();
    let to_sink = sink.bind(ZmqQueuePublish.envelope(NoOp));
    RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .with_broker(feed, |b| {
            b.include(subscriber("ticks", Relay).reply().build())
                .out_reply(to_prices);
        })
        .with_broker(prices, |_b| {})
        // The orders arrive in the default three frames.
        .with_broker(
            ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
            |b| {
                b.include(subscriber("orders", Audit).reply().build())
                    .out_reply(to_sink);
            },
        )
        .with_broker(sink, |_b| {})
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::codec::JsonCodec;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::envelope::Framed;
    use ruststream_zeromq::prelude::*;
    use ruststream_zeromq::{Bind, Connect};

    use super::{Audited, Order, Price, Spaced, Tick, app};

    // The harness runs a broker in process by its type, so a broker on an envelope of the
    // service's own is registered once.
    ruststream::register_testable_broker!(ZmqFanout<Bind, Spaced>);

    #[tokio::test]
    async fn a_tick_from_the_feed_leaves_as_a_document() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqFanout<Bind, Spaced>>()
            .message(&Tick {
                symbol: "ACME".to_owned(),
                price: 42,
            })
            .publish()
            .await?;

        tb.broker::<ZmqFanout<Bind, Framed<JsonCodec>>>()
            .published::<Price>("prices")
            .assert_called_once()
            .with(&Price {
                symbol: "ACME".to_owned(),
                price: 42,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_audit_record_leaves_as_a_bare_payload() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<ZmqQueue>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<ZmqQueue<Connect>>()
            .published::<Audited>("audit")
            .assert_called_once()
            .with(&Audited { order: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
