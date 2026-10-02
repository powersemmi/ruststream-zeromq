// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a transport fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `zeromq` client it wraps, and what the runtime costs on top.
//!
//! Every scenario runs three times over, as three loops that differ in one thing each: what
//! carries the messages.
//!
//! - **raw** - the `zeromq` sockets, driven directly.
//! - **adapter** - this crate's own consumer and publisher: [`ZmqQueue`], the subscription it
//!   opens, the [`Subscriber`] stream it yields, its [`IncomingMessage`] and its `ack`, its
//!   [`Publisher`]. A loop in the benchmark pulls, decodes, reads a field and settles. No handler,
//!   no app, no dispatch.
//! - **framework** - the whole service a user writes: `#[subscriber]`, the app, the runtime, over
//!   the same crate.
//!
//! Two differences come out of that. Adapter against raw is what this crate's consumer and
//! publisher cost over the client they wrap, which is the question this repository answers for.
//! Framework against adapter is what the runtime costs on top, over this transport in particular:
//! the runtime belongs to the core, but how it meets a transport does not, and a share that
//! differs from one transport to another is a finding about this crate.
//!
//! Everything else is identical across all three: the same socket pattern, the same socket
//! options, the same three frames on the wire, the same decode into the same type, the same
//! payload bytes, the same tokio runtime and the same binary. The procedure the numbers follow is
//! the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # There is no server here
//!
//! `ZeroMQ` has no broker: the two peers talk to each other. So the other side of every loop is a
//! socket in this process, there is nothing to start or stop, and the setting that decides the
//! result is the transport rather than a server's configuration. Both scenarios are therefore the
//! same pattern over the two transports this crate admits, `tcp://` on the loopback and `ipc://`.
//!
//! # What a run is
//!
//! The consumer side binds first, the publisher then feeds it, and the window runs from the first
//! delivery to the end of the last one. Binding, dialling and the ZMTP handshake are startup cost
//! and sit outside it. Every run gets an address of its own - a free port on the loopback, a fresh
//! path in the temporary directory - so a run never meets what the one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! Rounds are interleaved - raw, adapter, framework, and again - and each loop reports its best,
//! median and worst round. The best is the headline: noise only ever slows a run down, so the
//! fastest round is the closest to the undisturbed cost. The distance between the best and the
//! worst is the noise a difference has to clear. Running one loop to the end and then the next
//! would charge every drift of the machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! Nothing settles a delivery on this transport. The adapter and framework loops still settle,
//! because that is where a settlement would go and the answer - unsupported - is one a caller
//! waits for; the raw loop has nothing to call. The bodies carry no headers either: the header
//! frame goes out empty, so what this crate parses per delivery is the frame layout and the
//! payload, and a service that does send headers pays its header parse on top of the figures
//! below.
//!
//! The broker-bound mark - transport-bound here - says the consumer spent the run waiting on the
//! transport, so the work above it happened inside a wait that was already being paid.
//! [`ROUND_TRIPS_PER_DELIVERY`] decides it, against a round trip measured on the same transport
//! outside every loop, and on this pattern it can only ever answer no: see that constant for why.

use std::convert::Infallible;
use std::env;
use std::fmt::Debug;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use ruststream::{AckError, ConnectedBroker, OutgoingMessage, Subscribe, Subscriber};
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use zeromq::prelude::*;
use zeromq::{
    PullSocket, PushSocket, RepSocket, ReqSocket, ZmqError as WireError, ZmqMessage as WireMessage,
};

// A benchmark measures what ships. The framework's harness feature swaps this crate's broker for
// an in-process stand-in and records what every handler saw, so a number taken with it on is not
// the transport and not the dispatch path either. The benchmark lives in a package of its own for
// the same reason: `ruststream-zeromq`'s dev-dependencies enable that feature through the
// conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw loop's rate.
const PROBE_MESSAGES: usize = 100_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 20_000_000;
/// Rounds run. Each loop reports its best, median and worst round.
const ROUNDS: usize = 3;
/// Worker threads every loop is driven on.
const WORKERS: usize = 4;

/// How far the publisher may run ahead of the consumer, in messages.
///
/// Every loop is held to the same ceiling, whatever else bounds it: the socket buffers on a raw
/// loop, those and the subscription's read-ahead on this crate's. 32768 bodies is about 18 MiB
/// outstanding, and far more than any loop is ever behind when it is keeping up.
const IN_FLIGHT: usize = 32_768;
/// How often the publisher checks that ceiling.
const CHECK_EVERY: usize = 512;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);
/// How long a raw send retries while the ZMTP handshake settles.
///
/// The implementation hands a message straight back while no peer is attached, which is routine
/// for the first sends after a dial. This crate's publisher retries on the same terms, so every
/// loop pays the same check.
const SEND_RETRY_STEP: Duration = Duration::from_millis(1);

/// Exchanges the round-trip probe takes on one connection.
const ROUND_TRIPS: usize = 20_000;

/// How many round trips one delivery costs the consumer on this pattern.
///
/// None. PUSH/PULL is one-way: the consumer reads frames off the stream and sends nothing back -
/// no poll, no fetch, no acknowledgement - and the implementation has no credit window either. So
/// a delivery never waits for an answer, and the arithmetic in [`measure`] can only ever report
/// the row as not transport-bound. The probe is taken and published anyway, so a reader can check
/// that rather than take it on trust.
const ROUND_TRIPS_PER_DELIVERY: f64 = 0.0;

/// The name frame every body carries, and the name every consumer subscribes under.
///
/// PUSH/PULL does not filter on it - the address is the queue - so it is a label every loop writes
/// identically. It has to match the literal in [`consume`]'s clause, which the macro needs spelled
/// out.
const NAME: &str = "bench";

/// The body size every loop publishes and decodes, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What every loop decodes a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The three frames of this crate's documented wire layout: name, headers, payload.
///
/// This is what the crate's own publisher writes for a message carrying no headers, spelled out
/// here so the raw loop puts exactly the same bytes on the wire.
fn frames(body: &Bytes) -> WireMessage {
    let mut message = WireMessage::from(NAME);
    message.push_back(Bytes::new());
    message.push_back(body.clone());
    message
}

/// The address one run owns, and the socket file it has to take away with it.
///
/// `ipc://` leaves a node in the filesystem behind; a run that left it there would hand the next
/// run an address that is already taken.
#[derive(Debug)]
struct Address {
    address: String,
    path: Option<PathBuf>,
}

impl Drop for Address {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Address {
    /// A free port on the loopback.
    ///
    /// The probe socket is closed before the address is handed out, so this is a claim on a port
    /// the kernel has just said is free rather than a reservation. On a machine given over to a
    /// benchmark run nothing else is competing for it, and a bind that loses the race fails
    /// loudly rather than measuring something else.
    fn tcp() -> Self {
        let probe = TcpListener::bind("127.0.0.1:0").expect("the loopback has a free port");
        let port = probe
            .local_addr()
            .expect("a bound listener has an address")
            .port();
        drop(probe);
        Self {
            address: format!("tcp://127.0.0.1:{port}"),
            path: None,
        }
    }

    /// A fresh socket path in the temporary directory.
    fn ipc() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "ruststream-zmq-bench-{}-{}.sock",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        Self {
            address: format!("ipc://{}", path.display()),
            path: Some(path),
        }
    }

    fn as_str(&self) -> &str {
        &self.address
    }
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Every loop calls the same methods, so every loop pays for the signal. A delivery pays one
/// relaxed increment and two comparisons; the waiter is a single future for the whole run, woken
/// once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once the last delivery's instant has been recorded.
    ///
    /// The wait is on that instant rather than on the counter, because the counter is bumped
    /// first and with `Relaxed`: a waiter can see the count reach its total while the instant
    /// behind it is not yet stored, and would then read a window that does not exist. The loops
    /// that join their consuming task never saw this; the one that has no task to join did.
    async fn drained(&self) {
        while self.0.last.get().is_none() {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last one.
    fn window(&self, loop_name: &str) -> Duration {
        let first = *self
            .0
            .first
            .get()
            .unwrap_or_else(|| panic!("{loop_name}: the run took a delivery"));
        let last = *self
            .0
            .last
            .get()
            .unwrap_or_else(|| panic!("{loop_name}: the run took its last delivery"));
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, loop_name: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{loop_name}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// Deliveries a second, from the window one loop measured.
fn rate(window: Duration, messages: usize) -> f64 {
    messages as f64 / window.as_secs_f64()
}

/// Holds the publisher until the consumer is within [`IN_FLIGHT`] of it.
///
/// The gate is the same in every loop, so none is held back more than another.
async fn wait_for_room(sent: usize, run: &Run) {
    while sent.saturating_sub(run.handled()) > IN_FLIGHT {
        sleep(Duration::from_micros(200)).await;
    }
}

/// Publishes the run's bodies through this crate's publisher.
///
/// The adapter and framework loops share it, so what separates them is the consumer side alone.
async fn feed<P>(publisher: &P, messages: usize, run: &Run)
where
    P: Publisher,
    P::Error: Debug,
{
    let body = Bytes::from(json_body(BODY_BYTES));
    for sent in 0..messages {
        if sent % CHECK_EVERY == 0 {
            wait_for_room(sent, run).await;
        }
        publisher
            .publish(OutgoingMessage::new(NAME, &body), None)
            .await
            .expect("the publisher accepts the message");
    }
}

/// Sends one raw message, retrying while the handshake settles.
///
/// `ReturnToSender` hands the message back rather than dropping it, so retrying loses nothing.
async fn send(socket: &mut PushSocket, message: WireMessage) {
    let mut pending = message;
    loop {
        match socket.send(pending).await {
            Ok(()) => return,
            Err(WireError::ReturnToSender { message, .. }) => {
                pending = message;
                sleep(SEND_RETRY_STEP).await;
            }
            Err(err) => panic!("the socket accepts the message: {err}"),
        }
    }
}

/// What one round trip costs between two peers on this transport.
///
/// PUSH/PULL has no answer to wait for, so the probe is the plainest pair that does: a REQ socket
/// waiting for a REP socket's reply, one exchange in flight at a time, on an address of its own.
/// It measures the transport rather than this crate, which is what the mark above is about, and it
/// runs outside every loop so none of them pays for it.
async fn round_trip(address: &Address) -> Duration {
    let mut server = RepSocket::new();
    server
        .bind(address.as_str())
        .await
        .expect("the REP socket binds");
    let serving = tokio::spawn(async move {
        while let Ok(message) = server.recv().await {
            if server.send(message).await.is_err() {
                break;
            }
        }
    });

    let mut client = ReqSocket::new();
    client
        .connect(address.as_str())
        .await
        .expect("the REQ socket reaches the REP socket");
    let body = WireMessage::from(Bytes::from_static(b"ping"));

    // One exchange outside the measured region, so the ZMTP handshake is not in the average.
    client.send(body.clone()).await.expect("the probe sends");
    client.recv().await.expect("the probe is answered");

    let started = Instant::now();
    for _ in 0..ROUND_TRIPS {
        client.send(body.clone()).await.expect("the probe sends");
        client.recv().await.expect("the probe is answered");
    }
    let elapsed = started.elapsed();
    serving.abort();
    elapsed / u32::try_from(ROUND_TRIPS).expect("the exchange count fits a u32")
}

// ---------------------------------------------------------------------------------------------
// raw: the client this crate wraps, driven directly
// ---------------------------------------------------------------------------------------------

async fn raw(address: &Address, messages: usize) -> Duration {
    let mut consumer = PullSocket::new();
    consumer
        .bind(address.as_str())
        .await
        .expect("the PULL socket binds");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            while let Ok(message) = consumer.recv().await {
                // Frame 2 of the documented layout. A peer that speaks this wire by hand reads it
                // the same way, which is what makes this the comparison and not a shortcut.
                let payload = message.get(2).expect("a message carries its three frames");
                let order: Order = serde_json::from_slice(payload).expect("the body decodes");
                black_box((order.id, order.quantity));
                // Nothing to settle: the transport performs no acknowledgement, so a loop written
                // by hand has nothing to call here.
                if run.arrived() {
                    break;
                }
            }
        }
    });

    let mut publisher = PushSocket::new();
    publisher
        .connect(address.as_str())
        .await
        .expect("the PUSH socket reaches the consumer");
    let body = Bytes::from(json_body(BODY_BYTES));
    for sent in 0..messages {
        if sent % CHECK_EVERY == 0 {
            wait_for_room(sent, &run).await;
        }
        send(&mut publisher, frames(&body)).await;
    }

    drain(&run, "raw").await;
    consuming.await.expect("the consuming task ends");
    run.window("raw")
}

// ---------------------------------------------------------------------------------------------
// adapter: this crate's own consumer and publisher, hand-driven
// ---------------------------------------------------------------------------------------------

async fn adapter(address: &Address, messages: usize) -> Duration {
    let connected = ZmqQueue::new(ZmqEndpoint::bind(address.as_str()))
        .connect()
        .await
        .expect("the broker connects");
    let subscriber = connected
        .subscribe(NAME)
        .await
        .expect("the subscription opens");
    let publisher = Publish
        .pair(&connected)
        .await
        .expect("the publish policy pairs with the connected broker");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            let mut stream = pin!(subscriber.stream());
            while let Some(delivery) = stream.next().await {
                let message = delivery.expect("the subscription yields a delivery");
                let order: Order =
                    serde_json::from_slice(message.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                // Where a settlement would go. This transport performs none and says so, which is
                // still an answer the caller has to wait for.
                match message.ack().await {
                    Ok(()) | Err(AckError::Unsupported) => {}
                    Err(err) => panic!("the delivery settles or reports why not: {err}"),
                }
                if done {
                    break;
                }
            }
        }
    });

    feed(&publisher, messages, &run).await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    let window = run.window("adapter");
    connected.shutdown().await.expect("the broker shuts down");
    window
}

// ---------------------------------------------------------------------------------------------
// framework: the service a user writes
// ---------------------------------------------------------------------------------------------

#[subscriber("bench")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn framework(address: &Address, messages: usize) -> Duration {
    let broker = ZmqQueue::new(ZmqEndpoint::bind(address.as_str()));
    // The same live publisher the adapter loop gets from the policy: `pair` hands out exactly
    // this. It has to be taken before the app takes the broker, and it resolves once the runtime
    // has connected.
    let publisher = broker.publisher();

    let run = Run::new(messages);
    let state = run.clone();
    let app = RustStream::new(AppInfo::new("zeromq-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state.clone()))
        .with_broker(broker, |b| {
            b.include(consume);
        })
        .start()
        .await
        .expect("the service starts");

    feed(&publisher, messages, &run).await;
    drain(&run, "framework").await;
    let window = run.window("framework");
    app.shutdown().await.expect("the service stops");
    window
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Tcp,
    Ipc,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Tcp => "PUSH/PULL over tcp:// on the loopback, 512 B JSON",
            Self::Ipc => "PUSH/PULL over ipc://, 512 B JSON",
        }
    }

    fn transport(self) -> &'static str {
        match self {
            Self::Tcp => "tcp://",
            Self::Ipc => "ipc://",
        }
    }

    fn address(self) -> Address {
        match self {
            Self::Tcp => Address::tcp(),
            Self::Ipc => Address::ipc(),
        }
    }
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

/// The procedure's honesty rule, applied to one comparison: a difference smaller than the
/// run-to-run spread of either side is a verdict, never a percentage.
fn verdict(raw: Stats, other: Stats) -> &'static str {
    if (raw.best - other.best).abs() < raw.spread().max(other.spread()) {
        "indistinguishable"
    } else {
        "measured"
    }
}

fn percent(raw: Stats, other: Stats) -> f64 {
    (raw.best - other.best) / raw.best * 100.0
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    broker_bound: bool,
    round_trip: Duration,
}

async fn measure(scenario: Scenario, rounds: usize, seconds: f64) -> Measured {
    let round_trip = round_trip(&scenario.address()).await;
    println!(
        "{}: one round trip on this transport takes {:.1} us",
        scenario.name(),
        round_trip.as_secs_f64() * 1e6
    );

    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probed = rate(
        raw(&scenario.address(), PROBE_MESSAGES).await,
        PROBE_MESSAGES,
    );
    let messages = ((probed * seconds * MARGIN) as usize).clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({probed:.0} msg/s probed)",
        scenario.name(),
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    for round in 1..=rounds {
        let one = raw(&scenario.address(), messages).await;
        let two = adapter(&scenario.address(), messages).await;
        let three = framework(&scenario.address(), messages).await;
        println!(
            "  round {round:>2}: raw {:>10.0}, adapter {:>10.0}, framework {:>10.0} msg/s",
            rate(one, messages),
            rate(two, messages),
            rate(three, messages)
        );
        raws.push(rate(one, messages));
        adapters.push(rate(two, messages));
        frameworks.push(rate(three, messages));
    }

    let raw = Stats::of(&raws);
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter: Stats::of(&adapters),
        framework: Stats::of(&frameworks),
        // The measured form of the mark: what a delivery spends waiting for the transport, against
        // what a delivery costs in total. Half is the line the framework's procedure draws. On
        // this pattern the left side is zero by construction; the arithmetic is written out so the
        // row states a measurement rather than an assumption.
        broker_bound: ROUND_TRIPS_PER_DELIVERY * round_trip.as_secs_f64() >= 0.5 / raw.best,
        round_trip,
    }
}

fn document(measured: &[Measured]) -> String {
    let probes: Vec<String> = measured
        .iter()
        .map(|row| {
            format!(
                "{} {:.1} us",
                row.scenario.transport(),
                row.round_trip.as_secs_f64() * 1e6
            )
        })
        .collect();
    let mut out = format!(
        concat!(
            "{{\n",
            "  \"round_trip\": \"{probes} ({rounds} REQ/REP exchanges on the same transport,",
            " outside every loop)\",\n",
            "  \"scenarios\": [\n",
        ),
        probes = probes.join(", "),
        rounds = ROUND_TRIPS,
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            adapter_overhead = percent(row.raw, row.adapter),
            adapter_verdict = verdict(row.raw, row.adapter),
            overhead = percent(row.raw, row.framework),
            verdict = verdict(row.raw, row.framework),
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a round count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let rounds = number("RUSTSTREAM_BENCH_PAIRS", ROUNDS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    let measured: Vec<Measured> = [Scenario::Tcp, Scenario::Ipc]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, rounds, seconds)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:.1}%, {}), framework {:.0} ({:.1}%, {}) msg/s{}",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            percent(row.raw, row.adapter),
            verdict(row.raw, row.adapter),
            row.framework.best,
            percent(row.raw, row.framework),
            verdict(row.raw, row.framework),
            if row.broker_bound {
                ", transport-bound"
            } else {
                ""
            },
        );
    }

    std::fs::write(&out, document(&measured)).expect("the summary is written");
    println!("\nwrote {out}");
}
