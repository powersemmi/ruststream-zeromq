//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the peer on the other end of the socket, the latch a handler counts deliveries
//! down on, and the measurement configuration. The method is the core's, described in its
//! `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, built on this crate's production broker
//! with the constructor a user writes, bound to a TCP port on the loopback, and started through
//! [`RustStream::start`]. The other end of the socket is a peer: a raw `zeromq` socket on a thread
//! and a tokio runtime of its own, writing the crate's documented three-frame layout the way a
//! foreign peer writes it by hand.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: starting the service, accepting the peer's connection, and
//! taking the first delivery.
//!
//! What a body measures is the start and the drain, in two regions. The start region ends once the
//! peer is connected. Between the two regions the peer writes every message, and the service's
//! runtime does not run: it runs only inside `block_on`, and nothing calls it there. So the
//! messages wait in the kernel's socket buffers, producing them is in neither region, and the
//! drain reads a queue that is already full. PUSH/PULL keeps what is written to a consumer that is
//! not reading; a PUB/SUB subscriber would lose it.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in, on the service's thread alone: callgrind keeps its collection state per thread, and
//! the peer's thread never enters the region. Everything the service's thread does inside the
//! region is counted: the framework's dispatch and codec, this crate's code, the `zeromq`
//! client's framing and socket handling, and tokio's share of driving them. [`measure`] is the
//! only frame that carries its name, because a toggle on a name that also appears inside closure
//! types switches collection off again one frame deeper. DHAT is pointed at the same frame; the
//! number read is `Total blocks`, allocations per run, and an allocation on the peer's thread has
//! no such frame on its stack.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_zeromq::{ZmqEndpoint, ZmqQueue, ZmqRpc};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;
use zeromq::prelude::*;
use zeromq::util::PeerIdentity;
use zeromq::{DealerSocket, PushSocket, SocketOptions, ZmqMessage as WireMessage};

// A benchmark measures what ships. The framework's harness feature compiles test hooks into the
// dispatch path, so a number taken with it on is not the service a user deploys.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The name every scenario delivers under. On these patterns it is the label in frame 0, not an
/// address: the address is the endpoint. A handler names it in its own `#[subscriber(..)]`
/// attribute, which takes a literal.
pub const INPUT: &str = "orders";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within seconds of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// How long the benchmark waits on the peer before it calls the run stuck. Valgrind slows both
/// threads about fifty times, so this is generous rather than tight.
const PEER_WAIT: Duration = Duration::from_secs(120);

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run
/// fails when the path allocates more than it does today. Both are floors the code is held to,
/// so a number that goes down is lowered here in the same change. The instruction limit is
/// relative: `just bench-code --save-baseline=main` records a baseline and
/// `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution, and the same instruction
/// count on every run.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// The three frames of this crate's documented wire layout, name, headers and payload, as a
/// foreign peer writes them for a message that carries no headers.
fn frames(body: &Bytes) -> WireMessage {
    let mut message = WireMessage::from(INPUT);
    message.push_back(Bytes::new());
    message.push_back(body.clone());
    message
}

/// A free TCP port on the loopback, as a `zeromq` endpoint.
///
/// The probe listener is closed before the address is handed out, so this is a claim on a port
/// the kernel has just said is free rather than a reservation; a bind that loses the race fails
/// loudly. TCP rather than `ipc://`: the kernel coalesces small writes on a TCP connection, so the
/// socket buffers hold every message of a run while the service is not reading, where a Unix
/// socket holds a few hundred.
fn loopback() -> String {
    let probe = TcpListener::bind("127.0.0.1:0").expect("the loopback has a free port");
    let port = probe
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    drop(probe);
    format!("tcp://127.0.0.1:{port}")
}

/// Which socket the peer holds: the counterpart of the service's pattern.
#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A PUSH socket feeding the service's PULL.
    Push,
    /// A DEALER socket issuing requests to the service's ROUTER and reading its answers.
    Dealer,
}

/// The peer's socket, on the peer's thread.
enum PeerSocket {
    Push(PushSocket),
    Dealer(DealerSocket),
}

impl PeerSocket {
    async fn connect(kind: Kind, address: &str) -> Self {
        match kind {
            Kind::Push => {
                let mut socket = PushSocket::new();
                socket.connect(address).await.expect("the peer connects");
                Self::Push(socket)
            }
            Kind::Dealer => {
                // A fixed identity, so the reply address the service derives from it is the same
                // on every run.
                let mut options = SocketOptions::default();
                options.peer_identity(
                    PeerIdentity::try_from(Bytes::from_static(b"bench-peer"))
                        .expect("a short identity is valid"),
                );
                let mut socket = DealerSocket::with_options(options);
                socket.connect(address).await.expect("the peer connects");
                Self::Dealer(socket)
            }
        }
    }

    async fn send(&mut self, message: WireMessage) {
        let sent = match self {
            Self::Push(socket) => socket.send(message).await,
            Self::Dealer(socket) => socket.send(message).await,
        };
        sent.expect("the connected service takes the message");
    }

    /// Reads `count` answers, when the peer is one that gets answers, and says how many came.
    async fn answers(&mut self, count: usize) -> usize {
        let Self::Dealer(socket) = self else {
            return 0;
        };
        for _ in 0..count {
            timeout(PEER_WAIT, socket.recv())
                .await
                .expect("the service answers every request")
                .expect("an answer arrives whole");
        }
        count
    }
}

/// The benchmark body's ends of the conversation with the peer's thread.
struct Peer {
    /// Tells the peer the service has bound its endpoint.
    bound: oneshot::Sender<()>,
    /// Resolves once the peer is connected.
    connected: oneshot::Receiver<()>,
    /// Tells the peer to write every message.
    fill: oneshot::Sender<()>,
    /// Receives once every message is written.
    filled: Receiver<()>,
    /// Resolves once the peer has read every answer, on a pattern that answers.
    answered: oneshot::Receiver<()>,
    /// Lets the peer close its socket. It holds the connection open until the drain is over, so
    /// the service never reads the end of the stream inside the region.
    release: oneshot::Sender<()>,
    /// The peer's thread, yielding the answers it read.
    thread: JoinHandle<usize>,
}

/// The peer's ends of the same conversation.
struct Conversation {
    bound: oneshot::Receiver<()>,
    connected: oneshot::Sender<()>,
    fill: oneshot::Receiver<()>,
    filled: SyncSender<()>,
    answered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Peer {
    /// Starts the peer's thread. It waits there until the service is bound.
    fn spawn(kind: Kind, address: String, messages: usize) -> Self {
        let (bound, bound_rx) = oneshot::channel();
        let (connected_tx, connected) = oneshot::channel();
        let (fill, fill_rx) = oneshot::channel();
        let (filled_tx, filled) = sync_channel(1);
        let (answered_tx, answered) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let talk = Conversation {
            bound: bound_rx,
            connected: connected_tx,
            fill: fill_rx,
            filled: filled_tx,
            answered: answered_tx,
            release: release_rx,
        };
        let thread =
            thread::spawn(move || runtime().block_on(converse(kind, &address, messages, talk)));
        Self {
            bound,
            connected,
            fill,
            filled,
            answered,
            release,
            thread,
        }
    }
}

/// What the peer does, on its own thread: connect once the service is bound, write every message
/// when told to, read the answers if the pattern gives any, and close once the drain is over.
async fn converse(kind: Kind, address: &str, messages: usize, talk: Conversation) -> usize {
    talk.bound
        .await
        .expect("the benchmark says when the service is bound");
    let mut socket = PeerSocket::connect(kind, address).await;
    let _ = talk.connected.send(());
    talk.fill.await.expect("the benchmark says when to write");
    let body = Bytes::from(json_body());
    for _ in 0..messages {
        socket.send(frames(&body)).await;
    }
    talk.filled
        .send(())
        .expect("the benchmark waits for the fill");
    let answers = socket.answers(messages).await;
    let _ = talk.answered.send(());
    let _ = talk.release.await;
    drop(socket);
    answers
}

/// A service that is built but not started, the peer it will talk to, and how many messages the
/// peer will write.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Start,
    peer: Peer,
    messages: usize,
    answers: bool,
}

/// Starts the service, tells the peer where it is bound, and waits for the peer to connect.
type Start = Box<dyn FnOnce(&Runtime, oneshot::Sender<()>, oneshot::Receiver<()>) -> RunningApp>;

/// The mount a PUSH/PULL scenario passes in: what `with_broker` does with the scope.
pub type QueueMount<'a> = &'a mut BrokerScope<ZmqQueue, Identity, (), Latch>;

/// The mount a DEALER/ROUTER scenario passes in.
pub type RpcMount<'a> = &'a mut BrokerScope<ZmqRpc, Identity, (), Latch>;

/// A one-handler PUSH/PULL service bound on the loopback, and a PUSH peer that feeds it.
pub fn queue(messages: usize, mount: impl FnOnce(QueueMount<'_>)) -> Pending {
    let address = loopback();
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(ZmqQueue::new(ZmqEndpoint::bind(address.clone())), mount);
    let peer = Peer::spawn(Kind::Push, address, messages);
    pending(app, latch, peer, messages, false)
}

/// A one-handler DEALER/ROUTER responder bound on the loopback, and a DEALER peer that sends it
/// requests and reads every answer.
pub fn rpc(messages: usize, mount: impl FnOnce(RpcMount<'_>)) -> Pending {
    let address = loopback();
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(ZmqRpc::new(ZmqEndpoint::bind(address.clone())), mount);
    let peer = Peer::spawn(Kind::Dealer, address, messages);
    pending(app, latch, peer, messages, true)
}

fn pending(
    app: RustStream<Identity, Latch>,
    latch: Latch,
    peer: Peer,
    messages: usize,
    answers: bool,
) -> Pending {
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime, bound, connected| {
            runtime.block_on(async move {
                let running = app.start().await.expect("the service starts");
                let _ = bound.send(());
                connected.await.expect("the peer connects");
                running
            })
        }),
        peer,
        messages,
        answers,
    }
}

/// Starts the service, lets the peer fill its socket, and drains it: the shape of every scenario
/// here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// up to the peer's connection; the second is the deliveries and, on a pattern that answers,
/// every answer reaching the peer.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        peer,
        messages,
        answers,
    } = pending;
    let running = measure(|| start(&runtime, peer.bound, peer.connected));
    latch.expect(messages);
    peer.fill
        .send(())
        .expect("the peer waits for the word to write");
    peer.filled
        .recv_timeout(PEER_WAIT)
        .expect("the socket buffers take every message while the service is not reading");
    assert_eq!(
        latch.remaining(),
        messages,
        "the service ran while the peer was writing, so the measured region would be short"
    );
    if answers {
        measure(|| runtime.block_on(peer.answered)).expect("the peer reads every answer");
    } else {
        measure(|| runtime.block_on(latch.drained()));
    }
    let _ = peer.release.send(());
    let read = peer.thread.join().expect("the peer's thread ends");
    if answers {
        assert_eq!(read, messages, "every request is answered");
    }
    black_box(read);
    drop(running);
}
