//! The sockets and tasks a broker opens run on the runtime it connected on, whichever runtime
//! opens a subscription or attaches a publisher.
//!
//! A handler on a dedicated thread publishes from that thread's current-thread runtime. A socket
//! attached there would register its connection with that runtime and start its accept loop and
//! peer tasks on it, so it would stop working once that runtime stops, while the broker it
//! belongs to lives on. Each check below opens a subscription or attaches a publisher from a
//! runtime of its own, stops that runtime, and proves the socket still carries messages.

use std::pin::pin;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, RequestReply, Subscribe,
    Subscriber,
};
use ruststream_zeromq::{ZmqEndpoint, ZmqFanout, ZmqQueue, ZmqRpc};
use tokio::runtime::Builder;
use tokio::sync::oneshot;

/// A guard against a hang, never a synchronization point: every wait below ends on an event.
const GUARD: Duration = Duration::from_secs(10);

/// How long one fan-out publish waits for its delivery before the next one: the PUB socket drops
/// what it sends before the subscriber's filter reached it.
const SLOW_JOINER: Duration = Duration::from_millis(200);

const LOOPBACK: &str = "tcp://127.0.0.1:0";

/// Runs `work` on a current-thread runtime of its own, on a thread of its own, and returns its
/// output once that runtime has stopped.
async fn on_foreign_runtime<Work, Output>(work: Work) -> Output
where
    Work: AsyncFnOnce() -> Output + Send + 'static,
    Output: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the foreign runtime builds");
        let output = runtime.block_on(work());
        drop(runtime);
        let _ = tx.send(output);
    });
    rx.await.expect("the foreign runtime's work completes")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_subscription_opened_on_another_runtime_keeps_delivering() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the queue connects");
    let (connected, mut subscriber) = on_foreign_runtime(async move || {
        let subscriber = connected
            .subscribe("jobs")
            .await
            .expect("the subscription opens");
        (connected, subscriber)
    })
    .await;

    connected
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"job".as_slice()), None)
        .await
        .expect("the publish leaves");
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(GUARD, stream.next())
        .await
        .expect("the delivery arrives")
        .expect("the stream is open")
        .expect("the delivery is ok");
    assert_eq!(message.payload(), b"job");

    connected.shutdown().await.expect("the queue shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_publisher_attached_on_another_runtime_keeps_sending() {
    let connected = ZmqQueue::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the queue connects");
    let mut subscriber = connected
        .subscribe("jobs")
        .await
        .expect("the subscription opens");
    let publisher = connected.publisher();
    let publisher = on_foreign_runtime(async move || {
        publisher
            .publish(OutgoingMessage::new("jobs", b"first".as_slice()), None)
            .await
            .expect("the first publish leaves");
        publisher
    })
    .await;

    let mut stream = pin!(subscriber.stream());
    for expected in [b"first".as_slice(), b"second".as_slice()] {
        if expected == b"second" {
            publisher
                .publish(OutgoingMessage::new("jobs", expected), None)
                .await
                .expect("a publish after that runtime stopped leaves");
        }
        let message = tokio::time::timeout(GUARD, stream.next())
            .await
            .expect("the delivery arrives")
            .expect("the stream is open")
            .expect("the delivery is ok");
        assert_eq!(message.payload(), expected);
    }

    connected.shutdown().await.expect("the queue shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fanout_subscription_opened_on_another_runtime_keeps_delivering() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the fan-out connects");
    let (connected, mut subscriber) = on_foreign_runtime(async move || {
        let subscriber = connected
            .subscribe("events")
            .await
            .expect("the subscription opens");
        (connected, subscriber)
    })
    .await;

    let publisher = connected.publisher();
    let mut stream = pin!(subscriber.stream());
    let mut delivered = None;
    for _ in 0..50 {
        publisher
            .publish(OutgoingMessage::new("events", b"event".as_slice()), None)
            .await
            .expect("the publish leaves");
        if let Ok(next) = tokio::time::timeout(SLOW_JOINER, stream.next()).await {
            delivered = next;
            break;
        }
    }
    let message = delivered
        .expect("a delivery arrives")
        .expect("the delivery is ok");
    assert_eq!(message.payload(), b"event");

    connected.shutdown().await.expect("the fan-out shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fanout_publisher_attached_on_another_runtime_keeps_sending() {
    let connected = ZmqFanout::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the fan-out connects");
    let mut subscriber = connected
        .subscribe("events")
        .await
        .expect("the subscription opens");

    // The first delivery proves the socket attached and the subscriber's filter reached it
    // before that runtime stops.
    let publisher = connected.publisher();
    let (publisher, subscriber) = on_foreign_runtime(async move || {
        let mut delivered = false;
        {
            let mut stream = pin!(subscriber.stream());
            for _ in 0..50 {
                publisher
                    .publish(OutgoingMessage::new("events", b"first".as_slice()), None)
                    .await
                    .expect("the publish leaves");
                if let Ok(Some(Ok(_))) = tokio::time::timeout(SLOW_JOINER, stream.next()).await {
                    delivered = true;
                    break;
                }
            }
        }
        assert!(delivered, "the first delivery arrives");
        (publisher, subscriber)
    })
    .await;
    let mut subscriber = subscriber;

    // Anything the first loop sent after its delivery is drained by the payload check.
    publisher
        .publish(OutgoingMessage::new("events", b"second".as_slice()), None)
        .await
        .expect("a publish after that runtime stopped leaves");
    let mut stream = pin!(subscriber.stream());
    let second = tokio::time::timeout(GUARD, async {
        while let Some(next) = stream.next().await {
            let message = next.expect("the delivery is ok");
            if message.payload() == b"second" {
                return Some(message);
            }
        }
        None
    })
    .await
    .expect("the publish after that runtime stopped arrives");
    assert!(second.is_some(), "the stream stays open");

    connected.shutdown().await.expect("the fan-out shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_rpc_responder_opened_on_another_runtime_keeps_answering() {
    let connected = ZmqRpc::new(ZmqEndpoint::bind(LOOPBACK))
        .connect()
        .await
        .expect("the rpc connects");
    let (connected, mut responder) = on_foreign_runtime(async move || {
        let responder = connected
            .subscribe("greeter")
            .await
            .expect("the responder opens");
        (connected, responder)
    })
    .await;

    let publisher = connected.publisher();
    let respond = async {
        let mut stream = pin!(responder.stream());
        let request = stream
            .next()
            .await
            .expect("the stream is open")
            .expect("the request is ok");
        let reply_to = request
            .headers()
            .reply_to()
            .expect("a request carries its reply address")
            .to_owned();
        let headers = request.headers().clone();
        publisher
            .publish(
                OutgoingMessage::new(&reply_to, b"pong".as_slice()).with_headers(headers),
                None,
            )
            .await
            .expect("the reply leaves");
    };
    let requester = connected.publisher();
    let request = requester.request(OutgoingMessage::new("greeter", b"ping".as_slice()), GUARD);
    let ((), reply) = tokio::time::timeout(GUARD, async { tokio::join!(respond, request) })
        .await
        .expect("the exchange completes");
    assert_eq!(reply.expect("the request is answered").payload(), b"pong");

    connected.shutdown().await.expect("the rpc shuts down");
}
