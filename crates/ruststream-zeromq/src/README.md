`ZeroMQ` broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, over the pure-Rust [`zeromq`](https://docs.rs/zeromq) client on `tcp://` and `ipc://`.

Two processes talk directly, with a documented frame layout between them, so the peer on the
other side may be written in any language. A service picks one broker per socket pattern:
[`ZmqQueue`] (PUSH/PULL), [`ZmqFanout`] (PUB/SUB) or [`ZmqRpc`] (DEALER/ROUTER), each on a
[`ZmqEndpoint`] that says whether this process binds or connects. A subscription is a name, and
the name is frame 0 of every message. Delivery is at most once:
[`ack`](ruststream::IncomingMessage::ack) reports
[`AckError::Unsupported`](ruststream::AckError::Unsupported), and a retry is a copy the service
publishes.

# A service

A worker on the PUSH/PULL queue, with the pattern's own prelude as its one import:

```
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

#[subscriber("jobs")]
async fn handle(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "working on the job");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
        |b| {
            b.include(handle);
        },
    )
}
```

`cargo run -- run` starts it, and any peer that sends the three frames pushes work into it.
[`ZmqQueue::new`] only records the endpoint; the runtime connects the broker at startup and shuts
it down when the service stops.

# Where things are

The [guides][guides] take a `ZeroMQ` service from a first worker to production, in this order:
[getting started][g-start], [pipelines with PUSH/PULL][g-pipelines],
[broadcast with PUB/SUB][g-broadcast], [request and reply][g-request-reply],
[retries and dead letters][g-retries], [peers in other languages][g-interop],
[testing][g-testing] and [running in production][g-production]. The
[architecture][architecture] pages show how the sockets work and where the crate sits between
them and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [Broker][c-broker]: the three brokers, each connected into its own form
  ([`ConnectedZmqQueue`], [`ConnectedZmqFanout`], [`ConnectedZmqRpc`]).
- [ZmqQueue][c-queue]: [`ZmqQueue`], PUSH/PULL, competing consumers; module [`queue`].
- [ZmqFanout][c-fanout]: [`ZmqFanout`], PUB/SUB, broadcast filtered by name prefix; module
  [`fanout`].
- [ZmqRpc][c-rpc]: [`ZmqRpc`], DEALER/ROUTER, request and reply; module [`rpc`].
- [ZmqEndpoint][c-endpoint]: [`ZmqEndpoint`], an address on `tcp://` or `ipc://` and the side
  this process takes.
- [bind][c-bind]: [`ZmqEndpoint::bind`], one subscription per bound endpoint, and port zero
  resolved by [`ConnectedZmqQueue::bound_address`] and its twins.
- [connect][c-connect]: [`ZmqEndpoint::connect`], subscriptions that dial a sending peer.
- [EndpointRole][c-role]: [`EndpointRole`], the side as a type, [`Bind`] or [`Connect`].
- [Startup][c-startup]: the endpoint check at connect, and a socket per subscription as it opens.
- [Shutdown][c-shutdown]: [`ZmqError::NotConnected`] for a publisher used afterwards.
- [Prelude][c-prelude]: [`queue::prelude`], [`fanout::prelude`] and [`rpc::prelude`] for a routes
  file on one pattern, [`prelude`] for one that mounts several.
- [Errors][c-errors]: [`ZmqError`], the one error type of the crate.

## Subscriptions

- [Subscriptions][c-subscriptions]: a name in `#[subscriber("jobs")]`, delivered as a
  [`ZmqMessage`].
- [Prefixes][c-prefixes]: the name as a prefix filter on [`ZmqFanout`].
- [Batches][c-batches]: slice handlers over [`ZmqSubscriber`] batches, assembled on the client;
  [`ZmqRpcSubscriber`] takes none.
- [read_ahead][c-read-ahead]: [`ZmqQueue::read_ahead`] and its twins, how far a subscription
  reads ahead of its handler.

## Acknowledgement

- [Acknowledgement][c-settlement]: what `ack`, `nack` and each handler outcome do on a transport
  that settles nothing.
- [Retries][c-retries]: `retry_after` copies and the `max_attempts` cap.
- [out_retry][c-out-retry]: where the copies go, by the pattern and the side.
- [Dead letters][c-dead-letters]: `dead_letter`, a publisher on another endpoint.

## Publishing

- [Publishing][c-publishing]: [`ZmqQueuePublish`], [`ZmqFanoutPublish`] and [`ZmqRpcPublish`],
  and their live [`ZmqQueuePublisher`], [`ZmqFanoutPublisher`] and [`ZmqRpcPublisher`].
- [Replies][c-replies]: a queue worker's replies through a queue on an endpoint of its own.
- [Cancellation][c-cancellation]: which publishes are cancel-safe.

## Request-reply

- [Request-reply][c-request-reply]: [`RequestReply`](ruststream::RequestReply) on
  [`ZmqRpcPublisher`], a DEALER per request matched by `correlation-id`.
- [Responders][c-responders]: an answer at the `reply-to` address each request carries.

## Wire format

- [Wire format][c-wire]: the three frames, name, headers and payload.
- [Headers][c-headers]: the `name: value` lines of the header frame.
- [Envelope][c-envelope]: the identity frame a ROUTER puts in front of a request and its answer.

## The generated document

- [AsyncAPI][c-asyncapi]: the `x-ruststream-zeromq` extension of the generated document (feature
  `asyncapi`).
- [Server][c-server]: the coordinate, the transport and the side of each endpoint.
- [Channels][c-channels]: the socket pair and the name frame of each channel.
- [Reply address][c-reply-address]: a responder's answers, addressed by `reply-to`.

## Testing

- [Testing][c-testing]: the in-process transport
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  each broker to (feature `testing`).
- [Live][c-live]: `TestApp::start_live`, the same test over loopback sockets.

# Cargo features

`default` is empty. `testing` adds the in-process mode `TestApp::start` connects the brokers
through, and a service enables it in its `[dev-dependencies]`. `asyncapi` contributes the server
and channel facts of the generated document. The codecs are the core's: a service enables `json`,
`msgpack` or `cbor` on `ruststream` itself.

[guides]: https://powersemmi.github.io/ruststream-zeromq/latest/
[g-start]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/getting-started/
[g-pipelines]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/pipelines/
[g-broadcast]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/broadcast/
[g-request-reply]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/request-reply/
[g-retries]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/retries/
[g-interop]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/interop/
[g-testing]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-zeromq/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-zeromq/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-zeromq/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/
[c-queue]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/zmq-queue/
[c-fanout]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/zmq-fanout/
[c-rpc]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/zmq-rpc/
[c-endpoint]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/endpoint/
[c-bind]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/endpoint/bind/
[c-connect]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/endpoint/connect/
[c-role]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/endpoint/endpoint-role/
[c-startup]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-zeromq/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-zeromq/latest/components/subscriptions/
[c-prefixes]: https://powersemmi.github.io/ruststream-zeromq/latest/components/subscriptions/prefixes/
[c-batches]: https://powersemmi.github.io/ruststream-zeromq/latest/components/subscriptions/batches/
[c-read-ahead]: https://powersemmi.github.io/ruststream-zeromq/latest/components/subscriptions/read-ahead/
[c-settlement]: https://powersemmi.github.io/ruststream-zeromq/latest/components/settlement/
[c-retries]: https://powersemmi.github.io/ruststream-zeromq/latest/components/settlement/retries/
[c-out-retry]: https://powersemmi.github.io/ruststream-zeromq/latest/components/settlement/out-retry/
[c-dead-letters]: https://powersemmi.github.io/ruststream-zeromq/latest/components/settlement/dead-letters/
[c-publishing]: https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/
[c-replies]: https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/replies/
[c-cancellation]: https://powersemmi.github.io/ruststream-zeromq/latest/components/publishing/cancellation/
[c-request-reply]: https://powersemmi.github.io/ruststream-zeromq/latest/components/request-reply/
[c-responders]: https://powersemmi.github.io/ruststream-zeromq/latest/components/request-reply/responders/
[c-wire]: https://powersemmi.github.io/ruststream-zeromq/latest/components/wire/
[c-headers]: https://powersemmi.github.io/ruststream-zeromq/latest/components/wire/headers/
[c-envelope]: https://powersemmi.github.io/ruststream-zeromq/latest/components/wire/envelope/
[c-asyncapi]: https://powersemmi.github.io/ruststream-zeromq/latest/components/asyncapi/
[c-server]: https://powersemmi.github.io/ruststream-zeromq/latest/components/asyncapi/server/
[c-channels]: https://powersemmi.github.io/ruststream-zeromq/latest/components/asyncapi/channels/
[c-reply-address]: https://powersemmi.github.io/ruststream-zeromq/latest/components/asyncapi/reply-address/
[c-testing]: https://powersemmi.github.io/ruststream-zeromq/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-zeromq/latest/components/testing/live/
