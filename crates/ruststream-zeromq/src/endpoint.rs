//! [`ZmqEndpoint`]: an address plus the side of it this process takes, as a type.
//!
//! There is no server in the middle, so which side listens is a deployment decision, not a
//! property of the transport; the endpoint states it. The side is a type parameter rather than a
//! field because it decides what a subscription can promise about a retry copy, and that is
//! settled where the mount site is compiled.

use std::marker::PhantomData;

use ruststream::ServerSpec;

#[cfg(feature = "asyncapi")]
use crate::bindings;
use crate::error::ZmqError;

/// The ZMTP version the underlying implementation greets a peer with.
///
/// The [`zeromq`](https://docs.rs/zeromq) crate sends `3.0` in every greeting and negotiates no
/// other, so this is a fact of the client rather than a configured value; the generated document
/// reports it as the server's protocol version.
pub(crate) const ZMTP_VERSION: &str = "3.0";

pub(crate) use sealed::Side;

mod sealed {
    /// The side of an endpoint, as the machinery the three patterns share reads it.
    ///
    /// It is the [`EndpointRole`](super::EndpointRole) a public type carries, read off that type
    /// once, when the endpoint is built: a socket attaches per side, and the attach code is
    /// shared by every pattern. Public inside this private module, so it can ride the sealed
    /// trait without being nameable outside the crate.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Side {
        Bind,
        Connect,
    }

    /// Keeps the sides at the two a socket can take, and carries the side the shared machinery
    /// reads.
    pub trait Sealed {
        const SIDE: Side;
    }
}

/// Which side of an endpoint this process takes: [`Bind`] or [`Connect`].
///
/// The endpoint constructors fix it - [`ZmqEndpoint::bind`] and [`ZmqEndpoint::connect`] - and
/// every broker built on the endpoint carries it as its `Role` parameter, so `ZmqQueue<Connect>`
/// is a queue this process dials. The side decides where a retry copy of a one-way subscription
/// can go: a subscription that binds takes a copy sent to its own listener, while one that dials
/// reads from a peer that only sends, so its mount site names where the copies go.
///
/// Sealed: the two sides are the whole set.
///
/// # Examples
///
/// A service that builds every queue with the same settings, on whichever side the endpoint
/// takes:
///
/// ```
/// use ruststream_zeromq::EndpointRole;
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", reply)]
/// async fn work(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// fn queue<Role: EndpointRole>(endpoint: ZmqEndpoint<Role>) -> ZmqQueue<Role> {
///     ZmqQueue::new(endpoint).read_ahead(nonzero!(64_usize))
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let results = queue(ZmqEndpoint::connect("tcp://sink:5556")).bindable();
///     let to_results = results.bind(Publish);
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(queue(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(work).out_reply(to_results);
///         })
///         .register_broker(results)
/// }
/// ```
pub trait EndpointRole: sealed::Sealed + Send + Sync + 'static {}

/// This process listens on the endpoint: the side [`ZmqEndpoint::bind`] takes.
///
/// # Examples
///
/// Routes for a queue this process binds retry on that queue and name no destination, because
/// the listener takes the copies:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_zeromq::Bind;
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn work(job: &Job) -> HandlerOutcome {
///     if job.id == 0 {
///         return HandlerOutcome::retry_after(Duration::from_secs(30));
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn routes() -> impl RouterDef<ZmqQueue<Bind>> {
///     Router::<ZmqQueue<Bind>>::new()
///         .include(work)
///         .max_attempts(nonzero!(5u32))
///         .out_retry(Publish)
///         .build()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
///         ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")),
///         |b| {
///             b.include_router(routes());
///         },
///     )
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Bind;

/// This process dials the endpoint: the side [`ZmqEndpoint::connect`] takes.
///
/// # Examples
///
/// A test of a worker that pushes its results to a collector it dials addresses that queue by its
/// side:
///
/// ```
/// # #[cfg(feature = "testing")]
/// # mod demo {
/// use std::error::Error;
///
/// use ruststream::testing::TestApp;
/// use ruststream_zeromq::Connect;
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", reply)]
/// async fn work(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// pub fn app() -> RustStream {
///     let collector = ZmqQueue::new(ZmqEndpoint::connect("tcp://collector:5560")).bindable();
///     let results = collector.bind(Publish);
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
///             b.include(work).out_reply(results);
///         })
///         .register_broker(collector)
/// }
///
/// pub async fn the_result_reaches_the_collector() -> Result<(), Box<dyn Error>> {
///     let tb = TestApp::start(app()).await?;
///
///     tb.broker::<ZmqQueue>()
///         .message(&Job { id: 7 })
///         .to("jobs")
///         .publish()
///         .await?;
///
///     tb.broker::<ZmqQueue<Connect>>()
///         .published::<Done>("results")
///         .assert_called_once()
///         .with(&Done { id: 7 });
///     tb.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # #[cfg(feature = "testing")]
/// # use std::error::Error;
/// # #[cfg(feature = "testing")]
/// # #[tokio::main(flavor = "multi_thread", worker_threads = 2)]
/// # async fn main() -> Result<(), Box<dyn Error>> {
/// #     demo::the_result_reaches_the_collector().await
/// # }
/// # #[cfg(not(feature = "testing"))]
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Connect;

impl Side {
    /// The side a public `Role` names.
    pub(crate) const fn of<Role: EndpointRole>() -> Self {
        <Role as sealed::Sealed>::SIDE
    }
}

impl sealed::Sealed for Bind {
    const SIDE: Side = Side::Bind;
}

impl sealed::Sealed for Connect {
    const SIDE: Side = Side::Connect;
}

impl EndpointRole for Bind {}

impl EndpointRole for Connect {}

/// An address (`tcp://...` or `ipc://...`) and the side of it this process takes, fixed by the
/// constructor and carried in the type.
///
/// # Examples
///
/// A worker that listens for jobs from a producer on the same host and pushes its results to a
/// collector it dials over the network:
///
/// ```
/// use ruststream_zeromq::queue::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", reply)]
/// async fn work(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let collector = ZmqQueue::new(ZmqEndpoint::connect("tcp://collector:5560")).bindable();
///     let results = collector.bind(Publish);
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("ipc:///tmp/jobs")), |b| {
///             b.include(work).out_reply(results);
///         })
///         .register_broker(collector)
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct ZmqEndpoint<Role = Bind> {
    inner: Endpoint,
    role: PhantomData<Role>,
}

impl ZmqEndpoint<Bind> {
    /// This process listens on `address`.
    ///
    /// # Examples
    ///
    /// A worker that listens on a local socket for the producers on its host:
    ///
    /// ```
    /// use ruststream_zeromq::queue::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     println!("working on job {}", job.id);
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
    ///         ZmqQueue::new(ZmqEndpoint::bind("ipc:///tmp/jobs")),
    ///         |b| {
    ///             b.include(work);
    ///         },
    ///     )
    /// }
    /// ```
    pub fn bind(address: impl Into<String>) -> Self {
        Self::on(address.into())
    }
}

impl ZmqEndpoint<Connect> {
    /// This process dials out to `address`.
    ///
    /// # Examples
    ///
    /// A worker that pushes its results to the collector it dials:
    ///
    /// ```
    /// use ruststream_zeromq::queue::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "results")]
    /// struct Done {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs", reply)]
    /// async fn work(job: &Job) -> Done {
    ///     Done { id: job.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let collector = ZmqQueue::new(ZmqEndpoint::connect("tcp://collector:5560")).bindable();
    ///     let results = collector.bind(Publish);
    ///     RustStream::new(AppInfo::new("worker", "0.1.0"))
    ///         .with_broker(ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5555")), |b| {
    ///             b.include(work).out_reply(results);
    ///         })
    ///         .register_broker(collector)
    /// }
    /// ```
    pub fn connect(address: impl Into<String>) -> Self {
        Self::on(address.into())
    }
}

impl<Role: EndpointRole> ZmqEndpoint<Role> {
    fn on(address: String) -> Self {
        Self {
            inner: Endpoint {
                address,
                side: Side::of::<Role>(),
            },
            role: PhantomData,
        }
    }
}

impl<Role> ZmqEndpoint<Role> {
    /// The address string.
    ///
    /// # Examples
    ///
    /// A worker that logs where it listens before it starts:
    ///
    /// ```
    /// use ruststream_zeromq::queue::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     println!("working on job {}", job.id);
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let endpoint = ZmqEndpoint::bind("tcp://0.0.0.0:5555");
    ///     println!("worker listening on {}", endpoint.address());
    ///     RustStream::new(AppInfo::new("worker", "0.1.0"))
    ///         .with_broker(ZmqQueue::new(endpoint), |b| {
    ///             b.include(work);
    ///         })
    /// }
    /// ```
    #[must_use]
    pub fn address(&self) -> &str {
        &self.inner.address
    }

    /// The endpoint the shared machinery holds, its side read off the type.
    pub(crate) fn into_inner(self) -> Endpoint {
        self.inner
    }
}

/// An endpoint with its side read off the public type: what the three patterns' shared
/// machinery holds, so one attach routine serves every broker whatever its `Role`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoint {
    address: String,
    side: Side,
}

impl Endpoint {
    /// The address string.
    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    /// The side of the endpoint this process takes.
    pub(crate) const fn side(&self) -> Side {
        self.side
    }

    /// The address a client connects to, without the transport scheme: a host and optional port
    /// on `tcp://`, the socket path on `ipc://`. That is the shape the generated document
    /// specifies for a server's host.
    ///
    /// The network form is the framework's own URL rule ([`ServerSpec::host_from_url`]), which
    /// drops the scheme and any userinfo an operator wrote into the endpoint, so a published
    /// document carries no credential. `ZeroMQ` addresses none of its own that way - its CURVE
    /// keys are socket options - so this guards a shape the transport does not use.
    ///
    /// An `ipc` address names a filesystem path rather than an authority, so it is kept whole: the
    /// URL rule would cut it at its first separator, and `@` is an ordinary character in a path.
    pub(crate) fn host(&self) -> String {
        match self.address.split_once("://") {
            Some(("ipc", path)) => path.to_owned(),
            _ => ServerSpec::host_from_url(&self.address),
        }
    }

    /// How every pattern of this crate describes itself: the coordinate a client connects to, the
    /// protocol, the ZMTP version the implementation greets with, and this crate's own binding.
    ///
    /// One place rather than three, because the answer does not depend on the socket pair: what a
    /// pattern adds of its own goes on its channels, through its publish policy.
    pub(crate) fn server_spec(&self) -> ServerSpec {
        let spec = ServerSpec::new(self.host(), "zeromq").protocol_version(ZMTP_VERSION);
        #[cfg(feature = "asyncapi")]
        let spec = spec.bindings(bindings::server(self));
        spec
    }

    /// Rejects endpoints the implementation cannot serve, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), ZmqError> {
        if self.address.starts_with("tcp://") || self.address.starts_with("ipc://") {
            Ok(())
        } else {
            Err(ZmqError::Invalid(format!(
                "'{}' must use the tcp:// or ipc:// transport",
                self.address
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use ruststream::DescribeServer;

    use super::*;
    use crate::{ZmqFanout, ZmqQueue, ZmqRpc};

    #[test]
    fn unsupported_transports_are_rejected_before_io() {
        assert!(
            ZmqEndpoint::bind("inproc://x")
                .into_inner()
                .validate()
                .is_err()
        );
        assert!(
            ZmqEndpoint::connect("udp://x:1")
                .into_inner()
                .validate()
                .is_err()
        );
        assert!(
            ZmqEndpoint::bind("tcp://0.0.0.0:5555")
                .into_inner()
                .validate()
                .is_ok()
        );
        assert!(
            ZmqEndpoint::bind("ipc:///tmp/x")
                .into_inner()
                .validate()
                .is_ok()
        );
    }

    /// The constructor is the only place a side is chosen, and the value the machinery reads is
    /// the one the type names.
    #[test]
    fn the_side_the_machinery_reads_is_the_one_the_type_names() {
        assert_eq!(
            ZmqEndpoint::bind("tcp://0.0.0.0:5555").into_inner().side(),
            Side::Bind
        );
        assert_eq!(
            ZmqEndpoint::connect("tcp://ml:5555").into_inner().side(),
            Side::Connect
        );
    }

    #[test]
    fn the_host_drops_the_scheme_on_both_transports() {
        assert_eq!(
            ZmqEndpoint::connect("tcp://broker:5555")
                .into_inner()
                .host(),
            "broker:5555"
        );
        assert_eq!(
            ZmqEndpoint::bind("tcp://0.0.0.0:0").into_inner().host(),
            "0.0.0.0:0"
        );
        assert_eq!(
            ZmqEndpoint::bind("ipc:///tmp/orders").into_inner().host(),
            "/tmp/orders"
        );
    }

    #[test]
    fn the_host_drops_userinfo_on_the_last_at_sign() {
        assert_eq!(
            ZmqEndpoint::connect("tcp://user:pa@ss@broker:5555")
                .into_inner()
                .host(),
            "broker:5555"
        );
        // A path is not an authority, so an '@' in it is part of the name, not a separator.
        assert_eq!(
            ZmqEndpoint::bind("ipc:///tmp/a@b").into_inner().host(),
            "/tmp/a@b"
        );
    }

    /// Every pattern describes itself the same way, and the generated document specifies a host,
    /// not a URL.
    #[test]
    fn no_pattern_puts_a_scheme_in_the_described_host() {
        for address in ["tcp://broker:5555", "ipc:///tmp/orders"] {
            let specs = [
                ZmqQueue::new(ZmqEndpoint::connect(address)).describe_server(),
                ZmqFanout::new(ZmqEndpoint::connect(address)).describe_server(),
                ZmqRpc::new(ZmqEndpoint::connect(address)).describe_server(),
            ];
            for spec in specs {
                let host = spec.host.expect("a networked broker describes a host");
                assert!(!host.contains("://"), "host carries a scheme: {host}");
                assert_eq!(spec.protocol, "zeromq");
            }
        }
    }
}
