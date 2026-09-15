//! [`ZmqEndpoint`]: an address plus the explicit bind-or-connect role.
//!
//! There is no server in the middle, so which side listens is a deployment decision, not a
//! property of the transport; the endpoint states it.

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Bind,
    Connect,
}

/// An address (`tcp://...` or `ipc://...`) with an explicit listening role.
///
/// # Examples
///
/// ```
/// use ruststream_zeromq::ZmqEndpoint;
///
/// let listener = ZmqEndpoint::bind("tcp://0.0.0.0:5555");
/// let dialer = ZmqEndpoint::connect("tcp://ml:5555");
/// let local = ZmqEndpoint::bind("ipc:///tmp/orders");
/// # let _ = (listener, dialer, local);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct ZmqEndpoint {
    pub(crate) address: String,
    pub(crate) role: Role,
}

impl ZmqEndpoint {
    /// This process listens on `address`.
    pub fn bind(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            role: Role::Bind,
        }
    }

    /// This process dials out to `address`.
    pub fn connect(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            role: Role::Connect,
        }
    }

    /// The address string.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
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
        assert!(ZmqEndpoint::bind("inproc://x").validate().is_err());
        assert!(ZmqEndpoint::connect("udp://x:1").validate().is_err());
        assert!(ZmqEndpoint::bind("tcp://0.0.0.0:5555").validate().is_ok());
        assert!(ZmqEndpoint::bind("ipc:///tmp/x").validate().is_ok());
    }

    #[test]
    fn the_host_drops_the_scheme_on_both_transports() {
        assert_eq!(
            ZmqEndpoint::connect("tcp://broker:5555").host(),
            "broker:5555"
        );
        assert_eq!(ZmqEndpoint::bind("tcp://0.0.0.0:0").host(), "0.0.0.0:0");
        assert_eq!(ZmqEndpoint::bind("ipc:///tmp/orders").host(), "/tmp/orders");
    }

    #[test]
    fn the_host_drops_userinfo_on_the_last_at_sign() {
        assert_eq!(
            ZmqEndpoint::connect("tcp://user:pa@ss@broker:5555").host(),
            "broker:5555"
        );
        // A path is not an authority, so an '@' in it is part of the name, not a separator.
        assert_eq!(ZmqEndpoint::bind("ipc:///tmp/a@b").host(), "/tmp/a@b");
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
