//! [`ZmqEndpoint`]: an address plus the explicit bind-or-connect role.
//!
//! There is no server in the middle, so which side listens is a deployment decision, not a
//! property of the transport; the endpoint states it.

use crate::error::ZmqError;

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
/// let listener = ZmqEndpoint::bind("tcp://*:5555");
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
    use super::*;

    #[test]
    fn unsupported_transports_are_rejected_before_io() {
        assert!(ZmqEndpoint::bind("inproc://x").validate().is_err());
        assert!(ZmqEndpoint::connect("udp://x:1").validate().is_err());
        assert!(ZmqEndpoint::bind("tcp://*:5555").validate().is_ok());
        assert!(ZmqEndpoint::bind("ipc:///tmp/x").validate().is_ok());
    }
}
