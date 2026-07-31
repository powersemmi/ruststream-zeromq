//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the `ZeroMQ` transport.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors so the public API does not leak the
/// implementation's error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ZmqError {
    /// Binding or connecting a socket failed.
    #[error("zeromq endpoint error on '{endpoint}': {source}")]
    Endpoint {
        /// The endpoint the socket targeted.
        endpoint: String,
        /// The implementation's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Sending failed (no connected peer after the retry window, or the transport failed).
    #[error("zeromq send error to '{name}': {reason}")]
    Send {
        /// The message name the send targeted.
        name: String,
        /// The failure reason.
        reason: String,
    },

    /// Receiving failed.
    #[error("zeromq receive error: {0}")]
    Receive(String),

    /// A peer sent frames that do not follow the documented wire layout.
    #[error("zeromq wire error: {0}")]
    Wire(String),

    /// A request did not produce a reply within the caller's timeout.
    #[error("zeromq request timed out")]
    RequestTimeout,

    /// The handle is used before `connect` filled the shared state, or after `shutdown`.
    #[error("zeromq transport is not connected")]
    NotConnected,

    /// An endpoint or descriptor is invalid.
    #[error("invalid zeromq descriptor: {0}")]
    Invalid(String),
}

/// Boxes an implementation error into the crate's `Box<dyn StdError>` source form.
pub(crate) fn box_err<E>(err: E) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
{
    Box::new(err)
}
