//! What this crate contributes to a generated `AsyncAPI` document beyond the server coordinate.
//!
//! The specification's protocol keys are a closed list and `ZeroMQ` is not on it, so everything
//! here travels in one `x-` extension at the level it belongs to. The key is namespaced to this
//! crate, because no community extension for `ZeroMQ` exists to align with: a reader who knows the
//! name knows who wrote the body.
//!
//! Two levels carry one. The server says how to attach to the endpoint: which transport, which
//! coordinate, and which side of it this service is. The channel says which socket pair the
//! messages on it travel over and what their name frame holds, which is what a peer needs before
//! it can open the other end and read from it.
//!
//! Nothing here reads a connection, and nothing here is a credential: the coordinate is the same
//! one [`DescribeServer`](ruststream::DescribeServer) reports, with the scheme and any userinfo
//! already dropped.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

use crate::endpoint::{Endpoint, Side};

/// The extension key both levels sit under.
const EXTENSION: &str = "x-ruststream-zeromq";

/// The socket pair of one pattern, as the extension names it on a channel.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SocketPair {
    /// `ZmqQueue`: competing consumers.
    PushPull,
    /// `ZmqFanout`: broadcast with prefix filtering.
    PubSub,
    /// `ZmqRpc`: request and reply.
    DealerRouter,
}

impl SocketPair {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PushPull => "PUSH/PULL",
            Self::PubSub => "PUB/SUB",
            Self::DealerRouter => "DEALER/ROUTER",
        }
    }

    /// Whether frame 0 of a message on this channel holds the channel's own name.
    ///
    /// The one-way patterns put the destination there, and a SUB peer filters on that very
    /// prefix. A reply on DEALER/ROUTER is addressed by the requester's identity instead, and
    /// its frame 0 holds the constant the wire layout documents, so the channel's name never
    /// reaches the peer and the binding does not claim it does.
    const fn addresses_by_name(self) -> bool {
        match self {
            Self::PushPull | Self::PubSub => true,
            Self::DealerRouter => false,
        }
    }
}

#[derive(Serialize)]
struct ServerBody {
    transport: &'static str,
    endpoint: String,
    role: &'static str,
}

#[derive(Serialize)]
struct ChannelBody {
    #[serde(rename = "socketPair")]
    socket_pair: &'static str,
    #[serde(rename = "nameFrame", skip_serializing_if = "Option::is_none")]
    name_frame: Option<String>,
}

/// The transport scheme, as this binding names it.
///
/// [`Endpoint::validate`] admits `tcp://` and `ipc://` and nothing else, so an endpoint a service
/// reaches here is one of the two.
fn transport(endpoint: &Endpoint) -> &'static str {
    if endpoint.address().starts_with("ipc://") {
        "ipc"
    } else {
        "tcp"
    }
}

/// The server binding: the transport, the coordinate, and which side of it this service takes.
pub(crate) fn server(endpoint: &Endpoint) -> Bindings {
    let body = ServerBody {
        transport: transport(endpoint),
        endpoint: endpoint.host(),
        role: match endpoint.side() {
            Side::Bind => "bind",
            Side::Connect => "connect",
        },
    };
    wrap(&body)
}

/// The channel binding: the socket pair the messages on this channel travel over, and the name
/// their first frame holds.
///
/// `destination` is what the document reports as the channel's address, so a peer reads the value
/// it must put in frame 0 to reach this channel, and on PUB/SUB the prefix it subscribes with.
/// Which patterns say it is [`SocketPair::addresses_by_name`]'s decision, not the caller's, so the
/// three publish policies cannot drift apart.
pub(crate) fn channel(pair: SocketPair, destination: &str) -> Bindings {
    wrap(&ChannelBody {
        socket_pair: pair.as_str(),
        name_frame: pair.addresses_by_name().then(|| destination.to_owned()),
    })
}

/// A binding that fails to build is a binding the document goes without: a description of the
/// transport never holds up a service.
fn wrap<T: Serialize>(body: &T) -> Bindings {
    Binding::extension(EXTENSION, body)
        .map_or_else(|_| Bindings::new(), |b| Bindings::new().with(b))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::ZmqEndpoint;

    #[test]
    fn the_server_binding_reports_the_transport_and_the_role() {
        let json = serde_json::to_value(server(
            &ZmqEndpoint::bind("tcp://0.0.0.0:5555").into_inner(),
        ))
        .expect("the binding serializes");
        let body = &json[EXTENSION];
        assert_eq!(body["transport"], "tcp");
        assert_eq!(body["endpoint"], "0.0.0.0:5555");
        assert_eq!(body["role"], "bind");
    }

    /// An operator who writes a password into the endpoint must not publish it: the coordinate
    /// here is the same credential-free one the server description carries.
    #[test]
    fn the_server_binding_drops_userinfo_with_the_scheme() {
        let endpoint = ZmqEndpoint::connect("tcp://user:hunter2@broker:5555").into_inner();
        let json = serde_json::to_string(&server(&endpoint)).expect("the binding serializes");
        assert!(
            !json.contains("hunter2"),
            "the binding carries a password: {json}"
        );
        assert!(
            json.contains("broker:5555"),
            "the binding lost the coordinate: {json}"
        );
    }

    #[test]
    fn an_ipc_endpoint_reports_its_socket_path() {
        let json = serde_json::to_value(server(
            &ZmqEndpoint::connect("ipc:///tmp/orders").into_inner(),
        ))
        .expect("the binding serializes");
        assert_eq!(json[EXTENSION]["transport"], "ipc");
        assert_eq!(json[EXTENSION]["endpoint"], "/tmp/orders");
        assert_eq!(json[EXTENSION]["role"], "connect");
    }

    #[test]
    fn every_pattern_names_its_socket_pair() {
        for (pair, expected) in [
            (SocketPair::PushPull, "PUSH/PULL"),
            (SocketPair::PubSub, "PUB/SUB"),
            (SocketPair::DealerRouter, "DEALER/ROUTER"),
        ] {
            let json =
                serde_json::to_value(channel(pair, "orders")).expect("the binding serializes");
            assert_eq!(json[EXTENSION]["socketPair"], expected);
        }
    }

    /// A peer that reads the document learns what to put in frame 0, and what to subscribe with
    /// on PUB/SUB, without being told the wire layout separately.
    #[test]
    fn the_one_way_patterns_report_the_destination_as_their_name_frame() {
        for pair in [SocketPair::PushPull, SocketPair::PubSub] {
            let json =
                serde_json::to_value(channel(pair, "orders")).expect("the binding serializes");
            assert_eq!(json[EXTENSION]["nameFrame"], "orders");
        }
    }

    /// A reply travels to the identity the ROUTER supplies, so the channel's name is not an
    /// address a peer can use, and the binding stays silent rather than inventing one.
    #[test]
    fn a_responder_reports_no_name_frame() {
        let json = serde_json::to_value(channel(SocketPair::DealerRouter, "reply"))
            .expect("the binding serializes");
        assert_eq!(json[EXTENSION]["nameFrame"], Value::Null);
    }
}
