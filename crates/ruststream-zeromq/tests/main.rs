//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod asyncapi;
mod back_pressure;
mod binding_publisher;
mod both_modes_zmq;
mod cancel_safety;
mod conformance_zmq;
mod connect_runtime;
mod harness_zmq;
mod header_values;
mod in_process_zmq;
mod integration_zmq;
mod peer_restart;
mod publish_handover;
mod publisher_before_subscription;
mod redelivery;
mod reply_destinations;
mod settlement;
mod ui;
