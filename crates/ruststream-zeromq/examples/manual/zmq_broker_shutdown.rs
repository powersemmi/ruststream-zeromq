//! The service of `examples/zmq_broker_shutdown.rs` without the macros: `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_zmq_broker_shutdown --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_zeromq::fanout::prelude::*;

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        ZmqFanout::new(ZmqEndpoint::bind("tcp://0.0.0.0:5557")),
        |b| {
            // The publisher lives as long as the hook; a task that kept it past shutdown would
            // get `ZmqError::NotConnected` from it.
            b.after_startup(Publish, async move |publisher| {
                let started = Started {
                    service: "worker".to_owned(),
                };
                publisher.message(&started).publish().await
            });
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Started {
    service: String,
}

// `#[outgoing(name = "workers.started")]` by hand: the type names the channel it is published to.
impl OutgoingDestination for Started {
    type Form = FixedName;
    const DESTINATION: &'static str = "workers.started";
}

impl MessageHeaders for Started {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::ZmqError;
    use ruststream_zeromq::fanout::prelude::*;

    use super::{Started, app};

    #[tokio::test]
    async fn the_service_announces_itself_once_and_shuts_down() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<ZmqFanout>()
            .published::<Started>("workers.started")
            .assert_called_once()
            .with(&Started {
                service: "worker".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    /// The subject is the lifecycle itself, so the app runs as `main` runs it: started, then shut
    /// down, with a publisher a sibling task took in between.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publisher_taken_before_shutdown_reports_not_connected()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let events = ZmqFanout::new(ZmqEndpoint::bind("tcp://127.0.0.1:0")).bindable();
        let announce = events.bind(Publish);
        let running = RustStream::new(AppInfo::new("worker", "0.1.0"))
            .register_broker(events)
            .start()
            .await?;
        let publisher = running.publisher(announce).await?;

        running.shutdown().await?;

        let err = publisher
            .message(&Started {
                service: "worker".to_owned(),
            })
            .publish()
            .await
            .expect_err("the broker is shut down");
        let mut cause: Option<&(dyn Error + 'static)> = Some(&err);
        while let Some(err) = cause {
            if let Some(zmq) = err.downcast_ref::<ZmqError>() {
                assert!(matches!(zmq, ZmqError::NotConnected), "{zmq}");
                return Ok(());
            }
            cause = err.source();
        }
        panic!("the publish did not fail with a ZmqError: {err}");
    }
}
