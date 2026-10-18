//! The watcher of the Broadcast guide: it dials the announcer's fan-out and receives every
//! announcement whose name starts with `jobs.`.
//!
//! ```text
//! cargo run --example guide_broadcast_watcher --features testing -- run
//! cargo test --example guide_broadcast_watcher --features testing
//! ```

// --8<-- [start:watcher]
use ruststream_zeromq::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
struct JobEvent {
    id: u64,
}

/// Every job announcement: `jobs.done`, `jobs.failed`, and whatever `jobs.` name comes next.
#[subscriber("jobs.")]
async fn watch(event: &JobEvent, ctx: &mut Context) -> HandlerOutcome {
    tracing::info!(event.id, name = ctx.name(), "job announced");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // A subscription that dials names where its retry copies go: a queue this service binds.
    let retries = ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5560")).bindable();
    let copies = retries.bind(ZmqQueuePublish);
    RustStream::new(AppInfo::new("watcher", "0.1.0"))
        .with_broker(
            ZmqFanout::new(ZmqEndpoint::connect("tcp://worker:5556")),
            |b| {
                b.include(watch).out_retry(copies).to("jobs.");
            },
        )
        .with_broker(retries, |b| {
            b.include(watch);
        })
}
// --8<-- [end:watcher]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_zeromq::Connect;
    use ruststream_zeromq::prelude::*;
    use serde::Serialize;

    use super::{JobEvent, app};

    /// An announcement as the worker publishes it.
    #[derive(Serialize, Outgoing)]
    struct Announced {
        id: u64,
    }

    #[tokio::test]
    async fn the_watcher_receives_every_name_under_its_prefix() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (name, id) in [("jobs.done", 1), ("jobs.failed", 2), ("metrics.cpu", 3)] {
            tb.broker::<ZmqFanout<Connect>>()
                .message(&Announced { id })
                .to(name)
                .publish()
                .await?;
        }

        let fanout = tb.broker::<ZmqFanout<Connect>>();
        let watcher = fanout.subscriber("jobs.");
        assert_eq!(
            watcher.received::<JobEvent>(),
            [JobEvent { id: 1 }, JobEvent { id: 2 }],
        );
        watcher.settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
