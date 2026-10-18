//! The sink of the Pipelines guide: it binds the endpoint every worker pushes its results to and
//! collects them in batches.
//!
//! ```text
//! cargo run --example guide_pipeline_sink --features testing -- run
//! cargo test --example guide_pipeline_sink --features testing
//! ```

// --8<-- [start:sink]
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
struct Done {
    id: u64,
    items: u32,
}

#[subscriber("results")]
async fn collect(results: &[Done]) -> HandlerOutcome {
    let items: u32 = results.iter().map(|done| done.items).sum();
    tracing::info!(results = results.len(), items, "results collected");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("sink", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5558")),
        |b| {
            // At most 32 results per call.
            b.include(collect.batch(nonzero!(32)));
        },
    )
}
// --8<-- [end:sink]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_zeromq::queue::prelude::*;
    use serde::Serialize;

    use super::app;

    /// A result as a worker pushes it.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "results")]
    struct Pushed {
        id: u64,
        items: u32,
    }

    #[tokio::test]
    async fn results_that_wait_together_are_collected_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let results: Vec<Pushed> = (1..=3).map(|id| Pushed { id, items: 2 }).collect();
        // Pushed at once, the three results wait for the handler together.
        let queue = tb.broker::<ZmqQueue>();
        for pushed in join_all(results.iter().map(|done| queue.message(done).publish())).await {
            pushed?;
        }

        tb.broker::<ZmqQueue>()
            .subscriber("results")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
