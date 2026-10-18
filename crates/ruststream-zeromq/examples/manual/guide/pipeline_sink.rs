//! The sink of the Pipelines guide, written without the macros: the handler is a type with an
//! `impl Handle` over a slice, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! cargo run --example manual_guide_pipeline_sink --features testing -- run
//! cargo test --example manual_guide_pipeline_sink --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:sink]
use ruststream_zeromq::queue::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Done {
    id: u64,
    items: u32,
}

struct Collect;

impl Handle<[Done]> for Collect {
    fn handle(
        &self,
        results: &[Done],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let items: u32 = results.iter().map(|done| done.items).sum();
        tracing::info!(results = results.len(), items, "results collected");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("sink", "0.1.0")).with_broker(
        ZmqQueue::new(ZmqEndpoint::bind("tcp://0.0.0.0:5558")),
        |b| {
            // At most 32 results per call.
            b.include(subscriber("results", Collect).batch(nonzero!(32)).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
    #[derive(Serialize)]
    struct Pushed {
        id: u64,
        items: u32,
    }

    impl OutgoingDestination for Pushed {
        type Form = FixedName;
        const DESTINATION: &'static str = "results";
    }

    impl MessageHeaders for Pushed {
        type Contract = NoHeaders;
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
