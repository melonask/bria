#![allow(
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::derivable_impls,
    clippy::field_reassign_with_default,
    clippy::map_flatten,
    clippy::needless_borrow
)]

pub mod cli;
pub mod config;
pub mod context;
pub mod error;
pub mod expression;
pub mod orchestrator;
pub mod pipeline;
pub mod server;
pub mod sinks;
pub mod sources;
pub mod state;
pub mod task_runner;
pub mod template;
pub mod util;

pub use cli::Cli;
pub use config::Config;
pub use context::{Context, Job, PipelineResult, StepResult};
pub use error::{Error, Result};
pub use orchestrator::{Orchestrator, run_pipeline_once, run_pipeline_once_with_config};
pub use state::{JobStateRecord, StateStore, create_store};

/// Main entry point for the library.
/// Loads configuration, validates it, and runs the orchestrator.
pub async fn run(cli: Cli) -> Result<()> {
    if cli.is_ping() {
        println!("pong");
        return Ok(());
    }

    let config = Config::load_from_path(&cli.config)?;
    config.validate()?;

    let orchestrator = Orchestrator::new(config).await?;
    orchestrator.run().await
}
