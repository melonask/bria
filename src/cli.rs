use clap::{Parser, Subcommand};

/// Bria — Multi-pipeline job orchestrator.
///
/// Configuration over hardcoding. Stateless by design. Simple. Consistent.
#[derive(Parser, Debug)]
#[command(name = "bria", version, about, long_about = None)]
pub struct Cli {
    /// Path to the TOML configuration file.
    /// Can also be set via BRIA_CONFIG environment variable.
    #[arg(
        short = 'c',
        long = "config",
        env = "BRIA_CONFIG",
        default_value = "Config.toml",
        global = true
    )]
    pub config: String,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Print "pong" to stdout and exit.
    Ping,
}

impl Cli {
    /// Returns true if the command is "ping".
    pub fn is_ping(&self) -> bool {
        matches!(self.command, Some(Command::Ping))
    }
}
