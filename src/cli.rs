use clap::{Parser, Subcommand};

/// Environment variable used to select Bria's universal configuration file.
pub const CONFIG_PATH_ENV: &str = "BRIA_CONFIG";
/// Configuration path used when neither `--config` nor `BRIA_CONFIG` is set.
pub const DEFAULT_CONFIG_PATH: &str = "Config.toml";

/// Bria — Multi-pipeline job orchestrator.
///
/// Configuration over hardcoding. Stateless by design. Simple. Consistent.
#[derive(Parser, Debug)]
#[command(name = "bria", version, about, long_about = None)]
pub struct Cli {
    /// Path to the TOML configuration file.
    /// Can also be set via BRIA_CONFIG environment variable.
    #[arg(
        long = "config",
        env = CONFIG_PATH_ENV,
        default_value = DEFAULT_CONFIG_PATH,
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
    /// Parse and strictly validate configuration, then exit without starting workers.
    Check,
}

impl Cli {
    /// Returns true if the command is "ping".
    pub fn is_ping(&self) -> bool {
        matches!(self.command, Some(Command::Ping))
    }

    /// Returns true if the command is `check`.
    pub fn is_check(&self) -> bool {
        matches!(self.command, Some(Command::Check))
    }
}
