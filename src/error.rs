use std::path::PathBuf;

/// Unified error type for the Bria orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML deserialization error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    #[error("Template render error: {0}")]
    Template(#[from] minijinja::Error),

    #[error("Expression evaluation error: {0}")]
    Expression(String),

    #[error("Task execution error: {0}")]
    Task(String),

    #[error("Pipeline error: {0}")]
    Pipeline(String),

    #[error("Source error ({source_id}): {message}")]
    Source { source_id: String, message: String },

    #[error("Source error ({source_id}): {message}: {error}")]
    SourceError {
        source_id: String,
        message: String,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Sink error ({sink_id}): {message}")]
    Sink { sink_id: String, message: String },

    #[error("Sink error ({sink_id}): {message}: {error}")]
    SinkError {
        sink_id: String,
        message: String,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Server error: {0}")]
    Server(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("AMQP error: {0}")]
    Amqp(#[from] lapin::Error),

    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),

    #[error("Cron parse error: {0}")]
    Cron(String),

    #[error("Environment variable not set: {0}")]
    EnvVar(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Invalid regex: {0}")]
    Regex(#[from] regex::Error),

    #[error("State store error: {0}")]
    State(String),

    #[error("{0}")]
    Message(String),
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Create a config error from a message.
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a validation error from a message.
    pub fn validation(msg: impl Into<String>) -> Self {
        Self::Validation(msg.into())
    }

    /// Create a pipeline error from a message.
    pub fn pipeline(msg: impl Into<String>) -> Self {
        Self::Pipeline(msg.into())
    }

    /// Create a task error from a message.
    pub fn task(msg: impl Into<String>) -> Self {
        Self::Task(msg.into())
    }

    /// Create an unsupported error from a message.
    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self::Unsupported(msg.into())
    }

    /// Create a state-store error.
    pub fn state(msg: impl Into<String>) -> Self {
        Self::State(msg.into())
    }

    /// Create an internal error.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Create a `SourceError` that preserves the inner error's source chain.
    pub fn source_err(
        source_id: impl Into<String>,
        message: impl Into<String>,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::SourceError {
            source_id: source_id.into(),
            message: message.into(),
            error: Box::new(error),
        }
    }

    /// Create a `SinkError` that preserves the inner error's source chain.
    pub fn sink_err(
        sink_id: impl Into<String>,
        message: impl Into<String>,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::SinkError {
            sink_id: sink_id.into(),
            message: message.into(),
            error: Box::new(error),
        }
    }
}

impl From<String> for Error {
    /// Converts a plain string into an `Internal` error. This removes the
    /// duplicate-formatting `Message` variant from the `?` path; callers that
    /// need structured error sub-types should use the explicit constructors.
    fn from(s: String) -> Self {
        Self::Internal(s)
    }
}
