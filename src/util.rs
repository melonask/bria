use crate::error::{Error, Result};
use std::time::{Duration, Instant};

/// Validate that a SQL identifier is safe for dynamic SQL interpolation.
pub fn validate_identifier(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Config(format!(
            "{kind} identifier must not be empty"
        )));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(Error::Config(format!(
            "{kind} identifier '{name}' must start with a letter or underscore"
        )));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::Config(format!(
            "{kind} identifier '{name}' contains invalid characters"
        )));
    }
    Ok(())
}

/// Surround a validated identifier with double quotes for safe interpolation
/// into a dynamic SQL string.
///
/// The `kind` parameter is used in validation-error messages (e.g. `"table"`,
/// `"column"`) to give callers meaningful feedback. The identifier is validated
/// at runtime (not just in debug builds) so that malformed configuration is
/// always rejected.
pub fn quote_ident(kind: &str, name: &str) -> Result<String> {
    validate_identifier(kind, name)?;
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

#[cfg(feature = "amqp")]
pub fn amqp_url_with_credentials(url: &str, username: &str, password: &str) -> Result<String> {
    if username.is_empty() && password.is_empty() {
        return Ok(url.to_string());
    }

    let mut parsed = url::Url::parse(url)?;
    if !username.is_empty() {
        parsed
            .set_username(username)
            .map_err(|_| Error::Config("AMQP URL cannot accept configured username".to_string()))?;
    }
    if !password.is_empty() {
        parsed
            .set_password(Some(password))
            .map_err(|_| Error::Config("AMQP URL cannot accept configured password".to_string()))?;
    }
    Ok(parsed.to_string())
}

pub fn cancel_signal_ttl(config: &crate::config::Config) -> Duration {
    Duration::from_secs(config.global.cancel_signal_ttl_secs.max(1))
}

pub fn prune_expired_cancel_signals(signals: &dashmap::DashMap<String, Instant>, ttl: Duration) {
    signals.retain(|_, inserted_at| inserted_at.elapsed() <= ttl);
}
