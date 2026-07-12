use std::path::PathBuf;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::Row;
use tokio::sync::mpsc;

use crate::config;
use crate::context::Job;
#[cfg(any(
    feature = "cron",
    feature = "amqp",
    feature = "sqlite",
    feature = "postgres"
))]
use crate::error::Error;
use crate::error::Result;
#[cfg(feature = "amqp")]
use crate::util::amqp_url_with_credentials;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::util::{quote_ident, validate_identifier};

/// Create a Job from a payload value.
pub fn create_job(source: &config::SourceConfig, value: &serde_json::Value) -> Job {
    let id = if source.id_field.is_empty() {
        ulid::Ulid::r#gen().to_string()
    } else {
        value
            .get(&source.id_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| ulid::Ulid::r#gen().to_string())
    };
    Job {
        id,
        source: source.id.clone(),
        payload: value.clone(),
        correlation_key: None,
        labels: source.labels.clone(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// File source
// ─────────────────────────────────────────────────────────────────────────────

pub async fn run_file_source_inline(
    source: &config::SourceConfig,
    tx: &mpsc::UnboundedSender<Job>,
) -> Result<()> {
    let path = &source.path;
    let poll_interval = std::time::Duration::from_secs(source.poll_interval_secs.max(1));
    let cursor_path = if source.track_cursor && !path.is_dir() {
        Some(PathBuf::from(format!("{}.bria-cursor", path.display())))
    } else {
        None
    };
    let mut last_size: u64 = 0;
    let mut directory_snapshot: std::collections::HashMap<PathBuf, u64> =
        std::collections::HashMap::new();
    // Track seen job IDs per file for authoritative mode.
    let mut seen_ids: std::collections::HashMap<PathBuf, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    if let Some(ref cursor) = cursor_path
        && let Ok(data) = std::fs::read_to_string(cursor)
        && let Ok(pos) = data.trim().parse::<u64>()
    {
        last_size = pos;
    }
    loop {
        match std::fs::metadata(path) {
            Ok(meta) => {
                if path.is_dir() {
                    // Collect files that currently exist for deleted-file detection in authoritative mode.
                    let mut current_files: std::collections::HashSet<PathBuf> =
                        std::collections::HashSet::new();
                    if let Ok(entries) = std::fs::read_dir(path) {
                        for entry in entries.flatten() {
                            let file_path = entry.path();
                            if let Some(ext) = file_path.extension().and_then(|e| e.to_str())
                                && matches!(ext, "json" | "jsonl" | "csv")
                            {
                                current_files.insert(file_path.clone());
                                let Ok(metadata) = std::fs::metadata(&file_path) else {
                                    continue;
                                };
                                let current_size = metadata.len();
                                let file_cursor_path =
                                    PathBuf::from(format!("{}.bria-cursor", file_path.display()));
                                let persisted_size = if source.track_cursor {
                                    std::fs::read_to_string(&file_cursor_path)
                                        .ok()
                                        .and_then(|data| data.trim().parse::<u64>().ok())
                                        .unwrap_or(0)
                                } else {
                                    0
                                };
                                let previous_size = directory_snapshot
                                    .get(&file_path)
                                    .copied()
                                    .unwrap_or(persisted_size);
                                if !source.authoritative
                                    && source.track_cursor
                                    && current_size <= previous_size
                                {
                                    continue;
                                }
                                let content =
                                    std::fs::read_to_string(&file_path).unwrap_or_default();
                                let is_authoritative_reload = source.authoritative;
                                let new_content = if is_authoritative_reload {
                                    &content
                                } else if ext == "jsonl"
                                    && previous_size > 0
                                    && (previous_size as usize) < content.len()
                                {
                                    &content[previous_size as usize..]
                                } else if source.track_cursor && previous_size > 0 {
                                    ""
                                } else {
                                    &content
                                };
                                if !new_content.is_empty() {
                                    let file_seen = seen_ids.entry(file_path.clone()).or_default();
                                    // In authoritative mode and on full re-read, emit cancellations for
                                    // job IDs that were in the previous snapshot but are not in the new one.
                                    if is_authoritative_reload {
                                        let new_ids = process_content_with_ids(
                                            source,
                                            new_content,
                                            &file_path,
                                            tx,
                                        )
                                        .await;
                                        let removed: Vec<String> =
                                            file_seen.difference(&new_ids).cloned().collect();
                                        for removed_id in &removed {
                                            file_seen.remove(removed_id);
                                        }
                                        emit_cancellations(source, &removed, tx);
                                        *file_seen = new_ids;
                                    } else {
                                        process_content(source, new_content, &file_path, tx).await;
                                        // For incremental reads, track IDs too (best effort from parse).
                                        let new_ids =
                                            extract_ids_from_content(new_content, &file_path);
                                        file_seen.extend(new_ids);
                                    }
                                }
                                directory_snapshot.insert(file_path, current_size);
                                if source.track_cursor {
                                    let _ =
                                        std::fs::write(file_cursor_path, current_size.to_string());
                                }
                            }
                        }
                    }
                    // Authoritative directory mode: cancel jobs from deleted files.
                    if source.authoritative {
                        let removed_files: Vec<PathBuf> = seen_ids
                            .keys()
                            .filter(|f| !current_files.contains(*f))
                            .cloned()
                            .collect();
                        for removed_file in &removed_files {
                            if let Some(ids) = seen_ids.remove(removed_file) {
                                let removed: Vec<String> = ids.into_iter().collect();
                                emit_cancellations(source, &removed, tx);
                            }
                        }
                    }
                } else if meta.is_file() {
                    let current_size = meta.len();
                    if current_size < last_size {
                        // File was truncated or replaced — full re-read.
                        last_size = 0;
                    }
                    if source.authoritative || current_size > last_size {
                        let content = std::fs::read_to_string(path)?;
                        let is_full_read = source.authoritative || last_size == 0;
                        let new_content = if source.authoritative {
                            &content
                        } else if last_size > 0 && (last_size as usize) < content.len() {
                            &content[last_size as usize..]
                        } else {
                            &content
                        };
                        if !new_content.is_empty() {
                            let file_seen = seen_ids.entry(path.clone()).or_default();
                            if source.authoritative && is_full_read {
                                let new_ids =
                                    process_content_with_ids(source, new_content, path, tx).await;
                                let removed: Vec<String> =
                                    file_seen.difference(&new_ids).cloned().collect();
                                for removed_id in &removed {
                                    file_seen.remove(removed_id);
                                }
                                emit_cancellations(source, &removed, tx);
                                *file_seen = new_ids;
                            } else {
                                process_content(source, new_content, path, tx).await;
                                let new_ids = extract_ids_from_content(new_content, path);
                                file_seen.extend(new_ids);
                            }
                            last_size = current_size;
                        }
                    }
                }
                if let Some(ref cursor) = cursor_path {
                    let _ = std::fs::write(cursor, last_size.to_string());
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("File source '{}' path {:?} not found", source.id, path);
            }
            Err(e) => {
                tracing::error!("File source '{}' metadata error: {e}", source.id);
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn process_content(
    source: &config::SourceConfig,
    content: &str,
    file_path: &std::path::Path,
    tx: &mpsc::UnboundedSender<Job>,
) {
    process_content_inner(source, content, file_path, tx, false).await;
}

/// Process content and return the set of job IDs parsed (for authoritative tracking).
async fn process_content_with_ids(
    source: &config::SourceConfig,
    content: &str,
    file_path: &std::path::Path,
    tx: &mpsc::UnboundedSender<Job>,
) -> std::collections::HashSet<String> {
    process_content_inner(source, content, file_path, tx, true)
        .await
        .unwrap_or_default()
}

/// Shared implementation for processing file content. When `collect_ids` is true,
/// the returned set contains the job IDs parsed from the content.
async fn process_content_inner(
    source: &config::SourceConfig,
    content: &str,
    file_path: &std::path::Path,
    tx: &mpsc::UnboundedSender<Job>,
    collect_ids: bool,
) -> Option<std::collections::HashSet<String>> {
    let mut ids = if collect_ids {
        Some(std::collections::HashSet::new())
    } else {
        None
    };
    let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match ext {
        "json" => {
            if let Ok(array) = serde_json::from_str::<Vec<serde_json::Value>>(content) {
                for item in array {
                    let job = create_job(source, &item);
                    if let Some(ref mut collected) = ids {
                        collected.insert(job.id.clone());
                    }
                    check_and_send_job(source, job, tx);
                }
            }
        }
        "jsonl" => {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(item) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    let job = create_job(source, &item);
                    if let Some(ref mut collected) = ids {
                        collected.insert(job.id.clone());
                    }
                    check_and_send_job(source, job, tx);
                }
            }
        }
        "csv" => {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(content.as_bytes());
            let hdrs = reader.headers().ok().cloned();
            for record in reader.records().flatten() {
                let mut map = serde_json::Map::new();
                if let Some(ref headers) = hdrs {
                    for (i, field) in record.iter().enumerate() {
                        let key = headers.get(i).unwrap_or("unknown").to_string();
                        map.insert(key, serde_json::Value::String(field.to_string()));
                    }
                }
                let job = create_job(source, &serde_json::Value::Object(map));
                if let Some(ref mut collected) = ids {
                    collected.insert(job.id.clone());
                }
                check_and_send_job(source, job, tx);
            }
        }
        _ => {}
    }
    ids
}

/// Extract job IDs from content without sending jobs (for incremental tracking).
fn extract_ids_from_content(
    content: &str,
    file_path: &std::path::Path,
) -> std::collections::HashSet<String> {
    let mut ids = std::collections::HashSet::new();
    let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match ext {
        "json" => {
            if let Ok(array) = serde_json::from_str::<Vec<serde_json::Value>>(content) {
                for item in array {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        ids.insert(id.to_string());
                    }
                }
            }
        }
        "jsonl" => {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(item) = serde_json::from_str::<serde_json::Value>(trimmed)
                    && let Some(id) = item.get("id").and_then(|v| v.as_str())
                {
                    ids.insert(id.to_string());
                }
            }
        }
        "csv" => {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .from_reader(content.as_bytes());
            let hdrs = reader.headers().ok().cloned();
            for record in reader.records().flatten() {
                if let Some(ref headers) = hdrs
                    && let Some(id_idx) = headers.iter().position(|h| h == "id")
                    && let Some(id_val) = record.get(id_idx)
                {
                    ids.insert(id_val.to_string());
                }
            }
        }
        _ => {}
    }
    ids
}

/// Send synthetic cancellation jobs for removed IDs.
fn emit_cancellations(
    source: &config::SourceConfig,
    ids: &[String],
    tx: &mpsc::UnboundedSender<Job>,
) {
    for target_id in ids {
        let cancel_job = Job {
            id: format!("__cancel__{}", target_id),
            source: source.id.clone(),
            payload: serde_json::json!({
                "__bria_cancel": true,
                "target_job_id": target_id,
            }),
            correlation_key: None,
            labels: source.labels.clone(),
        };
        tracing::info!(
            "File source '{}' authoritative: cancelling removed job '{}'",
            source.id,
            target_id
        );
        let _ = tx.send(cancel_job);
    }
}

/// Check if a job's serialized payload exceeds source.max_body_bytes; if so, log and skip.
/// Returns true if the job was sent, false if skipped.
fn check_and_send_job(
    source: &config::SourceConfig,
    job: Job,
    tx: &mpsc::UnboundedSender<Job>,
) -> bool {
    let max_bytes = if source.max_body_bytes > 0 {
        source.max_body_bytes
    } else {
        // Use the shared per-source default.
        config::default_max_body_bytes_val()
    };
    if let Ok(payload) = serde_json::to_vec(&job.payload)
        && payload.len() > max_bytes
    {
        tracing::warn!(
            "File source '{}' skipping job '{}': payload size {} exceeds source max_body_bytes {}",
            source.id,
            job.id,
            payload.len(),
            max_bytes
        );
        return false;
    }
    let _ = tx.send(job);
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Cron source
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "cron")]
pub async fn run_cron_source_inline(
    source: &config::SourceConfig,
    tx: &mpsc::UnboundedSender<Job>,
) -> Result<()> {
    let schedule = source
        .schedule
        .parse::<cron::Schedule>()
        .map_err(|e| Error::Cron(format!("Invalid cron schedule '{}': {e}", source.schedule)))?;
    let tz = source.tz.parse::<chrono_tz::Tz>().map_err(|e| {
        Error::Cron(format!(
            "Invalid timezone '{}' for cron source '{}': {e}",
            source.tz, source.id
        ))
    })?;
    loop {
        let now = chrono::Utc::now().with_timezone(&tz);
        if let Some(next) = schedule.upcoming(tz).next() {
            let until_next = (next - now)
                .to_std()
                .unwrap_or(std::time::Duration::from_secs(60));
            tokio::time::sleep(until_next).await;
            let job = Job {
                id: ulid::Ulid::r#gen().to_string(),
                source: source.id.clone(),
                payload: source.payload.clone(),
                correlation_key: None,
                labels: source.labels.clone(),
            };
            if tx.send(job).is_err() {
                break;
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Queue source (AMQP)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "amqp")]
pub async fn run_queue_source_inline(
    source: &config::SourceConfig,
    tx: &mpsc::UnboundedSender<Job>,
) -> Result<()> {
    loop {
        match run_queue_source_once(source, tx).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!(
                    "Queue source '{}' disconnected/error: {e}; reconnecting in {}s",
                    source.id,
                    source.reconnect_secs.max(1)
                );
                tokio::time::sleep(std::time::Duration::from_secs(source.reconnect_secs.max(1)))
                    .await;
            }
        }
    }
}

#[cfg(feature = "amqp")]
async fn run_queue_source_once(
    source: &config::SourceConfig,
    tx: &mpsc::UnboundedSender<Job>,
) -> Result<()> {
    use lapin::types::ShortString;

    let conn_url = amqp_url_with_credentials(&source.url, &source.username, &source.password)?;
    let conn = lapin::Connection::connect(&conn_url, lapin::ConnectionProperties::default())
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP connection error", e))?;

    let channel = conn
        .create_channel()
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP channel error", e))?;

    channel
        .exchange_declare(
            ShortString::from(source.exchange.as_str()),
            lapin::ExchangeKind::Topic,
            lapin::options::ExchangeDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP exchange error", e))?;

    let queue = channel
        .queue_declare(
            ShortString::from(""),
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP queue error", e))?;

    channel
        .queue_bind(
            queue.name().clone(),
            ShortString::from(source.exchange.as_str()),
            ShortString::from(source.submit_routing_key.as_str()),
            lapin::options::QueueBindOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP bind error", e))?;

    channel
        .queue_bind(
            queue.name().clone(),
            ShortString::from(source.exchange.as_str()),
            ShortString::from(source.cancel_routing_key.as_str()),
            lapin::options::QueueBindOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP cancel bind error", e))?;

    channel
        .basic_qos(
            source.qos_prefetch,
            lapin::options::BasicQosOptions::default(),
        )
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP QoS error", e))?;

    let consumer = channel
        .basic_consume(
            queue.name().clone(),
            ShortString::from(source.consumer_tag.as_str()),
            lapin::options::BasicConsumeOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|e| Error::source_err(&source.id, "AMQP consume error", e))?;

    let tx_clone = tx.clone();
    let source_clone = source.clone();
    let cancel_rk = source.cancel_routing_key.clone();
    consumer.set_delegate(move |delivery: lapin::message::DeliveryResult| {
        let tx = tx_clone.clone();
        let source = source_clone.clone();
        let cancel_rk = cancel_rk.clone();
        async move {
            match delivery {
                Ok(Some(delivery)) => {
                    let routing_key = delivery.routing_key.as_str();
                    if routing_key == cancel_rk.as_str() {
                        // Cancel delivery: parse target_job_id, job_id, or id from JSON body, or raw string body.
                        let body_str = String::from_utf8_lossy(&delivery.data);
                        let target_id = parse_cancel_target_id(&body_str);
                        if let Some(target_id) = target_id {
                            let cancel_job = Job {
                                id: format!("__cancel__{}", target_id),
                                source: source.id.clone(),
                                payload: serde_json::json!({
                                    "__bria_cancel": true,
                                    "target_job_id": target_id,
                                }),
                                correlation_key: None,
                                labels: source.labels.clone(),
                            };
                            let _ = tx.send(cancel_job);
                        } else {
                            tracing::warn!(
                                "Queue source '{}' cancel delivery had no identifiable target job id",
                                source.id
                            );
                        }
                    } else {
                        let payload: serde_json::Value =
                            serde_json::from_slice(&delivery.data).unwrap_or(serde_json::Value::Null);
                        let job = create_job(&source, &payload);
                        let _ = tx.send(job);
                    }
                    let _ = delivery
                        .ack(lapin::options::BasicAckOptions::default())
                        .await;
                }
                Ok(None) => {
                    tracing::info!("Queue source '{}' consumer cancelled", source.id);
                }
                Err(e) => {
                    tracing::error!("Queue source '{}' delivery error: {e}", source.id);
                }
            }
        }
    });

    let mut events = conn.events_listener();
    use tokio_stream::StreamExt;
    while let Some(event) = events.next().await {
        match event {
            lapin::Event::Error(e) => {
                return Err(Error::source_err(
                    &source.id,
                    "AMQP connection event error",
                    e,
                ));
            }
            lapin::Event::Connected => {
                tracing::info!("Queue source '{}' AMQP connection established", source.id);
            }
            lapin::Event::ConnectionBlocked(reason) => {
                tracing::warn!(
                    "Queue source '{}' AMQP connection blocked: {reason}",
                    source.id
                );
            }
            lapin::Event::ConnectionUnblocked => {
                tracing::info!("Queue source '{}' AMQP connection unblocked", source.id);
            }
            lapin::Event::SendFlow(active) => {
                tracing::debug!(
                    "Queue source '{}' AMQP send flow active={active}",
                    source.id
                );
            }
            _ => {
                tracing::debug!(
                    "Queue source '{}' received AMQP event: {event:?}",
                    source.id
                );
            }
        }
    }

    Err(Error::Source {
        source_id: source.id.clone(),
        message: "AMQP connection event stream ended".to_string(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// SQLite source
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "sqlite")]
pub async fn run_sqlite_source_inline(
    source: &config::SourceConfig,
    tx: &mpsc::UnboundedSender<Job>,
) -> Result<()> {
    let table = source.table.as_ref().ok_or_else(|| Error::Source {
        source_id: source.id.clone(),
        message: "SQLite source requires [sources.table] configuration".to_string(),
    })?;

    validate_identifier("table", &table.name)?;
    validate_identifier("column", &table.columns.id)?;
    validate_identifier("column", &table.columns.payload)?;
    validate_identifier("column", &table.columns.status)?;
    validate_identifier("column", &table.columns.created_at)?;

    let path_str = source.path.to_string_lossy();
    let database_url = format!("sqlite:{path_str}?mode=rwc");

    let pool = sqlx::SqlitePool::connect(&database_url)
        .await
        .map_err(|e| Error::source_err(&source.id, "SQLite source connect error", e))?;

    let poll_interval = std::time::Duration::from_secs(source.poll_interval_secs.max(1));

    // Quoted identifiers for safe interpolation (validated above, so safe)
    let tbl = quote_ident("table", &table.name)?;
    let col_id = quote_ident("column", &table.columns.id)?;
    let col_payload = quote_ident("column", &table.columns.payload)?;
    let col_status = quote_ident("column", &table.columns.status)?;
    let col_created_at = quote_ident("column", &table.columns.created_at)?;

    // Select rows that are not yet claimed, done, or failed.
    // A NULL or empty status means the row is fresh/pending.
    let select_sql = format!(
        "SELECT {col_id}, {col_payload} FROM {tbl} \
         WHERE {col_status} IS NULL \
            OR ({col_status} != ?1 AND {col_status} != ?2 AND {col_status} != ?3) \
         ORDER BY {col_created_at} ASC, {col_id} ASC \
         LIMIT 100"
    );

    let update_sql = format!("UPDATE {tbl} SET {col_status} = ?1 WHERE {col_id} = ?2");

    loop {
        let select_sql_arc = std::sync::Arc::new(select_sql.clone());
        let rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(sqlx::AssertSqlSafe(select_sql_arc))
            .bind(&table.columns.status_claimed_value)
            .bind(&table.columns.status_done_value)
            .bind(&table.columns.status_failed_value)
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::source_err(&source.id, "SQLite source select error", e))?;

        for row in &rows {
            let id: String = row.try_get::<String, _>(0).map_err(|e| {
                Error::source_err(&source.id, "SQLite source column 0 (id) error", e)
            })?;

            let payload_str: String = row.try_get::<String, _>(1).map_err(|e| {
                Error::source_err(&source.id, "SQLite source column 1 (payload) error", e)
            })?;

            // Claim the row
            let update_sql_arc = std::sync::Arc::new(update_sql.clone());
            sqlx::query(sqlx::AssertSqlSafe(update_sql_arc))
                .bind(&table.columns.status_claimed_value)
                .bind(&id)
                .execute(&pool)
                .await
                .map_err(|e| Error::source_err(&source.id, "SQLite source claim error", e))?;

            let payload: serde_json::Value = serde_json::from_str(&payload_str).map_err(|e| {
                Error::source_err(&source.id, "SQLite source payload parse error", e)
            })?;

            let job = create_job(source, &payload);
            let _ = tx.send(job);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PostgreSQL source
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "postgres")]
pub async fn run_pg_source_inline(
    source: &config::SourceConfig,
    tx: &mpsc::UnboundedSender<Job>,
) -> Result<()> {
    let table = source.table.as_ref().ok_or_else(|| Error::Source {
        source_id: source.id.clone(),
        message: "PG source requires [sources.table] configuration".to_string(),
    })?;

    validate_identifier("table", &table.name)?;
    validate_identifier("column", &table.columns.id)?;
    validate_identifier("column", &table.columns.payload)?;
    validate_identifier("column", &table.columns.status)?;
    validate_identifier("column", &table.columns.created_at)?;

    let pool = sqlx::PgPool::connect(&source.url)
        .await
        .map_err(|e| Error::source_err(&source.id, "PG source connect error", e))?;

    let poll_interval = std::time::Duration::from_secs(source.poll_interval_secs.max(1));

    // Quoted identifiers for safe interpolation (validated above, so safe)
    let tbl = quote_ident("table", &table.name)?;
    let col_id = quote_ident("column", &table.columns.id)?;
    let col_payload = quote_ident("column", &table.columns.payload)?;
    let col_status = quote_ident("column", &table.columns.status)?;
    let col_created_at = quote_ident("column", &table.columns.created_at)?;

    // Select rows that are not yet claimed, done, or failed.
    // Postgres: use IS DISTINCT FROM to handle NULL correctly.
    let select_sql = format!(
        "SELECT {col_id}, {col_payload} FROM {tbl} \
         WHERE {col_status} IS DISTINCT FROM $1 \
           AND {col_status} IS DISTINCT FROM $2 \
           AND {col_status} IS DISTINCT FROM $3 \
         ORDER BY {col_created_at} ASC, {col_id} ASC \
         LIMIT 100"
    );

    let update_sql = format!("UPDATE {tbl} SET {col_status} = $1 WHERE {col_id} = $2");

    loop {
        let select_sql_arc = std::sync::Arc::new(select_sql.clone());
        let rows: Vec<sqlx::postgres::PgRow> = sqlx::query(sqlx::AssertSqlSafe(select_sql_arc))
            .bind(&table.columns.status_claimed_value)
            .bind(&table.columns.status_done_value)
            .bind(&table.columns.status_failed_value)
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::source_err(&source.id, "PG source select error", e))?;

        for row in &rows {
            let id: String = row
                .try_get::<String, _>(0)
                .map_err(|e| Error::source_err(&source.id, "PG source column 0 (id) error", e))?;

            let payload_str: String = row.try_get::<String, _>(1).map_err(|e| {
                Error::source_err(&source.id, "PG source column 1 (payload) error", e)
            })?;

            // Claim the row
            let update_sql_arc = std::sync::Arc::new(update_sql.clone());
            sqlx::query(sqlx::AssertSqlSafe(update_sql_arc))
                .bind(&table.columns.status_claimed_value)
                .bind(&id)
                .execute(&pool)
                .await
                .map_err(|e| Error::source_err(&source.id, "PG source claim error", e))?;

            let payload: serde_json::Value = serde_json::from_str(&payload_str)
                .map_err(|e| Error::source_err(&source.id, "PG source payload parse error", e))?;

            let job = create_job(source, &payload);
            let _ = tx.send(job);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Parse a target job ID from a JSON string body for cancellation deliveries.
/// Tries `target_job_id`, `job_id`, then `id` field, or falls back to trimmed string body.
#[cfg(feature = "amqp")]
fn parse_cancel_target_id(body: &str) -> Option<String> {
    let trimmed = body.trim();
    // Try JSON parsing
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed)
        && let Some(id) = val
            .get("target_job_id")
            .or_else(|| val.get("job_id"))
            .or_else(|| val.get("id"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    {
        return Some(id.to_string());
    }
    // Fallback: use the raw string body as the ID if it looks like a simple identifier.
    if !trimmed.is_empty() && !trimmed.contains(char::is_whitespace) && !trimmed.contains('{') {
        return Some(trimmed.to_string());
    }
    None
}
