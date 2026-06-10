use std::collections::HashMap;

use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::Row;
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgPool, PgPoolOptions};
#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use crate::config::StateConfig;
use crate::context::Job;
use crate::error::Result;

// ─────────────────────────────────────────────────────────────────────────────
// Job state record
// ─────────────────────────────────────────────────────────────────────────────

/// A persisted record of a job's lifecycle state, used for crash recovery.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobStateRecord {
    pub job_id: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub correlation_key: Option<String>,
    pub pipeline_id: String,
    pub state: String,
    pub updated_at: String,
    /// Labels attached to the job (source + pipeline).
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// StateStore trait
// ─────────────────────────────────────────────────────────────────────────────

/// Persistence trait for the orchestrator job lifecycle.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Record that a job has been queued for a pipeline.
    async fn record_queued(&self, job: &Job, pipeline_id: &str) -> Result<()>;

    /// Record that a job has started executing.
    async fn record_running(&self, job: &Job, pipeline_id: &str) -> Result<()>;

    /// Record that a job has completed (status = "success" or "failure").
    async fn record_completed(&self, job_id: &str, pipeline_id: &str, status: &str) -> Result<()>;

    /// Return all jobs that were not completed, for crash recovery.
    async fn recover_incomplete(&self) -> Result<Vec<JobStateRecord>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

/// Create the state store configured in `[global.state]`.
pub async fn create_store(config: &StateConfig) -> Result<Box<dyn StateStore>> {
    match config.backend.as_str() {
        "memory" => Ok(Box::new(MemoryStore::new())),
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let store = SqliteStateStore::new(&config.sqlite_path).await?;
            Ok(Box::new(store))
        }
        #[cfg(not(feature = "sqlite"))]
        "sqlite" => Err(crate::error::Error::Unsupported(
            "State backend 'sqlite' requires the 'sqlite' feature".to_string(),
        )),
        #[cfg(feature = "postgres")]
        "pg" => {
            let store = PgStateStore::new(&config.pg_url).await?;
            Ok(Box::new(store))
        }
        #[cfg(not(feature = "postgres"))]
        "pg" => Err(crate::error::Error::Unsupported(
            "State backend 'pg' requires the 'postgres' feature".to_string(),
        )),
        other => Err(crate::error::Error::config(format!(
            "Unknown state backend: '{other}'. Supported: memory, sqlite, pg"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory backend
// ─────────────────────────────────────────────────────────────────────────────

pub struct MemoryStore {
    records: DashMap<(String, String), JobStateRecord>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            records: DashMap::new(),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn record_queued(&self, job: &Job, pipeline_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let record = JobStateRecord {
            job_id: job.id.clone(),
            source: job.source.clone(),
            payload: job.payload.clone(),
            correlation_key: job.correlation_key.clone(),
            pipeline_id: pipeline_id.to_string(),
            state: "queued".to_string(),
            updated_at: now,
            labels: job.labels.clone(),
        };
        self.records
            .insert((job.id.clone(), pipeline_id.to_string()), record);
        Ok(())
    }

    async fn record_running(&self, job: &Job, pipeline_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        if let Some(mut entry) = self
            .records
            .get_mut(&(job.id.clone(), pipeline_id.to_string()))
        {
            entry.state = "running".to_string();
            entry.updated_at = now;
        }
        Ok(())
    }

    async fn record_completed(&self, job_id: &str, pipeline_id: &str, status: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        if let Some(mut entry) = self
            .records
            .get_mut(&(job_id.to_string(), pipeline_id.to_string()))
        {
            entry.state = status.to_string();
            entry.updated_at = now;
        }
        Ok(())
    }

    async fn recover_incomplete(&self) -> Result<Vec<JobStateRecord>> {
        let records: Vec<JobStateRecord> = self
            .records
            .iter()
            .filter(|r| r.state == "queued" || r.state == "running")
            .map(|r| r.clone())
            .collect();
        Ok(records)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SQLite backend
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "sqlite")]
const CREATE_SQLITE_JOB_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS bria_job_state (
    job_id          TEXT NOT NULL,
    pipeline_id     TEXT NOT NULL,
    source          TEXT NOT NULL,
    payload         TEXT NOT NULL,
    correlation_key TEXT,
    state           TEXT NOT NULL DEFAULT 'queued',
    updated_at      TEXT NOT NULL,
    labels          TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (job_id, pipeline_id)
);
"#;

#[cfg(feature = "sqlite")]
pub struct SqliteStateStore {
    pool: SqlitePool,
}

#[cfg(feature = "sqlite")]
impl SqliteStateStore {
    pub async fn new(path: &str) -> Result<Self> {
        let conn_str = format!("sqlite:{path}?mode=rwc");
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&conn_str)
            .await?;

        sqlx::query(CREATE_SQLITE_JOB_STATE).execute(&pool).await?;

        Ok(Self { pool })
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl StateStore for SqliteStateStore {
    async fn record_queued(&self, job: &Job, pipeline_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let payload = serde_json::to_string(&job.payload)?;
        let labels = serde_json::to_string(&job.labels).unwrap_or_default();

        sqlx::query(
            "INSERT INTO bria_job_state \
                 (job_id, pipeline_id, source, payload, correlation_key, state, updated_at, labels) \
             VALUES (?, ?, ?, ?, ?, 'queued', ?, ?) \
             ON CONFLICT (job_id, pipeline_id) DO UPDATE SET \
                 source = EXCLUDED.source, \
                 payload = EXCLUDED.payload, \
                 correlation_key = EXCLUDED.correlation_key, \
                 state = 'queued', \
                 updated_at = EXCLUDED.updated_at, \
                 labels = EXCLUDED.labels",
        )
        .bind(&job.id)
        .bind(pipeline_id)
        .bind(&job.source)
        .bind(&payload)
        .bind(&job.correlation_key)
        .bind(&now)
        .bind(&labels)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn record_running(&self, job: &Job, pipeline_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE bria_job_state \
             SET state = 'running', updated_at = ? \
             WHERE job_id = ? AND pipeline_id = ?",
        )
        .bind(&now)
        .bind(&job.id)
        .bind(pipeline_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn record_completed(&self, job_id: &str, pipeline_id: &str, status: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE bria_job_state \
             SET state = ?, updated_at = ? \
             WHERE job_id = ? AND pipeline_id = ?",
        )
        .bind(status)
        .bind(&now)
        .bind(job_id)
        .bind(pipeline_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn recover_incomplete(&self) -> Result<Vec<JobStateRecord>> {
        let rows = sqlx::query(
            "SELECT job_id, source, payload, correlation_key, pipeline_id, state, updated_at, labels \
             FROM bria_job_state \
             WHERE state IN ('queued', 'running')",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let payload_str: String = row.get("payload");
            let payload: serde_json::Value = serde_json::from_str(&payload_str)?;

            let labels_str: Option<String> = row.try_get("labels").ok();
            let labels: HashMap<String, String> = labels_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();

            records.push(JobStateRecord {
                job_id: row.get("job_id"),
                source: row.get("source"),
                pipeline_id: row.get("pipeline_id"),
                payload,
                correlation_key: row.get::<Option<String>, _>("correlation_key"),
                state: row.get("state"),
                updated_at: row.get("updated_at"),
                labels,
            });
        }

        Ok(records)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PostgreSQL backend
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "postgres")]
const CREATE_PG_JOB_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS bria_job_state (
    job_id          TEXT NOT NULL,
    pipeline_id     TEXT NOT NULL,
    source          TEXT NOT NULL,
    payload         TEXT NOT NULL,
    correlation_key TEXT,
    state           TEXT NOT NULL DEFAULT 'queued',
    updated_at      TEXT NOT NULL,
    labels          TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (job_id, pipeline_id)
);
"#;

#[cfg(feature = "postgres")]
pub struct PgStateStore {
    pool: PgPool,
}

#[cfg(feature = "postgres")]
impl PgStateStore {
    pub async fn new(url: &str) -> Result<Self> {
        if url.is_empty() {
            return Err(crate::error::Error::config(
                "pg_url is required when state backend is 'pg'",
            ));
        }

        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;

        sqlx::query(CREATE_PG_JOB_STATE).execute(&pool).await?;

        Ok(Self { pool })
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl StateStore for PgStateStore {
    async fn record_queued(&self, job: &Job, pipeline_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let payload = serde_json::to_string(&job.payload)?;
        let labels = serde_json::to_string(&job.labels).unwrap_or_default();

        sqlx::query(
            "INSERT INTO bria_job_state \
                 (job_id, pipeline_id, source, payload, correlation_key, state, updated_at, labels) \
             VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7) \
             ON CONFLICT (job_id, pipeline_id) DO UPDATE SET \
                 source = EXCLUDED.source, \
                 payload = EXCLUDED.payload, \
                 correlation_key = EXCLUDED.correlation_key, \
                 state = 'queued', \
                 updated_at = EXCLUDED.updated_at, \
                 labels = EXCLUDED.labels",
        )
        .bind(&job.id)
        .bind(pipeline_id)
        .bind(&job.source)
        .bind(&payload)
        .bind(&job.correlation_key)
        .bind(&now)
        .bind(&labels)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn record_running(&self, job: &Job, pipeline_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE bria_job_state \
             SET state = 'running', updated_at = $1 \
             WHERE job_id = $2 AND pipeline_id = $3",
        )
        .bind(&now)
        .bind(&job.id)
        .bind(pipeline_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn record_completed(&self, job_id: &str, pipeline_id: &str, status: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE bria_job_state \
             SET state = $1, updated_at = $2 \
             WHERE job_id = $3 AND pipeline_id = $4",
        )
        .bind(status)
        .bind(&now)
        .bind(job_id)
        .bind(pipeline_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn recover_incomplete(&self) -> Result<Vec<JobStateRecord>> {
        let rows = sqlx::query(
            "SELECT job_id, source, payload, correlation_key, pipeline_id, state, updated_at, labels \
             FROM bria_job_state \
             WHERE state IN ('queued', 'running')",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let payload_str: String = row.get("payload");
            let payload: serde_json::Value = serde_json::from_str(&payload_str)?;

            let labels_str: Option<String> = row.try_get("labels").ok();
            let labels: HashMap<String, String> = labels_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();

            records.push(JobStateRecord {
                job_id: row.get("job_id"),
                source: row.get("source"),
                pipeline_id: row.get("pipeline_id"),
                payload,
                correlation_key: row.get::<Option<String>, _>("correlation_key"),
                state: row.get("state"),
                updated_at: row.get("updated_at"),
                labels,
            });
        }

        Ok(records)
    }
}
