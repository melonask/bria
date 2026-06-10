use std::io::Write as _;
use std::path::PathBuf;

#[cfg(any(feature = "amqp", feature = "sqlite", feature = "postgres"))]
use std::collections::HashMap;
#[cfg(any(feature = "amqp", feature = "sqlite", feature = "postgres"))]
use std::sync::Arc;

use crate::config;
use crate::context::{Context, PipelineResult, StepResult};
use crate::error::{Error, Result};
use crate::template::TemplateEngine;
#[cfg(feature = "amqp")]
use crate::util::amqp_url_with_credentials;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::util::{quote_ident, validate_identifier};

#[cfg(feature = "amqp")]
struct AmqpSinkClient {
    _connection: lapin::Connection,
    channel: lapin::Channel,
}

/// Result sink dispatcher — routes pipeline/step results to configured sinks.
pub struct SinkDispatcher {
    config: crate::config::Config,
    template: TemplateEngine,
    #[cfg(feature = "webhook")]
    http_client: reqwest::Client,
    #[cfg(feature = "sqlite")]
    sqlite_pools: tokio::sync::Mutex<HashMap<String, sqlx::SqlitePool>>,
    #[cfg(feature = "postgres")]
    pg_pools: tokio::sync::Mutex<HashMap<String, sqlx::PgPool>>,
    #[cfg(feature = "amqp")]
    amqp_clients: tokio::sync::Mutex<HashMap<String, Arc<AmqpSinkClient>>>,
    /// Broadcast channel for stream sink (server push).
    broadcast_tx: Option<tokio::sync::broadcast::Sender<serde_json::Value>>,
}

impl SinkDispatcher {
    pub fn new(
        config: crate::config::Config,
        template: TemplateEngine,
        broadcast_tx: Option<tokio::sync::broadcast::Sender<serde_json::Value>>,
    ) -> Self {
        Self {
            config,
            template,
            #[cfg(feature = "webhook")]
            http_client: reqwest::Client::new(),
            #[cfg(feature = "sqlite")]
            sqlite_pools: tokio::sync::Mutex::new(HashMap::new()),
            #[cfg(feature = "postgres")]
            pg_pools: tokio::sync::Mutex::new(HashMap::new()),
            #[cfg(feature = "amqp")]
            amqp_clients: tokio::sync::Mutex::new(HashMap::new()),
            broadcast_tx,
        }
    }

    /// Send pipeline result to all configured pipeline-level sinks.
    pub async fn send_pipeline_result(&self, result: &PipelineResult, ctx: &Context) {
        let pipeline = self
            .config
            .pipelines
            .iter()
            .find(|p| p.id == result.pipeline_id);

        let Some(pipeline) = pipeline else {
            return;
        };

        // ── Sink precedence per step: routing → step.sinks → pipeline-level ──

        // Collect step IDs that have been handled by routing or step.sinks.
        // These steps will NOT be included in the pipeline-level full send.
        let mut handled: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Pass 1: step routing rules have highest precedence.
        for step in &pipeline.steps {
            if let Some(step_result) = result.steps.get(&step.id)
                && !step.routing.is_empty()
            {
                let evaluator = crate::expression::Evaluator::with_pipeline_id(&result.pipeline_id);
                let mut routed = false;
                for route in &step.routing {
                    match evaluator.eval_bool(&route.condition, ctx) {
                        Ok(true) => {
                            for sink_id in &route.sinks {
                                if let Some(sink) = self.config.get_sink(sink_id) {
                                    let step_msg = make_step_synthetic(result, step, step_result);
                                    if let Err(e) = self.send_to_sink(sink, &step_msg, ctx).await {
                                        log_sink_error(sink, &e);
                                    }
                                }
                            }
                            routed = true;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::warn!("Step '{}' routing condition error: {e}", step.id);
                        }
                    }
                }
                if routed {
                    handled.insert(step.id.clone());
                }
            }
        }

        // Pass 2: step.sinks — only for steps not already handled by routing.
        for step in &pipeline.steps {
            if handled.contains(&step.id) {
                continue;
            }
            if !step.sinks.is_empty()
                && let Some(step_result) = result.steps.get(&step.id)
            {
                for sink_id in &step.sinks {
                    if let Some(sink) = self.config.get_sink(sink_id) {
                        let step_msg = make_step_synthetic(result, step, step_result);
                        if let Err(e) = self.send_to_sink(sink, &step_msg, ctx).await {
                            log_sink_error(sink, &e);
                        }
                    }
                }
                handled.insert(step.id.clone());
            }
        }

        // Pass 3: pipeline-level sinks — only for unhandled steps.
        let remaining_steps: std::collections::HashMap<String, StepResult> = result
            .steps
            .iter()
            .filter(|(id, _)| !handled.contains(*id))
            .map(|(id, r)| (id.clone(), r.clone()))
            .collect();

        if !remaining_steps.is_empty() {
            for sink_id in &pipeline.sinks {
                if let Some(sink) = self.config.get_sink(sink_id) {
                    let pipeline_msg = PipelineResult {
                        pipeline_id: result.pipeline_id.clone(),
                        job: result.job.clone(),
                        status: result.status.clone(),
                        duration_ms: result.duration_ms,
                        steps: remaining_steps.clone(),
                        occurred_at: result.occurred_at.clone(),
                    };
                    if let Err(e) = self.send_to_sink(sink, &pipeline_msg, ctx).await {
                        log_sink_error(sink, &e);
                    }
                }
            }
        }

        // ── Failure dead-letter: always sent ──
        if result.status == "failure"
            && pipeline.failure.action == config::FailureAction::DeadLetter
            && let Some(ref sink_id) = pipeline.failure.sink
            && let Some(sink) = self.config.get_sink(sink_id)
            && let Err(e) = self.send_to_sink(sink, result, ctx).await
        {
            log_sink_error(sink, &e);
        }

        // Step-level failure dead-letter sinks are independent of the pipeline-level
        // failure action. They receive a synthetic single-step result for each failed
        // step configured with `failure.action = "dead_letter"`.
        if result.status == "failure" {
            for step in &pipeline.steps {
                if step.failure.action != config::FailureAction::DeadLetter {
                    continue;
                }
                let Some(step_result) = result.steps.get(&step.id) else {
                    continue;
                };
                if step_result.exit_code == 0 {
                    continue;
                }
                let Some(ref sink_id) = step.failure.sink else {
                    continue;
                };
                if let Some(sink) = self.config.get_sink(sink_id) {
                    let step_msg = make_step_synthetic(result, step, step_result);
                    if let Err(e) = self.send_to_sink(sink, &step_msg, ctx).await {
                        log_sink_error(sink, &e);
                    }
                }
            }
        }
    }

    /// Send a result to a specific sink.
    async fn send_to_sink(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        ctx: &Context,
    ) -> Result<()> {
        match sink.r#type {
            config::SinkType::File => self.send_to_file(sink, result, ctx).await,
            #[cfg(feature = "webhook")]
            config::SinkType::Webhook => self.send_to_webhook(sink, result, ctx).await,
            #[cfg(not(feature = "webhook"))]
            config::SinkType::Webhook => Err(Error::Unsupported(
                "Sink type 'webhook' requires the 'webhook' feature".to_string(),
            )),
            #[cfg(feature = "sqlite")]
            config::SinkType::Sqlite => self.send_to_sqlite(sink, result, ctx).await,
            #[cfg(not(feature = "sqlite"))]
            config::SinkType::Sqlite => Err(Error::Unsupported(
                "Sink type 'sqlite' requires the 'sqlite' feature".to_string(),
            )),
            config::SinkType::Stream => self.send_to_stream(sink, result, ctx).await,
            #[cfg(feature = "amqp")]
            config::SinkType::Queue => self.send_to_queue(sink, result, ctx).await,
            #[cfg(not(feature = "amqp"))]
            config::SinkType::Queue => Err(Error::Unsupported(
                "Sink type 'queue' requires the 'amqp' feature".to_string(),
            )),
            #[cfg(feature = "postgres")]
            config::SinkType::Pg => self.send_to_pg(sink, result, ctx).await,
            #[cfg(not(feature = "postgres"))]
            config::SinkType::Pg => Err(Error::Unsupported(
                "Sink type 'pg' requires the 'postgres' feature".to_string(),
            )),
        }
    }

    async fn send_to_file(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        ctx: &Context,
    ) -> Result<()> {
        // Render sink.path as a template
        let path_str = self.template.render_result(
            &sink.path,
            ctx,
            &result.pipeline_id,
            &result.status,
            result.duration_ms,
            &result.occurred_at,
        )?;
        let path = PathBuf::from(&path_str);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let line = if let Some(ref tpl) = sink.template {
            self.template.render_result(
                tpl,
                ctx,
                &result.pipeline_id,
                &result.status,
                result.duration_ms,
                &result.occurred_at,
            )?
        } else {
            serde_json::to_string(result)?
        };

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        writeln!(file, "{line}")?;

        Ok(())
    }

    #[cfg(feature = "webhook")]
    async fn send_to_webhook(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        ctx: &Context,
    ) -> Result<()> {
        let body_string = serde_json::to_string(result)?;
        let body = body_string.as_bytes().to_vec();

        // Render sink.url as a template
        let rendered_url = self.template.render_result(
            &sink.url,
            ctx,
            &result.pipeline_id,
            &result.status,
            result.duration_ms,
            &result.occurred_at,
        )?;

        let mut rendered_headers = Vec::with_capacity(sink.headers.len());
        for (key, value) in &sink.headers {
            let rendered = self.template.render_result(
                value,
                ctx,
                &result.pipeline_id,
                &result.status,
                result.duration_ms,
                &result.occurred_at,
            )?;
            rendered_headers.push((key.clone(), rendered));
        }

        let signature = (!sink.secret.is_empty()).then(|| compute_hmac(&sink.secret, &body_string));

        let mut last_error = None;
        for attempt in 0..=sink.max_retries {
            let mut request = self
                .http_client
                .post(&rendered_url)
                .header("Content-Type", &sink.content_type)
                .body(body.clone());

            for (key, value) in &rendered_headers {
                request = request.header(key.as_str(), value.as_str());
            }

            if let Some(signature) = &signature {
                request = request.header(&sink.signature_header, signature.as_str());
            }

            match tokio::time::timeout(
                std::time::Duration::from_secs(sink.timeout_secs),
                request.send(),
            )
            .await
            {
                Ok(Ok(response)) => {
                    if response.status().is_success() {
                        return Ok(());
                    }
                    last_error = Some(Error::Sink {
                        sink_id: sink.id.clone(),
                        message: format!("Webhook returned status {}", response.status()),
                    });
                }
                Ok(Err(e)) => {
                    last_error = Some(Error::sink_err(&sink.id, "Webhook error", e));
                }
                Err(_) => {
                    last_error = Some(Error::Sink {
                        sink_id: sink.id.clone(),
                        message: "Webhook request timed out".to_string(),
                    });
                }
            }

            if attempt < sink.max_retries {
                let delay = sink.retry_base_ms * 2u64.pow(attempt);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Sink {
            sink_id: sink.id.clone(),
            message: "Webhook sink failed all retries".to_string(),
        }))
    }

    #[cfg(feature = "sqlite")]
    async fn get_sqlite_pool(
        &self,
        sink: &config::SinkConfig,
        table: &config::TableSinkConfig,
    ) -> Result<sqlx::SqlitePool> {
        let mut pools = self.sqlite_pools.lock().await;
        if let Some(pool) = pools.get(&sink.id) {
            return Ok(pool.clone());
        }

        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}?mode=rwc", sink.path))
            .await
            .map_err(|e| Error::sink_err(&sink.id, "SQLite connect error", e))?;
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await
            .map_err(|e| Error::sink_err(&sink.id, "SQLite WAL setup error", e))?;
        sqlx::query("PRAGMA busy_timeout = 5000")
            .execute(&pool)
            .await
            .map_err(|e| Error::sink_err(&sink.id, "SQLite busy-timeout setup error", e))?;
        create_sqlite_result_table(sink, table, &pool).await?;
        pools.insert(sink.id.clone(), pool.clone());
        Ok(pool)
    }

    #[cfg(feature = "postgres")]
    async fn get_pg_pool(
        &self,
        sink: &config::SinkConfig,
        table: &config::TableSinkConfig,
    ) -> Result<sqlx::PgPool> {
        let mut pools = self.pg_pools.lock().await;
        if let Some(pool) = pools.get(&sink.id) {
            return Ok(pool.clone());
        }

        let pool = sqlx::PgPool::connect(&sink.url)
            .await
            .map_err(|e| Error::sink_err(&sink.id, "PG connect error", e))?;
        create_pg_result_table(sink, table, &pool).await?;
        pools.insert(sink.id.clone(), pool.clone());
        Ok(pool)
    }

    #[cfg(feature = "sqlite")]
    async fn send_to_sqlite(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        _ctx: &Context,
    ) -> Result<()> {
        let table = sink.table.as_ref().ok_or_else(|| Error::Sink {
            sink_id: sink.id.clone(),
            message: "SQLite sink requires [sinks.table] configuration".to_string(),
        })?;

        let pool = self.get_sqlite_pool(sink, table).await?;

        let tbl = quote_ident("table", &table.name)?;
        let col_result_id = quote_ident("column", &table.columns.result_id)?;
        let col_job_id = quote_ident("column", &table.columns.job_id)?;
        let col_pipeline_id = quote_ident("column", &table.columns.pipeline_id)?;
        let col_step_id = quote_ident("column", &table.columns.step_id)?;
        let col_occurred_at = quote_ident("column", &table.columns.occurred_at)?;
        let col_exit_code = quote_ident("column", &table.columns.exit_code)?;
        let col_stdout = quote_ident("column", &table.columns.stdout)?;
        let col_stderr = quote_ident("column", &table.columns.stderr)?;
        let col_duration_ms = quote_ident("column", &table.columns.duration_ms)?;
        let col_attempt = quote_ident("column", &table.columns.attempt)?;
        let col_status = quote_ident("column", &table.columns.status)?;

        let insert_sql = format!(
            "INSERT INTO {tbl} (\
             {col_result_id}, {col_job_id}, {col_pipeline_id}, {col_step_id}, \
             {col_occurred_at}, {col_exit_code}, {col_stdout}, {col_stderr}, \
             {col_duration_ms}, {col_attempt}, {col_status}\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        let insert_sql_arc = std::sync::Arc::new(insert_sql);
        for (step_id, step_result) in &result.steps {
            let result_id = ulid::Ulid::new().to_string();
            sqlx::query(sqlx::AssertSqlSafe(insert_sql_arc.clone()))
                .bind(&result_id)
                .bind(&result.job.id)
                .bind(&result.pipeline_id)
                .bind(step_id)
                .bind(&result.occurred_at)
                .bind(step_result.exit_code)
                .bind(step_result.stdout.as_deref().unwrap_or(""))
                .bind(step_result.stderr.as_deref().unwrap_or(""))
                .bind(step_result.duration_ms as i64)
                .bind(step_result.attempt as i64)
                .bind(&result.status)
                .execute(&pool)
                .await
                .map_err(|e| Error::sink_err(&sink.id, "SQLite insert error", e))?;
        }

        Ok(())
    }

    async fn send_to_stream(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        _ctx: &Context,
    ) -> Result<()> {
        if let Some(ref tx) = self.broadcast_tx {
            let value = serde_json::to_value(result)
                .map_err(|e| Error::sink_err(&sink.id, "Serialization error", e))?;
            let _ = tx.send(value);
        } else {
            tracing::warn!(
                "Stream sink '{}' has no broadcast channel (server may not be enabled)",
                sink.id
            );
        }
        Ok(())
    }

    #[cfg(feature = "amqp")]
    async fn get_amqp_channel(&self, sink: &config::SinkConfig) -> Result<lapin::Channel> {
        let mut clients = self.amqp_clients.lock().await;
        if let Some(client) = clients.get(&sink.id) {
            return Ok(client.channel.clone());
        }

        let conn_url = amqp_url_with_credentials(&sink.url, &sink.username, &sink.password)?;
        let connection =
            lapin::Connection::connect(&conn_url, lapin::ConnectionProperties::default())
                .await
                .map_err(|e| Error::sink_err(&sink.id, "AMQP connection error", e))?;

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| Error::sink_err(&sink.id, "AMQP channel error", e))?;

        channel
            .exchange_declare(
                lapin::types::ShortString::from(sink.exchange.as_str()),
                lapin::ExchangeKind::Topic,
                lapin::options::ExchangeDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .map_err(|e| Error::sink_err(&sink.id, "AMQP exchange error", e))?;

        clients.insert(
            sink.id.clone(),
            Arc::new(AmqpSinkClient {
                _connection: connection,
                channel: channel.clone(),
            }),
        );

        Ok(channel)
    }

    #[cfg(feature = "amqp")]
    async fn send_to_queue(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        ctx: &Context,
    ) -> Result<()> {
        use lapin::types::ShortString;

        let max_retries = sink.max_retries.max(1);

        let routing_key_template = if result.status == "success" {
            &sink.success_routing_key
        } else {
            &sink.failure_routing_key
        };
        let routing_key_rendered = self.template.render_result(
            routing_key_template,
            ctx,
            &result.pipeline_id,
            &result.status,
            result.duration_ms,
            &result.occurred_at,
        )?;
        let routing_key = ShortString::from(routing_key_rendered.as_str());

        let body = serde_json::to_vec(result)
            .map_err(|e| Error::sink_err(&sink.id, "Serialization error", e))?;

        let mut last_error: Option<Error> = None;

        for attempt in 0..max_retries {
            if attempt > 0 {
                let delay_secs = sink
                    .reconnect_secs
                    .max(1)
                    .saturating_mul(2u64.saturating_pow((attempt - 1).min(5)));
                tracing::warn!(
                    sink_id = %sink.id,
                    attempt = attempt + 1,
                    max_retries,
                    delay_secs,
                    "Retrying AMQP sink publish after reconnect backoff"
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
            }

            let channel = match self.get_amqp_channel(sink).await {
                Ok(channel) => channel,
                Err(e) => {
                    last_error = Some(e);
                    self.amqp_clients.lock().await.remove(&sink.id);
                    continue;
                }
            };

            match channel
                .basic_publish(
                    ShortString::from(sink.exchange.as_str()),
                    routing_key.clone(),
                    lapin::options::BasicPublishOptions::default(),
                    &body,
                    lapin::BasicProperties::default(),
                )
                .await
            {
                Ok(confirm) => match confirm.await {
                    Ok(_confirmation) => return Ok(()),
                    Err(e) => {
                        self.amqp_clients.lock().await.remove(&sink.id);
                        last_error =
                            Some(Error::sink_err(&sink.id, "AMQP publish confirm error", e));
                    }
                },
                Err(e) => {
                    self.amqp_clients.lock().await.remove(&sink.id);
                    last_error = Some(Error::sink_err(&sink.id, "AMQP publish error", e));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Sink {
            sink_id: sink.id.clone(),
            message: "Queue sink failed all retries".to_string(),
        }))
    }

    #[cfg(feature = "postgres")]
    async fn send_to_pg(
        &self,
        sink: &config::SinkConfig,
        result: &PipelineResult,
        _ctx: &Context,
    ) -> Result<()> {
        let table = sink.table.as_ref().ok_or_else(|| Error::Sink {
            sink_id: sink.id.clone(),
            message: "PG sink requires [sinks.table] configuration".to_string(),
        })?;

        let pool = self.get_pg_pool(sink, table).await?;

        // Safe-quoted identifiers (validated via sink config validation; we trust the config)
        let tbl = quote_ident("table", &table.name)?;
        let col_result_id = quote_ident("column", &table.columns.result_id)?;
        let col_job_id = quote_ident("column", &table.columns.job_id)?;
        let col_pipeline_id = quote_ident("column", &table.columns.pipeline_id)?;
        let col_step_id = quote_ident("column", &table.columns.step_id)?;
        let col_occurred_at = quote_ident("column", &table.columns.occurred_at)?;
        let col_exit_code = quote_ident("column", &table.columns.exit_code)?;
        let col_stdout = quote_ident("column", &table.columns.stdout)?;
        let col_stderr = quote_ident("column", &table.columns.stderr)?;
        let col_duration_ms = quote_ident("column", &table.columns.duration_ms)?;
        let col_attempt = quote_ident("column", &table.columns.attempt)?;
        let col_status = quote_ident("column", &table.columns.status)?;

        let insert_sql = format!(
            "INSERT INTO {tbl} (\
             {col_result_id}, {col_job_id}, {col_pipeline_id}, {col_step_id}, \
             {col_occurred_at}, {col_exit_code}, {col_stdout}, {col_stderr}, \
             {col_duration_ms}, {col_attempt}, {col_status}\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
        );

        let insert_sql_arc = std::sync::Arc::new(insert_sql);
        for (step_id, step_result) in &result.steps {
            let result_id = ulid::Ulid::new().to_string();
            sqlx::query(sqlx::AssertSqlSafe(insert_sql_arc.clone()))
                .bind(&result_id)
                .bind(&result.job.id)
                .bind(&result.pipeline_id)
                .bind(step_id)
                .bind(&result.occurred_at)
                .bind(step_result.exit_code)
                .bind(step_result.stdout.as_deref().unwrap_or(""))
                .bind(step_result.stderr.as_deref().unwrap_or(""))
                .bind(step_result.duration_ms as i64)
                .bind(step_result.attempt as i32)
                .bind(&result.status)
                .execute(&pool)
                .await
                .map_err(|e| Error::sink_err(&sink.id, "PG insert error", e))?;
        }

        Ok(())
    }
}

/// Build a synthetic single-step PipelineResult for step-level sink dispatch.
fn make_step_synthetic(
    result: &PipelineResult,
    step: &config::StepConfig,
    step_result: &StepResult,
) -> PipelineResult {
    let mut steps = std::collections::HashMap::new();
    steps.insert(step.id.clone(), step_result.clone());
    PipelineResult {
        pipeline_id: result.pipeline_id.clone(),
        job: result.job.clone(),
        status: if step_result.exit_code == 0 {
            "success".to_string()
        } else {
            "failure".to_string()
        },
        duration_ms: step_result.duration_ms,
        steps,
        occurred_at: chrono::Utc::now().to_rfc3339(),
    }
}

#[cfg(feature = "sqlite")]
async fn create_sqlite_result_table(
    sink: &config::SinkConfig,
    table: &config::TableSinkConfig,
    pool: &sqlx::SqlitePool,
) -> Result<()> {
    validate_table_sink_identifiers(table)
        .map_err(|e| Error::sink_err(&sink.id, "SQLite sink identifier error", e))?;
    let tbl = quote_ident("table", &table.name)?;
    let col_result_id = quote_ident("column", &table.columns.result_id)?;
    let col_job_id = quote_ident("column", &table.columns.job_id)?;
    let col_pipeline_id = quote_ident("column", &table.columns.pipeline_id)?;
    let col_step_id = quote_ident("column", &table.columns.step_id)?;
    let col_occurred_at = quote_ident("column", &table.columns.occurred_at)?;
    let col_exit_code = quote_ident("column", &table.columns.exit_code)?;
    let col_stdout = quote_ident("column", &table.columns.stdout)?;
    let col_stderr = quote_ident("column", &table.columns.stderr)?;
    let col_duration_ms = quote_ident("column", &table.columns.duration_ms)?;
    let col_attempt = quote_ident("column", &table.columns.attempt)?;
    let col_status = quote_ident("column", &table.columns.status)?;

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {tbl} (\
         {col_result_id} TEXT PRIMARY KEY, \
         {col_job_id} TEXT NOT NULL, \
         {col_pipeline_id} TEXT NOT NULL, \
         {col_step_id} TEXT NOT NULL, \
         {col_occurred_at} TEXT NOT NULL, \
         {col_exit_code} INTEGER NOT NULL, \
         {col_stdout} TEXT, \
         {col_stderr} TEXT, \
         {col_duration_ms} INTEGER NOT NULL, \
         {col_attempt} INTEGER NOT NULL, \
         {col_status} TEXT NOT NULL\
         )"
    );

    sqlx::query(sqlx::AssertSqlSafe(Arc::new(create_sql)))
        .execute(pool)
        .await
        .map_err(|e| Error::sink_err(&sink.id, "SQLite create table error", e))?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn create_pg_result_table(
    sink: &config::SinkConfig,
    table: &config::TableSinkConfig,
    pool: &sqlx::PgPool,
) -> Result<()> {
    validate_table_sink_identifiers(table)
        .map_err(|e| Error::sink_err(&sink.id, "PG sink identifier error", e))?;
    let tbl = quote_ident("table", &table.name)?;
    let col_result_id = quote_ident("column", &table.columns.result_id)?;
    let col_job_id = quote_ident("column", &table.columns.job_id)?;
    let col_pipeline_id = quote_ident("column", &table.columns.pipeline_id)?;
    let col_step_id = quote_ident("column", &table.columns.step_id)?;
    let col_occurred_at = quote_ident("column", &table.columns.occurred_at)?;
    let col_exit_code = quote_ident("column", &table.columns.exit_code)?;
    let col_stdout = quote_ident("column", &table.columns.stdout)?;
    let col_stderr = quote_ident("column", &table.columns.stderr)?;
    let col_duration_ms = quote_ident("column", &table.columns.duration_ms)?;
    let col_attempt = quote_ident("column", &table.columns.attempt)?;
    let col_status = quote_ident("column", &table.columns.status)?;

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {tbl} (\
         {col_result_id} TEXT PRIMARY KEY, \
         {col_job_id} TEXT NOT NULL, \
         {col_pipeline_id} TEXT NOT NULL, \
         {col_step_id} TEXT NOT NULL, \
         {col_occurred_at} TEXT NOT NULL, \
         {col_exit_code} INTEGER NOT NULL, \
         {col_stdout} TEXT, \
         {col_stderr} TEXT, \
         {col_duration_ms} BIGINT NOT NULL, \
         {col_attempt} INTEGER NOT NULL, \
         {col_status} TEXT NOT NULL\
         )"
    );

    sqlx::query(sqlx::AssertSqlSafe(Arc::new(create_sql)))
        .execute(pool)
        .await
        .map_err(|e| Error::sink_err(&sink.id, "PG create table error", e))?;
    Ok(())
}

#[cfg(feature = "webhook")]
fn compute_hmac(secret: &str, message: &str) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

fn log_sink_error(sink: &config::SinkConfig, error: &Error) {
    tracing::error!(
        sink_id = %sink.id,
        sink_type = ?sink.r#type,
        error = %error,
        "Sink dispatch failed"
    );
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn validate_table_sink_identifiers(table: &config::TableSinkConfig) -> Result<()> {
    validate_identifier("table", &table.name)?;
    validate_identifier("column", &table.columns.result_id)?;
    validate_identifier("column", &table.columns.job_id)?;
    validate_identifier("column", &table.columns.pipeline_id)?;
    validate_identifier("column", &table.columns.step_id)?;
    validate_identifier("column", &table.columns.occurred_at)?;
    validate_identifier("column", &table.columns.exit_code)?;
    validate_identifier("column", &table.columns.stdout)?;
    validate_identifier("column", &table.columns.stderr)?;
    validate_identifier("column", &table.columns.duration_ms)?;
    validate_identifier("column", &table.columns.attempt)?;
    validate_identifier("column", &table.columns.status)?;
    Ok(())
}
