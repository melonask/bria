use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::context::{Context, Job};
use crate::error::{Error, Result};
use crate::expression::Evaluator;
use crate::pipeline;
use crate::sinks::SinkDispatcher;
use crate::state::StateStore;
use crate::template::TemplateEngine;
use crate::util::{cancel_signal_ttl, prune_expired_cancel_signals};

#[derive(Debug, Clone)]
struct PendingMergeGroup {
    jobs: Vec<Job>,
    created_at: Instant,
}

type MergeBuffers = Arc<Mutex<HashMap<String, Vec<PendingMergeGroup>>>>;

/// The Bria orchestrator manages sources, pipelines, sinks, and the HTTP server.
pub struct Orchestrator {
    config: Arc<Config>,
    /// Shutdown signal sender.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    _log_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    store: Arc<dyn StateStore>,
}

impl Orchestrator {
    /// Create a new orchestrator from configuration.
    pub async fn new(mut config: Config) -> Result<Self> {
        // Resolve pipeline sources
        for pipeline in &mut config.pipelines {
            pipeline.resolve_sources();
        }

        // Initialize tracing based on config
        let log_guard = Self::init_logging(&config);

        // Initialize the state store
        let store: Arc<dyn StateStore> =
            Arc::from(crate::state::create_store(&config.global.state).await?);

        let config = Arc::new(config);

        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);

        Ok(Self {
            config,
            shutdown_tx,
            _log_guard: log_guard,
            store,
        })
    }

    /// Run the orchestrator: start sources, server, and pipeline workers.
    pub async fn run(self) -> Result<()> {
        tracing::info!("Bria orchestrator starting...");

        // Create channels for each source
        let mut source_txs: HashMap<String, mpsc::UnboundedSender<Job>> = HashMap::new();
        let mut source_rxs: HashMap<String, mpsc::UnboundedReceiver<Job>> = HashMap::new();

        for source in self.config.sources.iter().filter(|source| source.enabled) {
            let (tx, rx) = mpsc::unbounded_channel();
            source_txs.insert(source.id.clone(), tx);
            source_rxs.insert(source.id.clone(), rx);
        }

        // Start source producers
        for source in self.config.sources.iter().filter(|source| source.enabled) {
            let tx = source_txs.get(&source.id).cloned().unwrap();
            match source.r#type {
                crate::config::SourceType::File => {
                    let source = source.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crate::sources::run_file_source_inline(&source, &tx).await {
                            tracing::error!("File source '{}' error: {e}", source.id);
                        }
                    });
                }
                crate::config::SourceType::Cron => {
                    let source = source.clone();
                    tokio::spawn(async move {
                        #[cfg(feature = "cron")]
                        if let Err(e) = crate::sources::run_cron_source_inline(&source, &tx).await {
                            tracing::error!("Cron source '{}' error: {e}", source.id);
                        }
                        #[cfg(not(feature = "cron"))]
                        {
                            tracing::error!(
                                "Cron source '{}' requires the 'cron' feature",
                                source.id
                            );
                            let _ = tx; // silence unused warning
                        }
                    });
                }
                crate::config::SourceType::Queue => {
                    let source = source.clone();
                    tokio::spawn(async move {
                        #[cfg(feature = "amqp")]
                        if let Err(e) = crate::sources::run_queue_source_inline(&source, &tx).await
                        {
                            tracing::error!("Queue source '{}' error: {e}", source.id);
                        }
                        #[cfg(not(feature = "amqp"))]
                        {
                            tracing::error!(
                                "Queue source '{}' requires the 'amqp' feature",
                                source.id
                            );
                            let _ = tx;
                        }
                    });
                }
                crate::config::SourceType::Pg => {
                    let source = source.clone();
                    tokio::spawn(async move {
                        #[cfg(feature = "postgres")]
                        if let Err(e) = crate::sources::run_pg_source_inline(&source, &tx).await {
                            tracing::error!("PG source '{}' error: {e}", source.id);
                        }
                        #[cfg(not(feature = "postgres"))]
                        {
                            tracing::error!(
                                "PG source '{}' requires the 'postgres' feature",
                                source.id
                            );
                            let _ = tx;
                        }
                    });
                }
                crate::config::SourceType::Sqlite => {
                    let source = source.clone();
                    tokio::spawn(async move {
                        #[cfg(feature = "sqlite")]
                        if let Err(e) = crate::sources::run_sqlite_source_inline(&source, &tx).await
                        {
                            tracing::error!("SQLite source '{}' error: {e}", source.id);
                        }
                        #[cfg(not(feature = "sqlite"))]
                        {
                            tracing::error!(
                                "SQLite source '{}' requires the 'sqlite' feature",
                                source.id
                            );
                            let _ = tx;
                        }
                    });
                }
                crate::config::SourceType::Http | crate::config::SourceType::Webhook => {
                    // Handled by the server
                }
            }
        }

        // Set up broadcast channel for stream sinks
        let broadcast_capacity = self
            .config
            .sinks
            .iter()
            .filter(|s| s.enabled && s.r#type == crate::config::SinkType::Stream)
            .map(|s| {
                s.broadcast_capacity
                    .max(crate::config::default_broadcast_capacity())
            })
            .next()
            .unwrap_or_else(crate::config::default_broadcast_capacity);

        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);

        // Set up the sink dispatcher
        let sink_dispatcher = Arc::new(SinkDispatcher::new(
            (*self.config).clone(),
            TemplateEngine::new(),
            Some(broadcast_tx.clone()),
        ));

        // Start the HTTP server if enabled
        let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::watch::channel(false);

        let server_h = crate::server::start_server(
            self.config.clone(),
            source_txs.clone(),
            Some(broadcast_tx.clone()),
            Some(server_shutdown_rx),
        )
        .await?;
        let cancel_signals: Arc<DashMap<String, Instant>> = server_h.cancel_signals;
        let pipeline_pauses: Arc<DashMap<String, Arc<pipeline::PipelinePause>>> =
            server_h.pipeline_pauses;
        let server_handle = server_h.join_handle;

        // Map source id -> list of pipeline ids consuming that source
        let mut source_to_pipelines: HashMap<String, Vec<String>> = HashMap::new();
        let pipeline_by_id: Arc<HashMap<String, crate::config::PipelineConfig>> = Arc::new(
            self.config
                .pipelines
                .iter()
                .map(|pipeline| (pipeline.id.clone(), pipeline.clone()))
                .collect(),
        );
        for pipeline in &self.config.pipelines {
            for source_id in pipeline.get_sources() {
                source_to_pipelines
                    .entry(source_id.clone())
                    .or_default()
                    .push(pipeline.id.clone());
            }
        }

        let merge_buffers: MergeBuffers = Arc::new(Mutex::new(HashMap::new()));
        let merge_cleanup_handle = spawn_merge_buffer_cleanup(
            merge_buffers.clone(),
            self.config.pipelines.clone(),
            self.shutdown_tx.subscribe(),
        );

        // Create a job channel for each pipeline
        let mut pipeline_txs: HashMap<String, mpsc::Sender<Job>> = HashMap::new();
        let mut pipeline_handles = Vec::new();

        for pipeline in &self.config.pipelines {
            let capacity = pipeline.queue_capacity.max(1);
            let (job_tx, job_rx) = mpsc::channel::<Job>(capacity);
            pipeline_txs.insert(pipeline.id.clone(), job_tx);

            let pipeline = pipeline.clone();
            let config = self.config.clone();
            let sink_dispatcher = sink_dispatcher.clone();
            let store = self.store.clone();
            let cancel_signals = cancel_signals.clone();
            let pipeline_pauses = pipeline_pauses.clone();

            let handle = tokio::spawn(async move {
                Self::pipeline_worker(
                    pipeline,
                    config,
                    sink_dispatcher,
                    job_rx,
                    store,
                    cancel_signals,
                    pipeline_pauses,
                )
                .await;
            });

            pipeline_handles.push(handle);
        }

        // Recover incomplete jobs from the state store and feed them into
        // the appropriate pipeline channels for re-execution.
        match self.store.recover_incomplete().await {
            Ok(incomplete) => {
                for record in incomplete {
                    let job = Job {
                        id: record.job_id,
                        source: record.source,
                        payload: record.payload,
                        correlation_key: record.correlation_key,
                        labels: record.labels.clone(),
                    };
                    let job_id = job.id.clone();
                    let pipeline_id = record.pipeline_id.clone();
                    if let Some(tx) = pipeline_txs.get(&pipeline_id) {
                        match tx.send(job).await {
                            Ok(()) => {
                                tracing::info!(
                                    "Recovered job '{job_id}' (state: {}) for pipeline '{pipeline_id}'",
                                    record.state,
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to send recovered job '{job_id}' to pipeline '{pipeline_id}': {e}",
                                );
                            }
                        }
                    } else {
                        tracing::warn!(
                            "Recovered job '{job_id}' for unknown pipeline '{pipeline_id}'; discarding",
                        );
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to recover incomplete jobs from state store: {e}");
            }
        }

        // Router: read from each source and forward to all consuming pipelines.
        // Keep router handles so shutdown can stop producers, release their
        // cloned pipeline senders, and let pipeline workers drain cleanly.
        let mut source_router_handles = Vec::new();
        for (source_id, mut rx) in source_rxs {
            let pipelines = source_to_pipelines
                .get(&source_id)
                .cloned()
                .unwrap_or_default();
            let pipeline_entries: Vec<(String, mpsc::Sender<Job>)> = pipelines
                .iter()
                .filter_map(|pid| pipeline_txs.get(pid).cloned().map(|tx| (pid.clone(), tx)))
                .collect();

            let source_id_clone = source_id.clone();
            let config = self.config.clone();
            let store = self.store.clone();
            let pipeline_by_id = pipeline_by_id.clone();
            let merge_buffers = merge_buffers.clone();
            let cancel_signals_router = cancel_signals.clone();
            let mut router_shutdown = self.shutdown_tx.subscribe();
            let handle = tokio::spawn(async move {
                loop {
                    let maybe_job = tokio::select! {
                        _ = router_shutdown.changed() => None,
                        maybe_job = rx.recv() => maybe_job,
                    };

                    let Some(mut job) = maybe_job else {
                        break;
                    };

                    // Handle synthetic cancellation jobs: insert into cancel map and skip enqueue.
                    if is_cancel_job(&job) {
                        if let Some(target) =
                            job.payload.get("target_job_id").and_then(|v| v.as_str())
                        {
                            prune_expired_cancel_signals(
                                &cancel_signals_router,
                                cancel_signal_ttl(&config),
                            );
                            cancel_signals_router.insert(target.to_string(), Instant::now());
                            tracing::info!(
                                "Router received cancellation for target job '{}' from source '{}'",
                                target,
                                source_id_clone
                            );
                        }
                        continue;
                    }

                    if let Ok(payload) = serde_json::to_vec(&job.payload)
                        && payload.len() > config.global.max_payload_bytes
                    {
                        tracing::error!(
                            "Source '{}' produced job '{}' exceeding max_payload_bytes ({} > {})",
                            source_id_clone,
                            job.id,
                            payload.len(),
                            config.global.max_payload_bytes
                        );
                        continue;
                    }
                    for (pipeline_id, tx) in &pipeline_entries {
                        let Some(pipeline) = pipeline_by_id.get(pipeline_id) else {
                            tracing::warn!("Pipeline '{pipeline_id}' is no longer configured");
                            continue;
                        };

                        // Merge pipeline labels into job labels (source labels first, pipeline labels override on collision).
                        for (k, v) in &pipeline.labels {
                            job.labels.insert(k.clone(), v.clone());
                        }

                        let ready_jobs = match collect_ready_jobs_for_pipeline(
                            pipeline,
                            job.clone(),
                            merge_buffers.clone(),
                        )
                        .await
                        {
                            Ok(jobs) => jobs,
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to merge job '{}' for pipeline '{}': {}",
                                    job.id,
                                    pipeline_id,
                                    e
                                );
                                continue;
                            }
                        };
                        for ready_job in ready_jobs {
                            if let Err(e) = store.record_queued(&ready_job, pipeline_id).await {
                                tracing::warn!(
                                    "Failed to record queued state for job '{}' pipeline '{}': {}",
                                    ready_job.id,
                                    pipeline_id,
                                    e
                                );
                            }
                            let _ = tx.send(ready_job).await;
                        }
                    }
                    if pipeline_entries.is_empty() {
                        tracing::warn!(
                            "Source '{}' produced a job but no pipeline consumes it",
                            source_id_clone
                        );
                    }
                }
                tracing::info!("Source '{}' router exiting", source_id_clone);
            });
            source_router_handles.push(handle);
        }

        // Wait for shutdown signal
        let mut shutdown_sub = self.shutdown_tx.subscribe();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received SIGINT; initiating graceful shutdown...");
                let _ = self.shutdown_tx.send(true);
                let _ = server_shutdown_tx.send(true);
            }
            _ = shutdown_sub.changed() => {
                tracing::info!("Shutdown signal received");
            }
        }

        // Wait for server to shut down gracefully if it was started
        if let Some(handle) = server_handle {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(self.config.server.shutdown_timeout_secs),
                handle,
            )
            .await;
        }

        // Graceful pipeline worker shutdown: give each worker a chance to
        // finish its in-flight job(s) before aborting. The shutdown signal
        // has already been broadcast via self.shutdown_tx.
        let drain_timeout = Duration::from_secs(self.config.global.shutdown_timeout_secs.max(1));

        // Stop source routers first. They watch the shutdown signal and should
        // exit quickly, releasing their cloned pipeline senders. Abort only if
        // a router fails to observe shutdown within the drain timeout.
        for mut handle in source_router_handles {
            match tokio::time::timeout(drain_timeout, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("Source router failed during shutdown: {e}"),
                Err(_elapsed) => {
                    tracing::warn!(
                        "Source router did not stop within {}s; aborting",
                        drain_timeout.as_secs()
                    );
                    handle.abort();
                }
            }
        }

        // Drop the original pipeline_txs so that when router tasks are dropped
        // the pipeline channels close and workers can observe None and exit.
        drop(pipeline_txs);

        for mut handle in pipeline_handles {
            match tokio::time::timeout(drain_timeout, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Pipeline worker panicked during shutdown: {e}");
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        "Pipeline worker did not drain within {}s; aborting",
                        drain_timeout.as_secs()
                    );
                    handle.abort();
                }
            }
        }
        let mut merge_cleanup_handle = merge_cleanup_handle;
        match tokio::time::timeout(drain_timeout, &mut merge_cleanup_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!("Merge cleanup task failed during shutdown: {e}"),
            Err(_elapsed) => {
                tracing::warn!(
                    "Merge cleanup task did not stop within {}s; aborting",
                    drain_timeout.as_secs()
                );
                merge_cleanup_handle.abort();
            }
        }

        tracing::info!("Bria orchestrator shut down complete.");
        Ok(())
    }

    /// Worker loop for a single pipeline: receives jobs and executes them.
    async fn pipeline_worker(
        pipeline: crate::config::PipelineConfig,
        config: Arc<crate::config::Config>,
        sink_dispatcher: Arc<SinkDispatcher>,
        mut job_rx: mpsc::Receiver<Job>,
        store: Arc<dyn StateStore>,
        cancel_signals: Arc<DashMap<String, Instant>>,
        pipeline_pauses: Arc<DashMap<String, Arc<pipeline::PipelinePause>>>,
    ) {
        let concurrency = if pipeline.concurrency > 0 {
            pipeline.concurrency
        } else {
            crate::config::default_concurrency()
        };
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        let mut in_flight = JoinSet::new();

        while let Some(job) = job_rx.recv().await {
            let pipeline = pipeline.clone();
            let config = config.clone();
            let sink_dispatcher = sink_dispatcher.clone();
            let semaphore = semaphore.clone();
            let pipeline_id = pipeline.id.clone();
            let store = store.clone();
            let cancel_signals = cancel_signals.clone();
            let pipeline_pauses = pipeline_pauses.clone();

            in_flight.spawn(async move {
                let _permit = semaphore.acquire().await.ok();

                // Check cancellation before marking as running.
                prune_expired_cancel_signals(&cancel_signals, cancel_signal_ttl(&config));
                let cancel_requested = cancel_signals
                    .get(&job.id)
                    .is_some_and(|inserted_at| inserted_at.elapsed() <= cancel_signal_ttl(&config));
                if cancel_requested {
                    cancel_signals.remove(&job.id);
                    tracing::info!(
                        "Job '{}' cancelled before execution on pipeline '{}'",
                        job.id,
                        pipeline_id
                    );
                    let _ = store
                        .record_completed(&job.id, &pipeline_id, "cancelled")
                        .await
                        .inspect_err(|e| {
                            tracing::warn!(
                                "Failed to record cancelled state for job '{}' pipeline '{}': {}",
                                job.id,
                                pipeline_id,
                                e
                            );
                        });
                    return;
                }

                // Record that this job is now running.
                let _ = store
                    .record_running(&job, &pipeline_id)
                    .await
                    .inspect_err(|e| {
                        tracing::warn!(
                            "Failed to record running state for job '{}' pipeline '{}': {}",
                            job.id,
                            pipeline_id,
                            e
                        );
                    });

                let template = Arc::new(TemplateEngine::new());
                let evaluator = Arc::new(Evaluator::with_pipeline_id(pipeline_id.clone()));

                let result = pipeline::run_pipeline(
                    &pipeline,
                    job.clone(),
                    config,
                    template,
                    evaluator,
                    pipeline_pauses,
                )
                .await;

                let mut ctx = Context::new(result.job.clone());
                ctx.steps = result.steps.clone();

                // Only send through sinks if not cancelled.
                if result.status != "cancelled" {
                    sink_dispatcher.send_pipeline_result(&result, &ctx).await;
                }

                // Record completion.
                let _ = store
                    .record_completed(&result.job.id, &pipeline_id, &result.status)
                    .await
                    .inspect_err(|e| {
                        tracing::warn!(
                            "Failed to record completed state for job '{}' pipeline '{}': {}",
                            result.job.id,
                            pipeline_id,
                            e
                        );
                    });

                cancel_signals.remove(&result.job.id);

                tracing::debug!(
                    "Pipeline '{}' completed job '{}' with status '{}' in {}ms",
                    pipeline_id,
                    result.job.id,
                    result.status,
                    result.duration_ms,
                );
            });
        }

        while let Some(result) = in_flight.join_next().await {
            if let Err(e) = result {
                tracing::warn!("Pipeline '{}' job task failed: {e}", pipeline.id);
            }
        }

        tracing::info!("Pipeline '{}' worker exiting (channel closed)", pipeline.id);
    }

    /// Initialize logging based on global config.
    fn init_logging(config: &Config) -> Option<tracing_appender::non_blocking::WorkerGuard> {
        if running_under_cargo_test() && std::env::var_os("BRIA_TEST_LOG").is_none() {
            let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.global.log.level));
            let _ = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_target(true)
                .with_thread_ids(true)
                .with_test_writer()
                .try_init();
            return None;
        }

        let log_cfg = &config.global.log;

        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log_cfg.level));

        if log_cfg.file.is_empty() {
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_target(true)
                .with_thread_ids(true);

            let _ = match log_cfg.effective_format() {
                "json" => subscriber.json().try_init(),
                _ => subscriber.try_init(),
            };
            return None;
        }

        let path = std::path::Path::new(&log_cfg.file);
        if let Some(parent) = path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            eprintln!("failed to create log directory '{}': {e}", parent.display());
            return None;
        }

        let file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => file,
            Err(e) => {
                eprintln!("failed to open log file '{}': {e}", path.display());
                return None;
            }
        };
        let (writer, guard) = tracing_appender::non_blocking(file);
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_writer(writer);

        let _ = match log_cfg.effective_format() {
            "json" => subscriber.json().try_init(),
            _ => subscriber.try_init(),
        };
        Some(guard)
    }
}

fn running_under_cargo_test() -> bool {
    std::env::args()
        .next()
        .and_then(|arg| {
            std::path::PathBuf::from(arg)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .is_some_and(|name| name.starts_with("integration") || name.starts_with("sinks_server"))
}

fn spawn_merge_buffer_cleanup(
    merge_buffers: MergeBuffers,
    pipelines: Vec<crate::config::PipelineConfig>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    prune_merge_buffers(&merge_buffers, &pipelines).await;
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() {
                        prune_merge_buffers(&merge_buffers, &pipelines).await;
                    }
                    break;
                }
            }
        }
    })
}

async fn prune_merge_buffers(
    merge_buffers: &MergeBuffers,
    pipelines: &[crate::config::PipelineConfig],
) {
    let now = Instant::now();
    let mut buffers = merge_buffers.lock().await;
    buffers.retain(|pipeline_id, groups| {
        let timeout = pipelines
            .iter()
            .find(|pipeline| &pipeline.id == pipeline_id)
            .and_then(|pipeline| pipeline.merge.as_ref())
            .map(|merge| Duration::from_secs(merge.timeout_secs.max(1)))
            .unwrap_or_else(|| Duration::from_secs(60));
        groups.retain(|group| now.duration_since(group.created_at) <= timeout);
        !groups.is_empty()
    });
}

async fn collect_ready_jobs_for_pipeline(
    pipeline: &crate::config::PipelineConfig,
    job: Job,
    merge_buffers: MergeBuffers,
) -> Result<Vec<Job>> {
    let sources = pipeline.get_sources();
    if sources.len() <= 1 {
        return Ok(vec![job]);
    }

    let Some(merge) = pipeline.merge.as_ref() else {
        return Ok(vec![job]);
    };

    if merge.strategy == "any" {
        let mut job = job;
        if let Some(key) = merge.correlation_key.as_deref() {
            job.correlation_key = payload_correlation_value(&job, key);
        }
        return Ok(vec![job]);
    }

    let timeout = Duration::from_secs(merge.timeout_secs.max(1));
    let now = Instant::now();
    let mut buffers = merge_buffers.lock().await;
    let groups = buffers.entry(pipeline.id.clone()).or_default();
    groups.retain(|group| now.duration_since(group.created_at) <= timeout);

    let matching_index = if let Some(key) = merge.correlation_key.as_deref() {
        let job_key = payload_correlation_value(&job, key).ok_or_else(|| {
            Error::Pipeline(format!(
                "Job '{}' from source '{}' missing correlation key '{}' for pipeline '{}'",
                job.id, job.source, key, pipeline.id
            ))
        })?;
        groups.iter().position(|group| {
            group.jobs.iter().any(|existing| {
                payload_correlation_value(existing, key).as_deref() == Some(job_key.as_str())
            })
        })
    } else if let Some(expr) = merge.correlation_expr.as_deref() {
        let evaluator = Evaluator::with_pipeline_id(pipeline.id.clone());
        let right = Context::new(job.clone());
        let mut found = None;
        for (idx, group) in groups.iter().enumerate() {
            let mut matches_group = false;
            for existing in &group.jobs {
                let left = Context::new(existing.clone());
                if evaluator.eval_merge_bool(expr, &left, &right)? {
                    matches_group = true;
                    break;
                }
            }
            if matches_group {
                found = Some(idx);
                break;
            }
        }
        found
    } else {
        None
    };

    let idx = if let Some(idx) = matching_index {
        idx
    } else {
        groups.push(PendingMergeGroup {
            jobs: Vec::new(),
            created_at: now,
        });
        groups.len() - 1
    };

    if !groups[idx]
        .jobs
        .iter()
        .any(|existing| existing.source == job.source)
    {
        groups[idx].jobs.push(job);
    } else {
        tracing::warn!(
            "Received duplicate source '{}' for merge group in pipeline '{}'; keeping first job",
            job.source,
            pipeline.id
        );
    }

    let has_all_sources = sources
        .iter()
        .all(|source_id| groups[idx].jobs.iter().any(|job| &job.source == source_id));
    if has_all_sources {
        let group = groups.remove(idx);
        Ok(vec![merge_group_to_job(
            pipeline,
            group.jobs,
            merge.correlation_key.as_deref(),
        )])
    } else {
        Ok(Vec::new())
    }
}

fn payload_correlation_value(job: &Job, key: &str) -> Option<String> {
    let value = job.payload.get(key)?;
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Check if a job is a synthetic cancellation job (has __bria_cancel marker).
fn is_cancel_job(job: &Job) -> bool {
    job.payload
        .get("__bria_cancel")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn merge_group_to_job(
    pipeline: &crate::config::PipelineConfig,
    jobs: Vec<Job>,
    correlation_key: Option<&str>,
) -> Job {
    let mut payload = serde_json::Map::new();
    let mut source_payloads = serde_json::Map::new();
    let mut job_entries = Vec::with_capacity(jobs.len());

    for job in &jobs {
        if let serde_json::Value::Object(obj) = &job.payload {
            for (key, value) in obj {
                if let Some(existing) = payload.get(key)
                    && existing != value
                {
                    tracing::warn!(
                        pipeline_id = %pipeline.id,
                        source = %job.source,
                        key = %key,
                        "Merge payload key conflict; keeping first value"
                    );
                    continue;
                }
                payload.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        source_payloads.insert(job.source.clone(), job.payload.clone());
        job_entries.push(serde_json::json!({
            "id": job.id,
            "source": job.source,
            "payload": job.payload,
        }));
    }

    if let Some(key) = correlation_key
        && let Some(value) = jobs
            .iter()
            .find_map(|job| payload_correlation_value(job, key))
    {
        payload.insert(key.to_string(), serde_json::Value::String(value.clone()));
        payload.insert(
            "correlation_key".to_string(),
            serde_json::Value::String(value),
        );
    }

    payload.insert(
        "sources".to_string(),
        serde_json::Value::Object(source_payloads),
    );
    payload.insert("jobs".to_string(), serde_json::Value::Array(job_entries));

    // Merge labels from all source jobs (first one wins for each key).
    let mut merged_labels: HashMap<String, String> = HashMap::new();
    for job in &jobs {
        for (k, v) in &job.labels {
            merged_labels.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    Job {
        id: ulid::Ulid::r#gen().to_string(),
        source: format!("merge:{}", pipeline.id),
        payload: serde_json::Value::Object(payload),
        correlation_key: correlation_key.and_then(|key| {
            jobs.iter()
                .find_map(|job| payload_correlation_value(job, key))
        }),
        labels: merged_labels,
    }
}

/// Run a single pipeline job synchronously (for integration test friendliness).
pub async fn run_pipeline_once(
    config: &str,
    pipeline_id: &str,
    job: Job,
) -> Result<crate::context::PipelineResult> {
    let config = crate::config::Config::from_str_with_env(config)?;
    config.validate()?;
    run_pipeline_once_with_config(&config, pipeline_id, job).await
}

/// Run a single pipeline job using an already parsed and validated config.
pub async fn run_pipeline_once_with_config(
    config: &crate::config::Config,
    pipeline_id: &str,
    job: Job,
) -> Result<crate::context::PipelineResult> {
    let pipeline = config
        .pipelines
        .iter()
        .find(|p| p.id == pipeline_id)
        .cloned()
        .ok_or_else(|| Error::NotFound(format!("Pipeline '{pipeline_id}' not found")))?;

    let template = Arc::new(TemplateEngine::new());
    let evaluator = Arc::new(Evaluator::with_pipeline_id(pipeline.id.clone()));
    let pipeline_pauses: Arc<DashMap<String, Arc<pipeline::PipelinePause>>> =
        Arc::new(DashMap::new());

    Ok(pipeline::run_pipeline(
        &pipeline,
        job,
        Arc::new(config.clone()),
        template,
        evaluator,
        pipeline_pauses,
    )
    .await)
}
