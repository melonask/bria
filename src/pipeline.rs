use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::Semaphore;

use crate::config::{self, FailureAction, PipelineConfig};
use crate::context::{Context, Job, PipelineResult, StepResult};
use crate::error::{Error, Result};
use crate::expression::Evaluator;
use crate::task_runner;
use crate::template::TemplateEngine;

struct PipelineFailureContext<'a> {
    duration_ms: u64,
    error_msg: String,
    pipeline_pauses: Arc<DashMap<String, Arc<PipelinePause>>>,
    failed_step_ids: &'a [String],
}

enum ConditionSignal {
    None,
    Emit,
    SkipTo(String),
}

/// Shared, durable-in-memory resume state for a stopped pipeline.
///
/// `Notify` alone loses a notification sent before a worker starts waiting.
/// The atomic flag records that resume request and `Notify` wakes existing
/// waiters, allowing either ordering of operator request and worker pause.
pub struct PipelinePause {
    resumed: AtomicBool,
    notify: tokio::sync::Notify,
}

impl PipelinePause {
    pub fn new() -> Self {
        Self {
            resumed: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Record an operator resume request and wake all currently stopped jobs.
    pub fn resume(&self) {
        self.resumed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Block until a resume request is observed without losing an early wakeup.
    async fn wait_for_resume(&self) {
        loop {
            let notified = self.notify.notified();
            if self.resumed.swap(false, Ordering::AcqRel) {
                return;
            }
            notified.await;
        }
    }
}

impl Default for PipelinePause {
    fn default() -> Self {
        Self::new()
    }
}

struct StepExecution {
    result: StepResult,
    context: Context,
    signal: ConditionSignal,
}

/// Run a pipeline for a single job.
/// Returns the pipeline result (success or failure).
pub async fn run_pipeline(
    pipeline: &PipelineConfig,
    job: Job,
    config: Arc<crate::config::Config>,
    template: Arc<TemplateEngine>,
    evaluator: Arc<Evaluator>,
    pipeline_pauses: Arc<DashMap<String, Arc<PipelinePause>>>,
) -> PipelineResult {
    let pipeline_start = Instant::now();
    let mut ctx = Context::new(job.clone());

    // Resolve task configs for each process step
    let step_task_map: HashMap<String, config::TaskConfig> = pipeline
        .steps
        .iter()
        .filter(|s| s.r#type == config::StepType::Process)
        .filter_map(|s| {
            let task_id = s.task.as_ref()?;
            config.get_task(task_id).cloned().map(|t| (s.id.clone(), t))
        })
        .collect();

    // Build the execution plan (topologically sorted levels)
    let execution_plan = match build_execution_plan(pipeline) {
        Ok(plan) => plan,
        Err(e) => {
            tracing::error!("Pipeline '{}' execution plan error: {e}", pipeline.id);
            let steps = ctx.steps.clone();
            return handle_pipeline_failure(
                pipeline,
                &ctx,
                job,
                steps,
                PipelineFailureContext {
                    duration_ms: pipeline_start.elapsed().as_millis() as u64,
                    error_msg: format!("{e}"),
                    pipeline_pauses,
                    failed_step_ids: &[],
                },
            )
            .await;
        }
    };

    // Execute each level (steps within a level run concurrently)
    let concurrency = if pipeline.concurrency > 0 {
        pipeline.concurrency
    } else {
        config::default_concurrency()
    };
    let semaphore = Arc::new(Semaphore::new(concurrency));

    let mut level_idx = 0;
    while level_idx < execution_plan.len() {
        let level = &execution_plan[level_idx];
        let level_steps: Vec<_> = level
            .iter()
            .filter_map(|step_id| pipeline.steps.iter().find(|s| &s.id == step_id))
            .collect();

        let mut handles = Vec::new();

        for step in level_steps {
            let ctx_clone = ctx.clone();
            let config_clone = config.clone();
            let template_clone = template.clone();
            let evaluator_clone = evaluator.clone();
            let semaphore_clone = semaphore.clone();
            let step_clone = step.clone();
            let task_config = step_task_map.get(&step.id).cloned();

            let handle = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await;
                execute_step(
                    &step_clone,
                    task_config.as_ref(),
                    &ctx_clone,
                    &config_clone,
                    template_clone,
                    evaluator_clone,
                )
                .await
            });

            handles.push((step.id.clone(), handle));
        }

        // Wait for all steps in this level to complete
        let mut level_has_failure = false;
        let mut level_error_msg = None;
        let mut failed_step_ids: Vec<String> = Vec::new();
        let mut pending_signal = ConditionSignal::None;

        for (step_id, handle) in handles {
            match handle.await {
                Ok(Ok(execution)) => {
                    ctx.steps.insert(step_id.clone(), execution.result);
                    let signal = execution.signal;
                    let updated_ctx = execution.context;
                    ctx.job = updated_ctx.job;
                    for (updated_step_id, updated_result) in updated_ctx.steps {
                        ctx.steps.insert(updated_step_id, updated_result);
                    }
                    match signal {
                        ConditionSignal::None => {}
                        ConditionSignal::Emit => {
                            pending_signal = ConditionSignal::Emit;
                        }
                        ConditionSignal::SkipTo(target) => {
                            if !matches!(pending_signal, ConditionSignal::Emit) {
                                pending_signal = ConditionSignal::SkipTo(target);
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!("Step '{}' failed: {e}", step_id);
                    let failed_result = StepResult {
                        stdout: None,
                        stderr: Some(format!("{e}")),
                        exit_code: -1,
                        duration_ms: 0,
                        attempt: 0,
                        outputs: HashMap::new(),
                    };
                    ctx.steps.insert(step_id.clone(), failed_result);
                    level_has_failure = true;
                    failed_step_ids.push(step_id.clone());
                    level_error_msg = Some(format!("{e}"));
                }
                Err(join_err) => {
                    tracing::error!("Step '{}' panicked: {join_err}", step_id);
                    let failed_result = StepResult {
                        stdout: None,
                        stderr: Some(format!("{join_err}")),
                        exit_code: -1,
                        duration_ms: 0,
                        attempt: 0,
                        outputs: HashMap::new(),
                    };
                    ctx.steps.insert(step_id.clone(), failed_result);
                    level_has_failure = true;
                    failed_step_ids.push(step_id.clone());
                    level_error_msg = Some(format!("{join_err}"));
                }
            }
        }

        if level_has_failure {
            let steps = ctx.steps.clone();
            let duration = pipeline_start.elapsed().as_millis() as u64;
            return handle_pipeline_failure(
                pipeline,
                &ctx,
                job,
                steps,
                PipelineFailureContext {
                    duration_ms: duration,
                    error_msg: level_error_msg.unwrap_or_else(|| "unknown error".to_string()),
                    pipeline_pauses,
                    failed_step_ids: &failed_step_ids,
                },
            )
            .await;
        }

        match pending_signal {
            ConditionSignal::None => {}
            ConditionSignal::Emit => {
                let steps = ctx.steps.clone();
                let duration = pipeline_start.elapsed().as_millis() as u64;
                return PipelineResult::success(pipeline.id.clone(), job, steps, duration);
            }
            ConditionSignal::SkipTo(target) => {
                let target_level = execution_plan.iter().position(|lev| lev.contains(&target));
                match target_level {
                    Some(idx) if idx > level_idx => {
                        level_idx = idx;
                        continue;
                    }
                    Some(_) => {
                        level_idx += 1;
                        continue;
                    }
                    None => {
                        tracing::error!(
                            "Skip-to target '{}' not found in pipeline '{}'",
                            target,
                            pipeline.id
                        );
                        let steps = ctx.steps.clone();
                        let duration = pipeline_start.elapsed().as_millis() as u64;
                        return handle_pipeline_failure(
                            pipeline,
                            &ctx,
                            job,
                            steps,
                            PipelineFailureContext {
                                duration_ms: duration,
                                error_msg: format!("Skip-to target '{}' not found", target),
                                pipeline_pauses,
                                failed_step_ids: &[],
                            },
                        )
                        .await;
                    }
                }
            }
        }

        level_idx += 1;
    }

    let steps = ctx.steps.clone();
    let duration = pipeline_start.elapsed().as_millis() as u64;
    PipelineResult::success(pipeline.id.clone(), job, steps, duration)
}

/// Execute a single pipeline step.
async fn execute_step(
    step: &config::StepConfig,
    task_config: Option<&config::TaskConfig>,
    ctx: &Context,
    config: &Arc<crate::config::Config>,
    template: Arc<TemplateEngine>,
    evaluator: Arc<Evaluator>,
) -> Result<StepExecution> {
    match step.r#type {
        config::StepType::Process => {
            execute_process_step(step, task_config, ctx, config, template).await
        }
        config::StepType::Map => execute_map_step(step, ctx, &evaluator),
        config::StepType::Condition => execute_condition_step(step, ctx, &evaluator),
    }
}

/// Execute a process step (run a task).
async fn execute_process_step(
    step: &config::StepConfig,
    task_config: Option<&config::TaskConfig>,
    ctx: &Context,
    config: &Arc<crate::config::Config>,
    template: Arc<TemplateEngine>,
) -> Result<StepExecution> {
    let task = task_config
        .ok_or_else(|| Error::pipeline(format!("Step '{}' has no task configured", step.id)))?;

    // Determine effective retry config (step > task > global)
    let max_attempts = step
        .retry
        .max_attempts
        .or({
            if task.retry.max_attempts > 0 {
                Some(task.retry.max_attempts)
            } else {
                None
            }
        })
        .unwrap_or(config.global.retry.max_attempts)
        .max(1);

    let base_delay_ms = step
        .retry
        .base_delay_ms
        .or({
            if task.retry.base_delay_ms > 0 {
                Some(task.retry.base_delay_ms)
            } else {
                None
            }
        })
        .unwrap_or(config.global.retry.base_delay_ms);

    let max_delay_ms = step
        .retry
        .max_delay_ms
        .or({
            if task.retry.max_delay_ms > 0 {
                Some(task.retry.max_delay_ms)
            } else {
                None
            }
        })
        .unwrap_or(config.global.retry.max_delay_ms);

    let jitter = step.retry.jitter;
    let jitter = if (0.0..=1.0).contains(&jitter) {
        jitter
    } else if (0.0..=1.0).contains(&task.retry.jitter) {
        task.retry.jitter
    } else {
        config.global.retry.jitter
    };

    let mut last_error = None;

    for attempt in 1..=max_attempts {
        match task_runner::run_task(
            task,
            step.with.as_ref(),
            step.outputs.as_ref(),
            &config.global,
            ctx,
            template.as_ref(),
        )
        .await
        {
            Ok(result) => {
                let step_result = StepResult {
                    stdout: result.stdout,
                    stderr: result.stderr,
                    exit_code: result.exit_code,
                    duration_ms: result.duration_ms,
                    attempt,
                    outputs: result.outputs,
                };

                let mut new_ctx = ctx.clone();
                new_ctx.set_step(step.id.clone(), step_result.clone());
                return Ok(StepExecution {
                    result: step_result,
                    context: new_ctx,
                    signal: ConditionSignal::None,
                });
            }
            Err(e) => {
                tracing::warn!(
                    "Step '{}' attempt {}/{} failed: {e}",
                    step.id,
                    attempt,
                    max_attempts
                );
                last_error = Some(e);

                if attempt < max_attempts {
                    // Calculate backoff with jitter
                    let delay = calculate_backoff(base_delay_ms, max_delay_ms, attempt - 1, jitter);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| Error::Task(format!("Step '{}' failed all attempts", step.id))))
}

/// Execute a map step (reshape context with CEL expressions).
fn execute_map_step(
    step: &config::StepConfig,
    ctx: &Context,
    evaluator: &Evaluator,
) -> Result<StepExecution> {
    let start = Instant::now();
    let mut new_ctx = ctx.clone();

    for set_entry in &step.set {
        let value = evaluator.eval_value(&set_entry.expr, ctx)?;
        set_context_value(&mut new_ctx, &set_entry.target, value)?;
    }

    let step_result = StepResult {
        stdout: None,
        stderr: None,
        exit_code: 0,
        duration_ms: start.elapsed().as_millis() as u64,
        attempt: 1,
        outputs: HashMap::new(),
    };

    new_ctx.set_step(step.id.clone(), step_result.clone());
    Ok(StepExecution {
        result: step_result,
        context: new_ctx,
        signal: ConditionSignal::None,
    })
}

/// Execute a condition step (branch based on CEL expression).
fn execute_condition_step(
    step: &config::StepConfig,
    ctx: &Context,
    evaluator: &Evaluator,
) -> Result<StepExecution> {
    let start = Instant::now();
    let expr = step.expr.as_deref().unwrap_or("true");

    let condition = evaluator.eval_bool(expr, ctx)?;

    if condition {
        // Expression is true — continue normally
        let step_result = StepResult {
            stdout: None,
            stderr: None,
            exit_code: 0,
            duration_ms: start.elapsed().as_millis() as u64,
            attempt: 1,
            outputs: HashMap::new(),
        };
        let mut new_ctx = ctx.clone();
        new_ctx.set_step(step.id.clone(), step_result.clone());
        Ok(StepExecution {
            result: step_result,
            context: new_ctx,
            signal: ConditionSignal::None,
        })
    } else {
        // Expression is false — take action
        let action = step.action.as_deref().unwrap_or("fail");
        match action {
            "fail" => {
                let reason = step
                    .reason
                    .clone()
                    .unwrap_or_else(|| format!("Condition '{expr}' evaluated to false"));
                Err(Error::Pipeline(reason))
            }
            "skip_to" => {
                let target = step.skip_to.as_deref().unwrap_or("");
                let step_result = StepResult {
                    stdout: Some(format!("skipped_to: {target}")),
                    stderr: None,
                    exit_code: 0,
                    duration_ms: start.elapsed().as_millis() as u64,
                    attempt: 1,
                    outputs: HashMap::new(),
                };
                let mut new_ctx = ctx.clone();
                new_ctx.set_step(step.id.clone(), step_result.clone());
                Ok(StepExecution {
                    result: step_result,
                    context: new_ctx,
                    signal: ConditionSignal::SkipTo(target.to_string()),
                })
            }
            "emit" => {
                let step_result = StepResult {
                    stdout: Some("emitted".to_string()),
                    stderr: None,
                    exit_code: 0,
                    duration_ms: start.elapsed().as_millis() as u64,
                    attempt: 1,
                    outputs: HashMap::new(),
                };
                let mut new_ctx = ctx.clone();
                new_ctx.set_step(step.id.clone(), step_result.clone());
                Ok(StepExecution {
                    result: step_result,
                    context: new_ctx,
                    signal: ConditionSignal::Emit,
                })
            }
            other => Err(Error::Pipeline(format!(
                "Unknown condition action: {other}"
            ))),
        }
    }
}

/// Handle pipeline failure based on the failure configuration.
async fn handle_pipeline_failure(
    pipeline: &PipelineConfig,
    _ctx: &Context,
    job: Job,
    steps: HashMap<String, StepResult>,
    failure: PipelineFailureContext<'_>,
) -> PipelineResult {
    tracing::error!(
        "Pipeline '{}' failed for job '{}': {}",
        pipeline.id,
        job.id,
        failure.error_msg,
    );

    let should_stop = failure.failed_step_ids.iter().any(|failed_step_id| {
        pipeline
            .steps
            .iter()
            .find(|step| &step.id == failed_step_id)
            .is_some_and(|step| step.failure.action == FailureAction::Stop)
    }) || pipeline.failure.action == FailureAction::Stop;

    if !should_stop {
        return PipelineResult::failure(pipeline.id.clone(), job, steps, failure.duration_ms);
    }

    tracing::warn!(
        "Pipeline '{}' STOPPED for job '{}' — waiting for operator intervention (POST /v1/pipelines/{}/resume)",
        pipeline.id,
        job.id,
        pipeline.id,
    );

    // Get or create a notification mechanism for this pipeline.
    let pause = failure
        .pipeline_pauses
        .entry(pipeline.id.clone())
        .or_insert_with(|| Arc::new(PipelinePause::new()))
        .clone();

    // Wait indefinitely until an operator calls the resume endpoint.
    pause.wait_for_resume().await;

    tracing::info!(
        "Pipeline '{}' resumed after operator intervention for job '{}'",
        pipeline.id,
        job.id
    );

    PipelineResult::failure(pipeline.id.clone(), job, steps, failure.duration_ms)
}

/// Build an execution plan: levels of steps that can run in parallel.
/// Returns a Vec of Vec<step_id> where each inner Vec is a level that can run concurrently.
fn build_execution_plan(pipeline: &PipelineConfig) -> Result<Vec<Vec<String>>> {
    // Build dependency graph
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();

    for (i, step) in pipeline.steps.iter().enumerate() {
        let step_deps = if step.depends_on.is_empty() {
            // Implicit dependency: depends on previous step
            if i > 0 {
                vec![pipeline.steps[i - 1].id.clone()]
            } else {
                vec![]
            }
        } else {
            step.depends_on.clone()
        };
        deps.insert(step.id.clone(), step_deps);
    }

    // Topological sort with levels
    let mut in_degree: HashMap<String, usize> = deps.keys().map(|k| (k.clone(), 0)).collect();
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();

    for (step_id, step_deps) in &deps {
        for dep in step_deps {
            *in_degree.get_mut(step_id).unwrap() += 1;
            adj.entry(dep.clone()).or_default().push(step_id.clone());
        }
    }

    let mut levels: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| id.clone())
        .collect();

    let total_steps = deps.len();
    let mut processed = 0;

    while !current.is_empty() {
        levels.push(current.clone());
        let mut next = Vec::new();

        for node in &current {
            processed += 1;
            if let Some(neighbors) = adj.get(node) {
                for neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            next.push(neighbor.clone());
                        }
                    }
                }
            }
        }

        current = next;
    }

    if processed != total_steps {
        return Err(Error::Pipeline(format!(
            "Pipeline '{}' has a cycle in its step dependencies",
            pipeline.id
        )));
    }

    Ok(levels)
}

/// Set a value in the context by dotted target path.
fn set_context_value(ctx: &mut Context, target: &str, value: serde_json::Value) -> Result<()> {
    let parts: Vec<&str> = target.splitn(2, '.').collect();
    if parts.len() != 2 {
        return Err(Error::Pipeline(format!("Invalid target path: {target}")));
    }

    match parts[0] {
        "job" => {
            let sub_parts: Vec<&str> = parts[1].splitn(2, '.').collect();
            if sub_parts.len() == 2 && sub_parts[0] == "payload" {
                if let serde_json::Value::Object(ref mut payload) = ctx.job.payload {
                    set_json_path(payload, sub_parts[1], value);
                } else {
                    return Err(Error::Pipeline("job.payload is not an object".to_string()));
                }
            } else {
                return Err(Error::Pipeline(format!("Cannot set job.{0}", parts[1])));
            }
        }
        other => {
            return Err(Error::Pipeline(format!(
                "Unknown target namespace: {other}"
            )));
        }
    }

    Ok(())
}

fn set_json_path(
    root: &mut serde_json::Map<String, serde_json::Value>,
    path: &str,
    value: serde_json::Value,
) {
    let mut parts = path.split('.').peekable();
    let mut current = root;
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            current.insert(part.to_string(), value);
            return;
        }
        let entry = current
            .entry(part.to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if !entry.is_object() {
            *entry = serde_json::Value::Object(serde_json::Map::new());
        }
        current = entry.as_object_mut().expect("object initialized above");
    }
}

/// Calculate exponential backoff with jitter.
fn calculate_backoff(base_ms: u64, max_ms: u64, attempt: u32, jitter: f64) -> u64 {
    let exp = 2u64.saturating_pow(attempt);
    let delay = (base_ms * exp).min(max_ms);

    if jitter <= 0.0 {
        return delay;
    }

    let bounded_jitter = jitter.clamp(0.0, 1.0);
    let random_ratio = rand::random::<f64>();
    let jitter_factor = 1.0 - (bounded_jitter * random_ratio);
    (delay as f64 * jitter_factor) as u64
}

#[cfg(test)]
mod tests {
    use super::PipelinePause;

    #[tokio::test]
    async fn pipeline_pause_retains_resume_before_waiting() {
        let pause = PipelinePause::new();
        pause.resume();

        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            pause.wait_for_resume(),
        )
        .await
        .expect("an early resume request must not be lost");
    }
}
