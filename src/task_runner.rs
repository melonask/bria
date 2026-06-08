use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command as TokioCommand;
use tokio::time::timeout;

use crate::config;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::template::TemplateEngine;

const WASM_PAGE_SIZE_BYTES: u64 = 65_536;
const DIRECT_STDIN_MAX_BYTES: usize = 65_536;

/// Result of running a task.
#[derive(Debug, Clone)]
pub struct TaskRunResult {
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: i32,
    pub duration_ms: u64,
    pub outputs: HashMap<String, serde_json::Value>,
}

/// Execute a task according to its configuration against a pipeline context.
pub async fn run_task(
    task: &config::TaskConfig,
    step_with: Option<&config::StepWithConfig>,
    step_outputs: Option<&config::StepOutputsConfig>,
    global_config: &config::GlobalConfig,
    ctx: &Context,
    template: &TemplateEngine,
) -> Result<TaskRunResult> {
    match task.kind() {
        config::TaskDriverKind::Local(task) => {
            run_local(task, step_with, step_outputs, global_config, ctx, template).await
        }
        config::TaskDriverKind::Docker(task) => {
            run_docker(task, step_with, step_outputs, global_config, ctx, template).await
        }
        config::TaskDriverKind::Wasm(task) => {
            run_wasm(task, step_with, step_outputs, global_config, ctx, template).await
        }
    }
}

/// Execute a local subprocess task.
async fn run_local(
    task: &config::TaskConfig,
    step_with: Option<&config::StepWithConfig>,
    step_outputs: Option<&config::StepOutputsConfig>,
    global_config: &config::GlobalConfig,
    ctx: &Context,
    template: &TemplateEngine,
) -> Result<TaskRunResult> {
    let start = std::time::Instant::now();

    // Apply StepWithConfig overrides for cmd
    let cmd = if let Some(with) = step_with {
        if let Some(ref with_cmd) = with.cmd {
            template.render(with_cmd, ctx)?
        } else {
            template.render(&task.cmd, ctx)?
        }
    } else {
        template.render(&task.cmd, ctx)?
    };

    let args: Vec<String> = if let Some(with) = step_with {
        if let Some(ref with_args) = with.args {
            with_args
                .iter()
                .map(|a| template.render(a, ctx))
                .collect::<Result<Vec<_>>>()?
        } else {
            task.args
                .iter()
                .map(|a| template.render(a, ctx))
                .collect::<Result<Vec<_>>>()?
        }
    } else {
        task.args
            .iter()
            .map(|a| template.render(a, ctx))
            .collect::<Result<Vec<_>>>()?
    };

    let env_vars = build_env(task, step_with, template, ctx)?;

    // Resolve working_dir: step_with > task
    let working_dir = step_with
        .and_then(|w| w.working_dir.as_ref())
        .or(task.working_dir.as_ref())
        .cloned();

    // Resolve stdin config: step_with > task
    let stdin_mode = step_with
        .and_then(|w| w.stdin.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stdin.mode.as_str());
    let stdin_template = step_with
        .and_then(|w| w.stdin.as_ref().and_then(|s| s.template.as_deref()))
        .or(task.stdin.template.as_deref());

    // Resolve stdout config: step_with > task
    let stdout_mode = step_with
        .and_then(|w| w.stdout.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stdout.mode.as_str());
    let stdout_max_bytes = step_with
        .and_then(|w| w.stdout.as_ref().map(|s| s.max_bytes))
        .unwrap_or(task.stdout.max_bytes);

    // Resolve stderr config: step_with > task
    let stderr_mode = step_with
        .and_then(|w| w.stderr.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stderr.mode.as_str());
    let stderr_max_bytes = step_with
        .and_then(|w| w.stderr.as_ref().map(|s| s.max_bytes))
        .unwrap_or(task.stderr.max_bytes);

    // Resolve success_exit_codes: step_with > task
    let success_exit_codes = step_with
        .and_then(|w| w.success_exit_codes.as_ref())
        .unwrap_or(&task.success_exit_codes);

    // Resolve timeout_secs: step_with > task > global
    let timeout_secs = step_with
        .and_then(|w| w.timeout_secs)
        .or({
            if task.timeout_secs > 0 {
                Some(task.timeout_secs)
            } else {
                None
            }
        })
        .unwrap_or(global_config.timeout.step_secs);

    // Resolve timeout_action: step_with > task > global
    let timeout_action = step_with
        .and_then(|w| w.timeout_action.as_deref())
        .or({
            if task.timeout_action.is_empty() {
                None
            } else {
                Some(task.timeout_action.as_str())
            }
        })
        .unwrap_or(global_config.timeout.action.as_str());

    // Resolve kill_grace_secs: step_with > task > global
    let kill_grace_secs = step_with
        .and_then(|w| w.kill_grace_secs)
        .or({
            if task.kill_grace_secs > 0 {
                Some(task.kill_grace_secs)
            } else {
                None
            }
        })
        .unwrap_or(global_config.timeout.kill_grace_secs);

    let capture_stdout = stdout_mode == "capture";
    let stream_stdout = stdout_mode == "stream";
    let capture_stderr = stderr_mode == "capture";
    let stream_stderr = stderr_mode == "stream";

    let mut child = {
        let mut command = TokioCommand::new(&cmd);
        command.args(&args);
        if capture_stdout {
            command.stdout(Stdio::piped());
        } else if stream_stdout {
            command.stdout(Stdio::inherit());
        } else {
            command.stdout(Stdio::null());
        }
        if capture_stderr {
            command.stderr(Stdio::piped());
        } else if stream_stderr {
            command.stderr(Stdio::inherit());
        } else {
            command.stderr(Stdio::null());
        }
        command.stdin(Stdio::piped());

        if !task.inherit_env {
            command.env_clear();
        }
        for (k, v) in &env_vars {
            command.env(k, v);
        }
        if let Some(ref wd) = working_dir {
            command.current_dir(wd);
        }

        // We handle kill ourselves in the timeout path
        command.kill_on_drop(false);
        command
            .spawn()
            .map_err(|e| Error::Task(format!("Failed to spawn '{cmd}': {e}")))?
    };

    // Handle stdin with tmp_dir support for large payloads.
    if let Some(mut writer) = child.stdin.take() {
        match stdin_mode {
            "payload" => {
                let payload_str = serde_json::to_string(&ctx.job.payload).unwrap_or_default();
                write_stdin_maybe_tmp(
                    &mut writer,
                    payload_str.into_bytes(),
                    &ctx.job.id,
                    "stdin-payload",
                    &global_config.tmp_dir,
                )
                .await?;
            }
            "template" => {
                if let Some(tpl) = stdin_template {
                    let rendered = template.render(tpl, ctx)?;
                    write_stdin_maybe_tmp(
                        &mut writer,
                        rendered.into_bytes(),
                        &ctx.job.id,
                        "stdin-template",
                        &global_config.tmp_dir,
                    )
                    .await?;
                }
            }
            _ => {}
        }
        drop(writer);
    }

    // Take stdout/stderr for concurrent capture
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = stdout_handle {
            let _ = reader.read_to_end(&mut buf).await;
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = stderr_handle {
            let _ = reader.read_to_end(&mut buf).await;
        }
        buf
    });

    // Wait for child exit, with optional timeout and graceful termination
    let exit_status = if timeout_secs > 0 {
        match timeout(Duration::from_secs(timeout_secs), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => return Err(Error::Task(format!("Process error: {e}"))),
            Err(_elapsed) => {
                // Timeout fired — apply timeout_action
                if timeout_action == "term" {
                    send_sigterm(&child);
                    let grace = Duration::from_secs(kill_grace_secs);
                    if timeout(grace, child.wait()).await.is_err() {
                        // Still alive after grace — force kill
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                    }
                    return Err(Error::Timeout(format!(
                        "Task timed out after {timeout_secs}s (terminated with {kill_grace_secs}s grace)"
                    )));
                } else {
                    // Default: kill immediately
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return Err(Error::Timeout(format!(
                        "Task timed out after {timeout_secs}s"
                    )));
                }
            }
        }
    } else {
        child
            .wait()
            .await
            .map_err(|e| Error::Task(format!("Process error: {e}")))?
    };

    // Collect stdout/stderr
    let stdout_buf = stdout_task
        .await
        .map_err(|e| Error::Task(format!("stdout read task failed: {e}")))?;
    let stderr_buf = stderr_task
        .await
        .map_err(|e| Error::Task(format!("stderr read task failed: {e}")))?;

    let exit_code = exit_status.code().unwrap_or(-1);
    let duration_ms = start.elapsed().as_millis() as u64;

    let stdout_str = if capture_stdout {
        let s = String::from_utf8_lossy(&stdout_buf).to_string();
        if stdout_max_bytes > 0 && s.len() > stdout_max_bytes {
            return Err(Error::Task(format!(
                "stdout exceeded max_bytes limit of {}",
                stdout_max_bytes
            )));
        }
        Some(s)
    } else {
        None
    };

    let stderr_str = if capture_stderr {
        let s = String::from_utf8_lossy(&stderr_buf).to_string();
        if stderr_max_bytes > 0 && s.len() > stderr_max_bytes {
            return Err(Error::Task(format!(
                "stderr exceeded max_bytes limit of {}",
                stderr_max_bytes
            )));
        }
        Some(s)
    } else {
        None
    };

    if !success_exit_codes.contains(&exit_code) && !success_exit_codes.is_empty() {
        return Err(Error::Task(format!(
            "Exit code {exit_code} is not in success_exit_codes {:?}",
            success_exit_codes
        )));
    }

    let outputs = extract_outputs(step_outputs, &stdout_str);

    Ok(TaskRunResult {
        stdout: stdout_str,
        stderr: stderr_str,
        exit_code,
        duration_ms,
        outputs,
    })
}

/// Execute a docker-based task.
async fn run_docker(
    task: &config::TaskConfig,
    step_with: Option<&config::StepWithConfig>,
    step_outputs: Option<&config::StepOutputsConfig>,
    global_config: &config::GlobalConfig,
    ctx: &Context,
    template: &TemplateEngine,
) -> Result<TaskRunResult> {
    let docker_cfg = task
        .docker
        .as_ref()
        .ok_or_else(|| Error::Task("Docker config missing".to_string()))?;

    let start = std::time::Instant::now();

    // Apply StepWithConfig overrides for cmd (image)
    let image = if let Some(with) = step_with {
        if let Some(ref with_cmd) = with.cmd {
            template.render(with_cmd, ctx)?
        } else {
            template.render(&task.cmd, ctx)?
        }
    } else {
        template.render(&task.cmd, ctx)?
    };

    let args: Vec<String> = if let Some(with) = step_with {
        if let Some(ref with_args) = with.args {
            with_args
                .iter()
                .map(|a| template.render(a, ctx))
                .collect::<Result<Vec<_>>>()?
        } else {
            task.args
                .iter()
                .map(|a| template.render(a, ctx))
                .collect::<Result<Vec<_>>>()?
        }
    } else {
        task.args
            .iter()
            .map(|a| template.render(a, ctx))
            .collect::<Result<Vec<_>>>()?
    };

    let mut docker_args: Vec<String> = vec!["run".to_string()];

    if docker_cfg.pull != "missing" {
        docker_args.push(format!("--pull={}", docker_cfg.pull));
    }

    for flag in &docker_cfg.flags {
        for part in flag.split_whitespace() {
            docker_args.push(template.render(part, ctx)?);
        }
    }

    for mount in &docker_cfg.mounts {
        let rendered = template.render(mount, ctx)?;
        docker_args.push("-v".to_string());
        docker_args.push(rendered);
    }

    let env_vars = build_env(task, step_with, template, ctx)?;
    for (k, v) in &env_vars {
        docker_args.push("-e".to_string());
        docker_args.push(format!("{k}={v}"));
    }

    // Resolve working_dir: step_with > task
    let working_dir = step_with
        .and_then(|w| w.working_dir.as_ref())
        .or(task.working_dir.as_ref());
    if let Some(wd) = working_dir {
        let rendered = template.render(&wd.to_string_lossy(), ctx)?;
        docker_args.push("-w".to_string());
        docker_args.push(rendered);
    }

    docker_args.push(image);
    docker_args.extend(args);

    // Resolve stdin config: step_with > task
    let stdin_mode = step_with
        .and_then(|w| w.stdin.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stdin.mode.as_str());
    let stdin_template = step_with
        .and_then(|w| w.stdin.as_ref().and_then(|s| s.template.as_deref()))
        .or(task.stdin.template.as_deref());

    // Resolve stdout config: step_with > task
    let stdout_mode = step_with
        .and_then(|w| w.stdout.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stdout.mode.as_str());
    let stdout_max_bytes = step_with
        .and_then(|w| w.stdout.as_ref().map(|s| s.max_bytes))
        .unwrap_or(task.stdout.max_bytes);

    // Resolve stderr config: step_with > task
    let stderr_mode = step_with
        .and_then(|w| w.stderr.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stderr.mode.as_str());
    let stderr_max_bytes = step_with
        .and_then(|w| w.stderr.as_ref().map(|s| s.max_bytes))
        .unwrap_or(task.stderr.max_bytes);

    // Resolve success_exit_codes: step_with > task
    let success_exit_codes = step_with
        .and_then(|w| w.success_exit_codes.as_ref())
        .unwrap_or(&task.success_exit_codes);

    let timeout_secs = step_with
        .and_then(|w| w.timeout_secs)
        .or({
            if task.timeout_secs > 0 {
                Some(task.timeout_secs)
            } else {
                None
            }
        })
        .unwrap_or(global_config.timeout.step_secs);

    // Resolve timeout_action: step_with > task > global
    let timeout_action = step_with
        .and_then(|w| w.timeout_action.as_deref())
        .or({
            if task.timeout_action.is_empty() {
                None
            } else {
                Some(task.timeout_action.as_str())
            }
        })
        .unwrap_or(global_config.timeout.action.as_str());

    // Resolve kill_grace_secs: step_with > task > global
    let kill_grace_secs = step_with
        .and_then(|w| w.kill_grace_secs)
        .or({
            if task.kill_grace_secs > 0 {
                Some(task.kill_grace_secs)
            } else {
                None
            }
        })
        .unwrap_or(global_config.timeout.kill_grace_secs);

    let capture_stdout = stdout_mode == "capture";
    let stream_stdout = stdout_mode == "stream";
    let capture_stderr = stderr_mode == "capture";
    let stream_stderr = stderr_mode == "stream";

    let mut child = {
        let mut command = TokioCommand::new("docker");
        command.args(&docker_args);
        if capture_stdout {
            command.stdout(Stdio::piped());
        } else if stream_stdout {
            command.stdout(Stdio::inherit());
        } else {
            command.stdout(Stdio::null());
        }
        if capture_stderr {
            command.stderr(Stdio::piped());
        } else if stream_stderr {
            command.stderr(Stdio::inherit());
        } else {
            command.stderr(Stdio::null());
        }
        command.stdin(if stdin_mode == "none" {
            Stdio::null()
        } else {
            Stdio::piped()
        });
        if !task.inherit_env {
            command.env_clear();
        }
        command.kill_on_drop(false);
        command
            .spawn()
            .map_err(|e| Error::Task(format!("Failed to spawn docker: {e}")))?
    };

    if let Some(mut writer) = child.stdin.take() {
        match stdin_mode {
            "payload" => {
                let payload = serde_json::to_vec(&ctx.job.payload).unwrap_or_default();
                write_stdin_maybe_tmp(
                    &mut writer,
                    payload,
                    &ctx.job.id,
                    "docker-stdin-payload",
                    &global_config.tmp_dir,
                )
                .await?;
            }
            "template" => {
                if let Some(tpl) = stdin_template {
                    let rendered = template.render(tpl, ctx)?;
                    write_stdin_maybe_tmp(
                        &mut writer,
                        rendered.into_bytes(),
                        &ctx.job.id,
                        "docker-stdin-template",
                        &global_config.tmp_dir,
                    )
                    .await?;
                }
            }
            _ => {}
        }
        drop(writer);
    }

    // Take stdout/stderr for concurrent capture
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = stdout_handle {
            let _ = reader.read_to_end(&mut buf).await;
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut reader) = stderr_handle {
            let _ = reader.read_to_end(&mut buf).await;
        }
        buf
    });

    let exit_status = if timeout_secs > 0 {
        match timeout(Duration::from_secs(timeout_secs), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => return Err(Error::Task(format!("Docker process error: {e}"))),
            Err(_elapsed) => {
                // Timeout fired — apply timeout_action
                if timeout_action == "term" {
                    // Docker: SIGTERM propagates to container; then grace, then kill
                    send_sigterm(&child);
                    let grace = Duration::from_secs(kill_grace_secs);
                    if timeout(grace, child.wait()).await.is_err() {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                    }
                    return Err(Error::Timeout(format!(
                        "Docker task timed out after {timeout_secs}s (terminated with {kill_grace_secs}s grace)"
                    )));
                } else {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return Err(Error::Timeout(format!(
                        "Docker task timed out after {timeout_secs}s"
                    )));
                }
            }
        }
    } else {
        child
            .wait()
            .await
            .map_err(|e| Error::Task(format!("Docker process error: {e}")))?
    };

    // Collect stdout/stderr
    let stdout_buf = stdout_task
        .await
        .map_err(|e| Error::Task(format!("stdout read task failed: {e}")))?;
    let stderr_buf = stderr_task
        .await
        .map_err(|e| Error::Task(format!("stderr read task failed: {e}")))?;

    let exit_code = exit_status.code().unwrap_or(-1);
    let duration_ms = start.elapsed().as_millis() as u64;

    let stdout_str = if capture_stdout {
        let s = String::from_utf8_lossy(&stdout_buf).to_string();
        if stdout_max_bytes > 0 && s.len() > stdout_max_bytes {
            return Err(Error::Task(format!(
                "stdout exceeded max_bytes limit of {}",
                stdout_max_bytes
            )));
        }
        Some(s)
    } else {
        None
    };

    let stderr_str = if capture_stderr {
        let s = String::from_utf8_lossy(&stderr_buf).to_string();
        if stderr_max_bytes > 0 && s.len() > stderr_max_bytes {
            return Err(Error::Task(format!(
                "stderr exceeded max_bytes limit of {}",
                stderr_max_bytes
            )));
        }
        Some(s)
    } else {
        None
    };

    if !success_exit_codes.contains(&exit_code) && !success_exit_codes.is_empty() {
        return Err(Error::Task(format!(
            "Exit code {exit_code} is not in success_exit_codes {:?}",
            success_exit_codes
        )));
    }

    let outputs = extract_outputs(step_outputs, &stdout_str);

    Ok(TaskRunResult {
        stdout: stdout_str,
        stderr: stderr_str,
        exit_code,
        duration_ms,
        outputs,
    })
}

/// Execute a WebAssembly task using wasmtime 45.x WASIp1.
async fn run_wasm(
    task: &config::TaskConfig,
    step_with: Option<&config::StepWithConfig>,
    step_outputs: Option<&config::StepOutputsConfig>,
    global_config: &config::GlobalConfig,
    ctx: &Context,
    template: &TemplateEngine,
) -> Result<TaskRunResult> {
    let wasm_cfg = task
        .wasm
        .as_ref()
        .ok_or_else(|| Error::Task("Wasm config missing".to_string()))?;

    let start = std::time::Instant::now();

    // Apply StepWithConfig overrides for cmd
    let wasm_path_str = if let Some(with) = step_with {
        if let Some(ref with_cmd) = with.cmd {
            template.render(with_cmd, ctx)?
        } else {
            template.render(&task.cmd, ctx)?
        }
    } else {
        template.render(&task.cmd, ctx)?
    };
    let wasm_path = std::path::Path::new(&wasm_path_str);

    if !wasm_path.exists() {
        return Err(Error::Task(format!("Wasm file not found: {wasm_path_str}")));
    }

    // Render args
    let args: Vec<String> = if let Some(with) = step_with {
        if let Some(ref with_args) = with.args {
            with_args
                .iter()
                .map(|a| template.render(a, ctx))
                .collect::<Result<Vec<_>>>()?
        } else {
            task.args
                .iter()
                .map(|a| template.render(a, ctx))
                .collect::<Result<Vec<_>>>()?
        }
    } else {
        task.args
            .iter()
            .map(|a| template.render(a, ctx))
            .collect::<Result<Vec<_>>>()?
    };

    let engine = create_wasm_engine(wasm_cfg.max_memory_pages)?;
    let module = wasmtime::Module::from_file(&engine, &wasm_path_str)
        .map_err(|e| Error::Task(format!("Wasm module compile error: {e}")))?;

    let mut linker: wasmtime::Linker<wasmtime_wasi::p1::WasiP1Ctx> = wasmtime::Linker::new(&engine);
    wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| t)
        .map_err(|e| Error::Task(format!("WASI linker error: {e}")))?;

    // Build WASI context
    let mut wasi_builder = wasmtime_wasi::WasiCtxBuilder::new();

    // Args: first arg is the program name (module path)
    let mut all_args = vec![wasm_path_str.clone()];
    all_args.extend(args);
    wasi_builder.args(&all_args);

    let env_vars = build_env(task, step_with, template, ctx)?;
    for (key, value) in env_vars {
        wasi_builder.env(&key, &value);
    }

    // Resolve stdout config: step_with > task
    let stdout_mode = step_with
        .and_then(|w| w.stdout.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stdout.mode.as_str());
    let stdout_max_bytes = step_with
        .and_then(|w| w.stdout.as_ref().map(|s| s.max_bytes))
        .unwrap_or(task.stdout.max_bytes);

    // Resolve stderr config: step_with > task
    let stderr_mode = step_with
        .and_then(|w| w.stderr.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stderr.mode.as_str());
    let stderr_max_bytes = step_with
        .and_then(|w| w.stderr.as_ref().map(|s| s.max_bytes))
        .unwrap_or(task.stderr.max_bytes);

    let capture_stdout = stdout_mode == "capture";
    let capture_stderr = stderr_mode == "capture";
    let stdout_pipe = capture_stdout
        .then(|| wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(stdout_max_bytes.max(1)));
    let stderr_pipe = capture_stderr
        .then(|| wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(stderr_max_bytes.max(1)));

    if let Some(pipe) = stdout_pipe.clone() {
        wasi_builder.stdout(pipe);
    } else if stdout_mode == "stream" {
        wasi_builder.inherit_stdout();
    }

    if let Some(pipe) = stderr_pipe.clone() {
        wasi_builder.stderr(pipe);
    } else if stderr_mode == "stream" {
        wasi_builder.inherit_stderr();
    }

    // Preopened dirs
    for dir_pair in &wasm_cfg.dirs {
        let parts: Vec<&str> = dir_pair.splitn(2, ':').collect();
        if parts.len() == 2 {
            let host_path_str = template.render(parts[0], ctx)?;
            let host_path = std::path::Path::new(&host_path_str);
            if host_path.exists() {
                let _ = wasi_builder.preopened_dir(
                    host_path,
                    parts[1],
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                );
            }
        }
    }

    // Handle stdin
    // Resolve stdin config: step_with > task
    let stdin_mode = step_with
        .and_then(|w| w.stdin.as_ref().map(|s| s.mode.as_str()))
        .unwrap_or(task.stdin.mode.as_str());
    let stdin_template = step_with
        .and_then(|w| w.stdin.as_ref().and_then(|s| s.template.as_deref()))
        .or(task.stdin.template.as_deref());
    if stdin_mode != "none" {
        // Feed stdin data directly into the WASI context via an in-memory pipe.
        let stdin_data = match stdin_mode {
            "payload" => Some(serde_json::to_vec(&ctx.job.payload).unwrap_or_default()),
            "template" => {
                if let Some(tpl) = stdin_template {
                    let rendered = template.render(tpl, ctx)?;
                    Some(rendered.into_bytes())
                } else {
                    None
                }
            }
            _ => None,
        };

        if let Some(data) = stdin_data {
            wasi_builder.stdin(wasmtime_wasi::p2::pipe::MemoryInputPipe::new(data));
        }
    }

    let wasi_ctx = wasi_builder.build_p1();

    let mut store = wasmtime::Store::new(&engine, wasi_ctx);

    // Set fuel if configured
    if wasm_cfg.fuel > 0 {
        store
            .set_fuel(wasm_cfg.fuel)
            .map_err(|e| Error::Task(format!("Wasm fuel config error: {e}")))?;
    }

    let instance = linker
        .instantiate_async(&mut store, &module)
        .await
        .map_err(|e| Error::Task(format!("Wasm instantiate error: {e}")))?;

    let timeout_secs = step_with
        .and_then(|w| w.timeout_secs)
        .or(if task.timeout_secs > 0 {
            Some(task.timeout_secs)
        } else {
            None
        })
        .unwrap_or(global_config.timeout.step_secs);

    let (exit_code, wasm_error) = {
        let exec = async {
            // Try calling _start, then main for modules that export it directly.
            if let Ok(func) = instance.get_typed_func::<(), ()>(&mut store, "_start") {
                func.call_async(&mut store, ()).await
            } else if let Ok(func) = instance.get_typed_func::<(), ()>(&mut store, "main") {
                func.call_async(&mut store, ()).await
            } else {
                Ok(())
            }
        };

        let result = if timeout_secs > 0 {
            match timeout(Duration::from_secs(timeout_secs), exec).await {
                Ok(r) => r,
                Err(_elapsed) => {
                    return Err(Error::Timeout(format!(
                        "Wasm task timed out after {timeout_secs}s"
                    )));
                }
            }
        } else {
            exec.await
        };

        match result {
            Ok(()) => (0, None),
            Err(e) => {
                // WASI proc_exit(n) always traps with an I32Exit error.
                // proc_exit(0) is a successful exit so treat it like a
                // normal return. Non-zero n is reported as the exit code.
                if let Some(exit) = e.downcast_ref::<wasmtime_wasi::I32Exit>() {
                    (exit.0, None)
                } else {
                    (-1, Some(wasm_error_string(e)))
                }
            }
        }
    };

    if let Some(err) = wasm_error {
        return Err(Error::Task(format!("Wasm execution error: {err}")));
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    let stdout_str = if let Some(pipe) = stdout_pipe {
        let bytes = pipe.contents();
        if stdout_max_bytes > 0 && bytes.len() > stdout_max_bytes {
            return Err(Error::Task(format!(
                "stdout exceeded max_bytes limit of {}",
                stdout_max_bytes
            )));
        }
        Some(String::from_utf8_lossy(&bytes).to_string())
    } else {
        None
    };

    let stderr_str = if let Some(pipe) = stderr_pipe {
        let bytes = pipe.contents();
        if stderr_max_bytes > 0 && bytes.len() > stderr_max_bytes {
            return Err(Error::Task(format!(
                "stderr exceeded max_bytes limit of {}",
                stderr_max_bytes
            )));
        }
        Some(String::from_utf8_lossy(&bytes).to_string())
    } else {
        None
    };

    let outputs = extract_outputs(step_outputs, &stdout_str);

    Ok(TaskRunResult {
        stdout: stdout_str,
        stderr: stderr_str,
        exit_code,
        duration_ms,
        outputs,
    })
}

fn create_wasm_engine(max_memory_pages: u32) -> Result<wasmtime::Engine> {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);
    let max_memory_bytes = u64::from(max_memory_pages.max(1)) * WASM_PAGE_SIZE_BYTES;
    config.memory_reservation(max_memory_bytes);
    config.memory_reservation_for_growth(0);
    let engine = wasmtime::Engine::new(&config)
        .map_err(|e| Error::Task(format!("Wasm engine creation error: {e}")))?;
    Ok(engine)
}

fn wasm_error_string(e: wasmtime::Error) -> String {
    let err_str = format!("{e:?}");
    if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
        format!("Wasm trap: {trap}")
    } else {
        err_str
    }
}

/// Send SIGTERM to a child process on Unix platforms.
///
/// This is a best-effort graceful termination request. On Unix it sends the
/// actual SIGTERM signal via libc. The caller (the timeout path) follows up
/// with a grace period and then `start_kill` if the process is still alive.
///
/// On non-Unix platforms this function is intentionally a no-op because
/// SIGTERM-like graceful signals are not available. The timeout path
/// unconditionally calls [`tokio::process::Child::start_kill`] after the
/// grace period to terminate the process through the platform's hard-kill
/// mechanism.
#[cfg(unix)]
fn send_sigterm(child: &tokio::process::Child) {
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
}

#[cfg(not(unix))]
#[allow(clippy::trivially_copy_pass_by_ref)]
fn send_sigterm(_child: &tokio::process::Child) {
    // On non-Unix platforms, graceful SIGTERM is not available. The caller
    // (the timeout path) still follows up with `start_kill` after the grace
    // period, so the process will be terminated through the platform's
    // hard-kill mechanism.
}

/// Build the environment map for a subprocess.
fn build_env(
    task: &config::TaskConfig,
    step_with: Option<&config::StepWithConfig>,
    template: &TemplateEngine,
    ctx: &Context,
) -> Result<HashMap<String, String>> {
    let mut env: HashMap<String, String> = HashMap::new();

    for (k, v) in &task.env {
        let rendered = template.render(v, ctx)?;
        env.insert(k.clone(), rendered);
    }

    if let Some(with) = step_with {
        for (k, v) in &with.env {
            let rendered = template.render(v, ctx)?;
            env.insert(k.clone(), rendered);
        }
    }

    Ok(env)
}

/// Extract named outputs from stdout based on step outputs config.
fn extract_outputs(
    step_outputs: Option<&config::StepOutputsConfig>,
    stdout: &Option<String>,
) -> HashMap<String, serde_json::Value> {
    let mut outputs = HashMap::new();

    let Some(stdout_str) = stdout else {
        return outputs;
    };

    let Some(outputs_cfg) = step_outputs else {
        return outputs;
    };

    match outputs_cfg.format.as_str() {
        "json" => {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(stdout_str) {
                for field in &outputs_cfg.fields {
                    let value = if field.key.is_empty() {
                        parsed.clone()
                    } else {
                        navigate_json(&parsed, &field.key).unwrap_or(serde_json::Value::Null)
                    };
                    outputs.insert(field.name.clone(), value);
                }
            }
        }
        "text" => {
            for field in &outputs_cfg.fields {
                outputs.insert(
                    field.name.clone(),
                    serde_json::Value::String(stdout_str.clone()),
                );
            }
        }
        other => {
            tracing::warn!("Unknown output format: {other}");
        }
    }

    outputs
}

fn navigate_json(value: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
    let mut current = value;
    for part in path.split('.') {
        match current {
            serde_json::Value::Object(obj) => {
                current = obj.get(part)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

/// Write stdin data to a child process, using a temp file under `tmp_dir`
/// for large payloads to avoid buffering everything in memory.
async fn write_stdin_maybe_tmp(
    writer: &mut tokio::process::ChildStdin,
    data: Vec<u8>,
    job_id: &str,
    suffix: &str,
    tmp_dir: &PathBuf,
) -> Result<()> {
    // For small payloads, just write directly.
    if data.len() <= DIRECT_STDIN_MAX_BYTES {
        writer
            .write_all(&data)
            .await
            .map_err(|e| Error::Task(format!("Failed to write stdin: {e}")))?;
        return Ok(());
    }

    // For larger payloads, write to a temp file and then stream it.
    let _ = std::fs::create_dir_all(tmp_dir);
    let safe_id = sanitize_filename(job_id);
    let tmp_path = tmp_dir.join(format!("bria-{suffix}-{safe_id}"));

    std::fs::write(&tmp_path, &data)
        .map_err(|e| Error::Task(format!("Failed to write stdin tmp file: {e}")))?;

    // RAII guard ensures the temp file is cleaned up on any return path.
    let guard = TempFileGuard::new(tmp_path.clone());

    // Stream the file into the child's stdin
    let mut file = tokio::fs::File::open(&tmp_path)
        .await
        .map_err(|e| Error::Task(format!("Failed to open stdin tmp file: {e}")))?;

    tokio::io::copy(&mut file, writer)
        .await
        .map_err(|e| Error::Task(format!("Failed to stream stdin from tmp file: {e}")))?;

    // Success — disarm the guard and remove the file explicitly so we can
    // surface removal errors that might indicate a filesystem problem.
    guard.disarm();
    std::fs::remove_file(&tmp_path)
        .map_err(|e| Error::Task(format!("Failed to remove stdin tmp file: {e}")))?;
    Ok(())
}

/// Sanitize a string for use as a safe filename component.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// RAII guard that removes a temp file on drop, unless explicitly disarmed.
struct TempFileGuard(Option<PathBuf>);

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self(Some(path))
    }

    /// Disarm the guard — the file will not be removed on drop.
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(ref path) = self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}
