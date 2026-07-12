use minijinja::{Environment, Value};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::context::Context;
use crate::error::Result;

/// Template engine for rendering placeholders in configuration values.
/// Uses minijinja with strict undefined behavior — missing values are errors.
pub struct TemplateEngine {
    cache: Mutex<TemplateCache>,
    env_snapshot: HashMap<String, Value>,
}

/// A template environment and its source-to-name index share one lock so a
/// cache miss cannot interleave with another insertion.
struct TemplateCache {
    env: Environment<'static>,
    names: HashMap<String, String>,
}

impl TemplateEngine {
    /// Create a new template engine.
    pub fn new() -> Self {
        let mut env = Environment::new();
        // Strict mode: undefined variables cause errors, never silently substituted
        env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
        let env_snapshot = std::env::vars()
            .map(|(key, val)| (key, Value::from(val)))
            .collect();
        Self {
            cache: Mutex::new(TemplateCache {
                env,
                names: HashMap::new(),
            }),
            env_snapshot,
        }
    }

    /// Render a template string with the given context.
    /// The context provides job, steps, env, now, and now_unix variables.
    pub fn render(&self, template: &str, ctx: &Context) -> Result<String> {
        let vars = Value::from(self.build_template_vars(ctx));
        self.render_vars(template, &vars)
    }

    /// Build the template variable context from a Context.
    fn build_template_vars(&self, ctx: &Context) -> HashMap<String, Value> {
        let mut vars = HashMap::new();

        // job.*
        let mut job_map = HashMap::new();
        job_map.insert("id".to_string(), Value::from(ctx.job.id.clone()));
        job_map.insert("source".to_string(), Value::from(ctx.job.source.clone()));

        // job.payload.<field>
        let payload = serde_json_to_value(&ctx.job.payload);
        job_map.insert("payload".to_string(), payload);

        // job.labels
        let mut labels_map = HashMap::new();
        for (k, v) in &ctx.job.labels {
            labels_map.insert(k.clone(), Value::from(v.clone()));
        }
        job_map.insert("labels".to_string(), Value::from(labels_map));

        vars.insert("job".to_string(), Value::from(job_map));

        // steps.<id>.*
        let mut steps_map = HashMap::new();
        for (step_id, result) in &ctx.steps {
            let mut step_map = HashMap::new();
            step_map.insert(
                "stdout".to_string(),
                Value::from(result.stdout.clone().unwrap_or_default()),
            );
            step_map.insert(
                "stderr".to_string(),
                Value::from(result.stderr.clone().unwrap_or_default()),
            );
            step_map.insert(
                "exit_code".to_string(),
                Value::from(i64::from(result.exit_code)),
            );

            // steps.<id>.outputs.<name>
            let mut outputs_map = HashMap::new();
            for (name, val) in &result.outputs {
                outputs_map.insert(name.clone(), serde_json_to_value(val));
            }
            step_map.insert("outputs".to_string(), Value::from(outputs_map));

            steps_map.insert(step_id.clone(), Value::from(step_map));
        }
        vars.insert("steps".to_string(), Value::from(steps_map));

        // env.* — OS environment variables at runtime
        vars.insert("env".to_string(), Value::from(self.env_snapshot.clone()));

        // now and now_unix
        let now = chrono::Utc::now();
        vars.insert("now".to_string(), Value::from(now.to_rfc3339()));
        vars.insert("now_unix".to_string(), Value::from(now.timestamp()));

        vars
    }

    /// Render a template string with additional pipeline result context.
    /// Used for sink templates.
    pub fn render_result(
        &self,
        template: &str,
        ctx: &Context,
        pipeline_id: &str,
        result_status: &str,
        duration_ms: u64,
        occurred_at: &str,
    ) -> Result<String> {
        let mut vars = self.build_template_vars(ctx);

        let mut pipeline_map = HashMap::new();
        pipeline_map.insert("id".to_string(), Value::from(pipeline_id.to_string()));
        vars.insert("pipeline".to_string(), Value::from(pipeline_map));

        let mut result_map = HashMap::new();
        result_map.insert("status".to_string(), Value::from(result_status.to_string()));
        result_map.insert("duration_ms".to_string(), Value::from(duration_ms));
        vars.insert("result".to_string(), Value::from(result_map));

        vars.insert(
            "occurred_at".to_string(),
            Value::from(occurred_at.to_string()),
        );

        let vars = Value::from(vars);

        self.render_vars(template, &vars)
    }

    /// Render while holding the combined cache lock exactly once. MiniJinja
    /// templates borrow the environment, so both lookup and rendering must be
    /// completed before releasing it.
    fn render_vars(&self, source: &str, vars: &Value) -> Result<String> {
        let mut cache = self.cache.lock().expect("template cache mutex poisoned");
        let name = match cache.names.get(source) {
            Some(name) => name.clone(),
            None => {
                let name = format!("inline:{}", cache.names.len());
                cache
                    .env
                    .add_template_owned(name.clone(), source.to_string())
                    .map_err(crate::error::Error::Template)?;
                cache.names.insert(source.to_string(), name.clone());
                name
            }
        };
        cache
            .env
            .get_template(&name)
            .map_err(crate::error::Error::Template)?
            .render(vars)
            .map_err(crate::error::Error::Template)
    }
}

impl Default for TemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a serde_json::Value to a minijinja Value.
fn serde_json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::UNDEFINED,
        serde_json::Value::Bool(b) => Value::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(f) = n.as_f64() {
                Value::from(f)
            } else {
                Value::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            let vals: Vec<Value> = arr.iter().map(serde_json_to_value).collect();
            Value::from(vals)
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), serde_json_to_value(v));
            }
            Value::from(map)
        }
    }
}
