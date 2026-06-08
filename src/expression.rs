use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::context::Context;
use crate::error::{Error, Result};

/// Expression evaluator for CEL expressions used in Bria configuration.
pub struct Evaluator {
    pipeline_id: Option<String>,
    programs: Mutex<HashMap<String, Arc<cel::Program>>>,
}

impl Evaluator {
    pub fn new() -> Self {
        Self {
            pipeline_id: None,
            programs: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_pipeline_id(pipeline_id: impl Into<String>) -> Self {
        Self {
            pipeline_id: Some(pipeline_id.into()),
            programs: Mutex::new(HashMap::new()),
        }
    }

    pub fn eval_bool(&self, expr: &str, ctx: &Context) -> Result<bool> {
        let program = self.compile_program(expr)?;
        let cel_ctx = self.build_context(ctx);
        let result = program
            .execute(&cel_ctx)
            .map_err(|e| Error::Expression(format!("CEL execution error: {e:?}")))?;
        match result {
            cel::Value::Bool(b) => Ok(b),
            other => Err(Error::Expression(format!(
                "Expected boolean result for expression '{expr}', got: {other:?}"
            ))),
        }
    }

    pub fn eval_value(&self, expr: &str, ctx: &Context) -> Result<JsonValue> {
        let program = self.compile_program(expr)?;
        let cel_ctx = self.build_context(ctx);
        let result = program
            .execute(&cel_ctx)
            .map_err(|e| Error::Expression(format!("CEL execution error: {e:?}")))?;
        Ok(cel_value_to_json(result))
    }

    pub fn eval_merge_bool(&self, expr: &str, left: &Context, right: &Context) -> Result<bool> {
        let program = self.compile_program(expr)?;
        let mut cel_ctx = cel::Context::default();
        cel_ctx.add_variable_from_value("a", self.context_to_cel_value(left));
        cel_ctx.add_variable_from_value("b", self.context_to_cel_value(right));
        let result = program
            .execute(&cel_ctx)
            .map_err(|e| Error::Expression(format!("CEL execution error: {e:?}")))?;
        match result {
            cel::Value::Bool(b) => Ok(b),
            other => Err(Error::Expression(format!(
                "Expected boolean result for merge expression '{expr}', got: {other:?}"
            ))),
        }
    }

    fn build_context(&self, ctx: &Context) -> cel::Context<'_> {
        let mut cel_ctx = cel::Context::default();

        if let cel::Value::Map(root) = self.context_to_cel_value(ctx) {
            for (key, value) in root.map.iter() {
                if let cel::objects::Key::String(name) = key {
                    cel_ctx.add_variable_from_value(name.as_str(), value.clone());
                }
            }
        }

        cel_ctx
    }

    fn context_to_cel_value(&self, ctx: &Context) -> cel::Value {
        // job
        let mut job_hash: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
        job_hash.insert(
            cel::objects::Key::String(arc_str("id")),
            cel::Value::String(arc_str(&ctx.job.id)),
        );
        job_hash.insert(
            cel::objects::Key::String(arc_str("source")),
            cel::Value::String(arc_str(&ctx.job.source)),
        );
        job_hash.insert(
            cel::objects::Key::String(arc_str("payload")),
            json_to_cel_value(&ctx.job.payload),
        );
        // job.labels
        let mut labels_hash: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
        for (k, v) in &ctx.job.labels {
            labels_hash.insert(
                cel::objects::Key::String(arc_str(k)),
                cel::Value::String(arc_str(v)),
            );
        }
        job_hash.insert(
            cel::objects::Key::String(arc_str("labels")),
            cel::Value::Map(labels_hash.into()),
        );
        let job_value = cel::Value::Map(job_hash.into());

        // steps
        let mut steps_hash: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
        for (step_id, result) in &ctx.steps {
            let mut step_hash: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
            step_hash.insert(
                cel::objects::Key::String(arc_str("stdout")),
                cel::Value::String(arc_str(&result.stdout.clone().unwrap_or_default())),
            );
            step_hash.insert(
                cel::objects::Key::String(arc_str("stderr")),
                cel::Value::String(arc_str(&result.stderr.clone().unwrap_or_default())),
            );
            step_hash.insert(
                cel::objects::Key::String(arc_str("exit_code")),
                cel::Value::Int(result.exit_code.into()),
            );
            let mut outputs_hash: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
            for (name, val) in &result.outputs {
                outputs_hash.insert(
                    cel::objects::Key::String(arc_str(name)),
                    json_to_cel_value(val),
                );
            }
            step_hash.insert(
                cel::objects::Key::String(arc_str("outputs")),
                cel::Value::Map(outputs_hash.into()),
            );
            steps_hash.insert(
                cel::objects::Key::String(arc_str(step_id)),
                cel::Value::Map(step_hash.into()),
            );
        }
        let steps_value = cel::Value::Map(steps_hash.into());

        // pipeline
        let mut pipeline_hash: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
        pipeline_hash.insert(
            cel::objects::Key::String(arc_str("id")),
            cel::Value::String(arc_str(self.pipeline_id.as_deref().unwrap_or_default())),
        );
        let pipeline_value = cel::Value::Map(pipeline_hash.into());

        let mut root: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
        root.insert(cel::objects::Key::String(arc_str("job")), job_value);
        root.insert(cel::objects::Key::String(arc_str("steps")), steps_value);
        root.insert(
            cel::objects::Key::String(arc_str("pipeline")),
            pipeline_value,
        );
        cel::Value::Map(root.into())
    }

    fn compile_program(&self, expr: &str) -> Result<Arc<cel::Program>> {
        if let Some(program) = self
            .programs
            .lock()
            .expect("CEL program cache mutex poisoned")
            .get(expr)
            .cloned()
        {
            return Ok(program);
        }

        let program = Arc::new(
            cel::Program::compile(expr)
                .map_err(|e| Error::Expression(format!("CEL parse error: {e}")))?,
        );
        self.programs
            .lock()
            .expect("CEL program cache mutex poisoned")
            .insert(expr.to_string(), program.clone());
        Ok(program)
    }
}

impl Default for Evaluator {
    fn default() -> Self {
        Self::new()
    }
}

fn arc_str(s: &str) -> Arc<String> {
    Arc::new(s.to_string())
}

fn cel_value_to_json(val: cel::Value) -> JsonValue {
    match val {
        cel::Value::Null => JsonValue::Null,
        cel::Value::Bool(b) => JsonValue::Bool(b),
        cel::Value::Int(i) => JsonValue::Number(i.into()),
        cel::Value::UInt(u) => JsonValue::Number(u.into()),
        cel::Value::Float(f) => {
            serde_json::Number::from_f64(f).map_or(JsonValue::Null, JsonValue::Number)
        }
        cel::Value::String(s) => JsonValue::String(s.to_string()),
        cel::Value::List(list) => {
            JsonValue::Array(list.iter().map(|v| cel_value_to_json(v.clone())).collect())
        }
        cel::Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (key, value) in map.map.iter() {
                let key_str = match key {
                    cel::objects::Key::String(s) => s.to_string(),
                    cel::objects::Key::Int(i) => i.to_string(),
                    cel::objects::Key::Uint(u) => u.to_string(),
                    cel::objects::Key::Bool(b) => b.to_string(),
                };
                obj.insert(key_str, cel_value_to_json(value.clone()));
            }
            JsonValue::Object(obj)
        }
        cel::Value::Bytes(_) => JsonValue::Null,
        cel::Value::Duration(_) => JsonValue::Null,
        cel::Value::Timestamp(dt) => JsonValue::String(dt.to_rfc3339()),
        cel::Value::Function(_, _) => JsonValue::Null,
        cel::Value::Opaque(_) => JsonValue::Null,
        #[allow(unreachable_patterns)]
        other => {
            tracing::warn!("Unknown CEL value variant encountered: {other:?}; mapping to null");
            JsonValue::Null
        }
    }
}

fn json_to_cel_value(val: &JsonValue) -> cel::Value {
    match val {
        JsonValue::Null => cel::Value::Null,
        JsonValue::Bool(b) => cel::Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                cel::Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                cel::Value::UInt(u)
            } else if let Some(f) = n.as_f64() {
                cel::Value::Float(f)
            } else {
                cel::Value::Null
            }
        }
        JsonValue::String(s) => cel::Value::String(arc_str(s)),
        JsonValue::Array(arr) => {
            let vals: Vec<cel::Value> = arr.iter().map(json_to_cel_value).collect();
            cel::Value::List(Arc::new(vals))
        }
        JsonValue::Object(obj) => {
            let mut map: HashMap<cel::objects::Key, cel::Value> = HashMap::new();
            for (k, v) in obj {
                map.insert(cel::objects::Key::String(arc_str(k)), json_to_cel_value(v));
            }
            cel::Value::Map(map.into())
        }
    }
}
