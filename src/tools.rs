//! Tools for models (spec: Tools for models): `sql`, `describe`, `diff`, `dry_run`, `checkpoint`,
//! `rewind` and `done`, bound to one run, with descriptions written for models. The descriptions
//! are the specification's own `tools.json`, embedded unchanged.
//!
//! Wire them into any agent loop: hand [`anthropic_tools`] (or [`tool_specs`]) to the model, and
//! answer each tool call with [`run_tool`].

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{Error, Result};
use crate::json::stable_json;
use crate::run::{Run, SqlOptions};

const TOOLS_JSON: &str = include_str!("../spec/tools.json");

/// One tool: its name, a description for models, and a JSON Schema of its input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input: Value,
}

#[derive(Deserialize)]
struct ToolsFile {
    tools: Vec<ToolSpec>,
}

static SPECS: LazyLock<Vec<ToolSpec>> = LazyLock::new(|| {
    serde_json::from_str::<ToolsFile>(TOOLS_JSON)
        .expect("spec/tools.json parses")
        .tools
});

/// The seven tools, in the specification's order.
pub fn tool_specs() -> &'static [ToolSpec] {
    &SPECS
}

/// The tools in the Anthropic Messages API shape (`name`, `description`, `input_schema`).
pub fn anthropic_tools() -> Vec<Value> {
    tool_specs()
        .iter()
        .map(|t| json!({ "name": t.name, "description": t.description, "input_schema": t.input }))
        .collect()
}

/// The tools in the OpenAI function-calling shape.
pub fn openai_tools() -> Vec<Value> {
    tool_specs()
        .iter()
        .map(|t| json!({ "type": "function", "function": { "name": t.name, "description": t.description, "parameters": t.input } }))
        .collect()
}

/// What a tool call returns: text for the model, whether it is an error, and the value.
#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub text: String,
    pub is_error: bool,
    pub value: Option<Value>,
}

fn str_arg<'a>(input: &'a Value, k: &str) -> Option<&'a str> {
    input.get(k).and_then(Value::as_str)
}

async fn call(run: &Run, name: &str, input: &Value) -> Result<Value> {
    Ok(match name {
        "sql" => {
            let statement = str_arg(input, "statement").unwrap_or("");
            let opts = SqlOptions {
                reason: str_arg(input, "reason").map(str::to_string),
                ..Default::default()
            };
            let r = run.sql_opts(statement, Default::default(), opts).await?;
            // the statement committed: never report it as failed over a value we cannot convert
            let rows: Vec<Value> = r
                .rows
                .iter()
                .map(|row| Value::Object(crate::json::row_to_json_lossy(row)))
                .collect();
            json!({ "rows": rows, "rowCount": r.row_count, "truncated": r.truncated })
        }
        "describe" => run.describe(str_arg(input, "schema")).await?,
        "diff" => serde_json::to_value(run.diff().await?)?,
        "dry_run" => {
            let statements: Vec<String> = input
                .get("statements")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            run.dry_run(&statements, true).await?
        }
        "checkpoint" => {
            json!({ "checkpoint": run.checkpoint(str_arg(input, "name").unwrap_or("")).await? })
        }
        "rewind" => run.rewind(str_arg(input, "name").unwrap_or("")).await?,
        "done" => {
            let summary = str_arg(input, "summary").unwrap_or("").to_string();
            let title: String = summary
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(100)
                .collect();
            let title = if title.is_empty() { None } else { Some(title) };
            let r = run.submit_with(title, Some(summary)).await?;
            json!({ "change_request": r.id(), "note": "a person reviews it now" })
        }
        other => return Err(Error::other(format!("unknown tool {other}"))),
    })
}

/// Run one tool on a run. The result is text for a model; errors are returned as text with
/// `is_error`, never as `Err`.
pub async fn run_tool(run: &Run, name: &str, input: &Value) -> ToolOutput {
    match call(run, name, input).await {
        Ok(v) => ToolOutput {
            text: stable_json(&v),
            is_error: false,
            value: Some(v),
        },
        Err(e) => ToolOutput {
            text: e.to_string(),
            is_error: true,
            value: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seven_tools() {
        let names: Vec<&str> = tool_specs().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "sql",
                "describe",
                "diff",
                "dry_run",
                "checkpoint",
                "rewind",
                "done"
            ]
        );
        assert_eq!(
            anthropic_tools()[0]["input_schema"]["required"][0],
            "statement"
        );
    }
}
