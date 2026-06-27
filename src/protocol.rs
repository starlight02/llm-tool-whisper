use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{AppError, AppResult};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiProtocol {
    Chat,
    Responses,
    Messages,
}

impl ApiProtocol {
    /// The upstream path component appended to the provider `base_url`.
    pub fn upstream_path(self) -> &'static str {
        match self {
            Self::Chat => "chat/completions",
            Self::Responses => "responses",
            Self::Messages => "messages",
        }
    }

    pub fn as_path_label(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Responses => "responses",
            Self::Messages => "messages",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

/// Extract a flat text representation from a message content value, supporting
/// strings, arrays of parts and the common text-bearing object shapes across
/// all three upstream protocols.
pub fn content_to_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(content_part_to_text)
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) => {
            if let Some(text) = map
                .get("text")
                .or_else(|| map.get("input_text"))
                .or_else(|| map.get("output_text"))
                .and_then(Value::as_str)
            {
                text.to_string()
            } else if let Some(content) = map.get("content") {
                content_to_text(content)
            } else {
                String::new()
            }
        }
        _ => String::new(),
    }
}

fn content_part_to_text(value: &Value) -> Option<String> {
    if let Some(text) = value
        .get("text")
        .or_else(|| value.get("input_text"))
        .or_else(|| value.get("output_text"))
        .and_then(Value::as_str)
    {
        return Some(text.to_string());
    }
    if value.get("type").and_then(Value::as_str) == Some("tool_result") {
        return Some(format!(
            "<tool_result>{}</tool_result>",
            content_to_text(value.get("content").unwrap_or(&Value::Null))
        ));
    }
    None
}

/// Extract the assistant text from a non-streaming upstream JSON response.
/// Used to detect XML tool calls and to drive the tool loop.
pub fn extract_text(protocol: ApiProtocol, value: &Value) -> AppResult<String> {
    match protocol {
        ApiProtocol::Chat => value
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| AppError::Upstream(format!("missing chat content: {value}"))),
        ApiProtocol::Responses => {
            if let Some(text) = value.get("output_text").and_then(Value::as_str) {
                return Ok(text.to_string());
            }

            let text = value
                .get("output")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|item| item.get("content").and_then(Value::as_array))
                .flatten()
                .map(|part| {
                    part.get("text")
                        .or_else(|| part.get("output_text"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| content_to_text(part))
                })
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n");

            if text.is_empty() {
                Err(AppError::Upstream(format!(
                    "missing response text: {value}"
                )))
            } else {
                Ok(text)
            }
        }
        ApiProtocol::Messages => {
            let text = value
                .get("content")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            if text.is_empty() {
                Err(AppError::Upstream(format!("missing message text: {value}")))
            } else {
                Ok(text)
            }
        }
    }
}

/// Extract the delta text from a single upstream SSE JSON frame.
pub fn extract_stream_delta(protocol: ApiProtocol, value: &Value) -> Option<String> {
    match protocol {
        ApiProtocol::Chat => value
            .pointer("/choices/0/delta/content")
            .or_else(|| value.pointer("/choices/0/message/content"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        ApiProtocol::Responses => value
            .get("delta")
            .or_else(|| value.get("text"))
            .or_else(|| value.get("output_text"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        ApiProtocol::Messages => value
            .pointer("/delta/text")
            .or_else(|| value.pointer("/content_block/text"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    }
}

/// Collect tools defined in a client request body, regardless of protocol.
pub fn collect_tools(value: &Value) -> AppResult<Vec<ToolDefinition>> {
    let mut tools = Vec::new();

    if let Some(items) = value.get("tools").and_then(Value::as_array) {
        for item in items {
            if item.get("type").and_then(Value::as_str) == Some("function") {
                if let Some(function) = item.get("function") {
                    tools.push(openai_function_tool(function)?);
                }
            } else if item.get("name").and_then(Value::as_str).is_some() {
                // Anthropic messages-style tool definitions are flat.
                tools.push(messages_tool(item)?);
            }
        }
    }
    if let Some(items) = value.get("functions").and_then(Value::as_array) {
        for item in items {
            tools.push(openai_function_tool(item)?);
        }
    }

    Ok(tools)
}

fn openai_function_tool(value: &Value) -> AppResult<ToolDefinition> {
    Ok(ToolDefinition {
        name: required_string(value, "name")?,
        description: value
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        parameters: value
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    })
}

fn messages_tool(value: &Value) -> AppResult<ToolDefinition> {
    Ok(ToolDefinition {
        name: required_string(value, "name")?,
        description: value
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        parameters: value
            .get("input_schema")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    })
}

fn required_string(value: &Value, key: &str) -> AppResult<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| AppError::BadRequest(format!("missing string field `{key}`")))
}
