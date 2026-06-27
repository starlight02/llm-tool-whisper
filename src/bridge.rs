use std::{sync::Arc, time::Duration, time::Instant};

use bytes::Bytes;
use futures_channel::mpsc::{self, UnboundedSender};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use tracing::{info, warn};
use uuid::Uuid;
use warp::http::{HeaderMap, HeaderName, HeaderValue, Response as HttpResponse, StatusCode};
use warp::hyper::Body;

use crate::{
    config::{AppConfig, ProviderConfig},
    error::{AppError, AppResult},
    protocol::{ApiProtocol, ToolDefinition, collect_tools},
    provider::{models_response, resolve},
    stream::{OutputPiece, ScanEvent, Scanner, ThinkingSplitter, start_tool_events},
    upstream::{
        complete_turn_json, drive_successful_stream, forward_request, split_leading_thinking,
    },
    xml_protocol::{
        XmlToolCall, build_system_prompt, build_tool_call, build_tool_result, parse_tool_calls,
    },
};

#[derive(Clone)]
pub struct Bridge {
    config: Arc<AppConfig>,
    /// Long-lived client for streaming requests. Has no overall request
    /// timeout because a stream may legitimately last minutes.
    stream_client: reqwest::Client,
    /// Long-lived client for non-streaming requests. Bounded by a full
    /// request timeout so a hung upstream cannot pin a worker forever.
    json_client: reqwest::Client,
    created: i64,
}

impl Bridge {
    pub fn new(config: AppConfig) -> Self {
        let connect_timeout = config.upstream_connect_timeout;
        let json_timeout = config.upstream_json_total_timeout;
        let stream_client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .expect("reqwest stream client builds with valid defaults");
        let json_client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(json_timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .expect("reqwest json client builds with valid defaults");
        Self {
            config: Arc::new(config),
            stream_client,
            json_client,
            created: chrono::Utc::now().timestamp(),
        }
    }

    /// Maximum request body size accepted, in bytes. Routes use this to set
    /// warp's content-length limit.
    pub fn body_limit_bytes(&self) -> u64 {
        self.config.body_limit_bytes
    }

    pub fn models_response(&self) -> Value {
        models_response(&self.config, self.created)
    }

    pub async fn handle(
        &self,
        protocol: ApiProtocol,
        headers: HeaderMap,
        body: Bytes,
    ) -> AppResult<BridgeReply> {
        let value: Value = serde_json::from_slice(&body)
            .map_err(|err| AppError::BadRequest(format!("invalid JSON body: {err}")))?;
        let model = value
            .get("model")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::BadRequest("missing `model` field".to_string()))?;

        let route = resolve(&self.config, protocol, model)?;
        let url = format!(
            "{}/{}",
            route.provider.base_url.trim_end_matches('/'),
            protocol.upstream_path()
        );
        let headers = apply_provider_headers(headers, &route.provider);
        let mut upstream_value = value;
        upstream_value["model"] = json!(route.upstream_model);

        if !needs_tool_bridge(protocol, &upstream_value) {
            info!(
                protocol = protocol.as_path_label(),
                model = route.upstream_model,
                tool_bridge = false,
                "forwarding request"
            );
            let body = Bytes::from(serde_json::to_vec(&upstream_value)?);
            let stream = upstream_value
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            return self
                .passthrough(
                    &url,
                    &headers,
                    body,
                    protocol,
                    &route.upstream_model,
                    stream,
                )
                .await;
        }
        let tools = collect_tools(&upstream_value)?;
        let tool_names = tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        info!(
            protocol = protocol.as_path_label(),
            model = route.upstream_model,
            tool_bridge = true,
            tool_count = tools.len(),
            tool_names = %tool_names,
            "bridging tool request"
        );

        let stream = upstream_value
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let template = prepare_tool_request(protocol, upstream_value, &tools);

        if stream {
            self.bridge_stream(protocol, &url, &headers, template, tools)
                .await
        } else {
            self.bridge_json(protocol, &url, &headers, template, tools)
                .await
        }
    }

    async fn passthrough(
        &self,
        url: &str,
        headers: &HeaderMap,
        body: Bytes,
        protocol: ApiProtocol,
        model: &str,
        stream: bool,
    ) -> AppResult<BridgeReply> {
        let started = Instant::now();
        let client = if stream {
            &self.stream_client
        } else {
            &self.json_client
        };
        let response = match forward_request(client, url, headers, body).send().await {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    protocol = protocol.as_path_label(),
                    model,
                    elapsed_ms = started.elapsed().as_millis(),
                    error = %error,
                    "upstream passthrough failed"
                );
                return Err(AppError::Http(error));
            }
        };
        let status = status_from_reqwest(response.status());
        info!(
            protocol = protocol.as_path_label(),
            model,
            upstream_status = status.as_u16(),
            elapsed_ms = started.elapsed().as_millis(),
            "upstream passthrough completed"
        );
        let headers = convert_headers(response.headers());
        let body = Body::wrap_stream(
            response
                .bytes_stream()
                .map(|r| r.map_err(|e| std::io::Error::other(e.to_string()))),
        );
        Ok(BridgeReply::Raw {
            status,
            headers,
            body,
        })
    }

    async fn bridge_json(
        &self,
        protocol: ApiProtocol,
        url: &str,
        headers: &HeaderMap,
        template: Value,
        tools: Vec<ToolDefinition>,
    ) -> AppResult<BridgeReply> {
        let started = Instant::now();
        let body = Bytes::from(serde_json::to_vec(&template)?);
        let turn = complete_turn_json(&self.json_client, url, headers, body, protocol).await?;
        let status = status_from_reqwest(turn.status);
        let model = template
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default();
        info!(
            protocol = protocol.as_path_label(),
            model,
            upstream_status = status.as_u16(),
            elapsed_ms = started.elapsed().as_millis(),
            "tool bridge upstream completed"
        );
        let response_headers = convert_headers(&turn.headers);
        if !turn.status.is_success() {
            return Ok(BridgeReply::Raw {
                status,
                headers: response_headers,
                body: Body::from(turn.body),
            });
        }

        let text = turn.text.ok_or_else(|| {
            AppError::Upstream("upstream response did not contain text".to_string())
        })?;
        let calls = parse_tool_calls(&text, &tools);
        if calls.is_empty() {
            info!(
                protocol = protocol.as_path_label(),
                model,
                upstream_text = %log_snippet(&text),
                "tool bridge upstream returned no XML tool call"
            );
            let body = structure_response_body(protocol, turn.body);
            return Ok(BridgeReply::Raw {
                status,
                headers: response_headers,
                body: Body::from(body),
            });
        }

        // Strip every tool_call block; the prose that remains is the
        // assistant's user-visible commentary (which may also contain a
        // leaked "Thinking..." preamble).
        let visible = remove_tool_call_blocks(&text);
        let split = split_leading_thinking(&visible);
        let (thinking, visible_text) = match split {
            Some(s) => (Some(s.thinking), s.text),
            None => (None, visible),
        };
        let visible_text = visible_text.trim().to_string();

        info!(
            protocol = protocol.as_path_label(),
            model,
            tool_count = calls.len(),
            "synthesized native tool calls"
        );
        for call in &calls {
            info!(
                tool = call.name,
                arguments = %json_snippet(&call.arguments),
                "synthesized native tool call"
            );
        }

        Ok(BridgeReply::Json(native_tool_calls_response(
            protocol,
            model,
            &calls,
            visible_text.as_str(),
            thinking.as_deref(),
        )))
    }

    async fn bridge_stream(
        &self,
        protocol: ApiProtocol,
        url: &str,
        headers: &HeaderMap,
        template: Value,
        tools: Vec<ToolDefinition>,
    ) -> AppResult<BridgeReply> {
        let started = Instant::now();
        let model = template
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let body = Bytes::from(serde_json::to_vec(&template)?);
        let response = match forward_request(&self.stream_client, url, headers, body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    protocol = protocol.as_path_label(),
                    model,
                    elapsed_ms = started.elapsed().as_millis(),
                    error = %error,
                    "tool bridge upstream stream send failed"
                );
                return Err(AppError::Http(error));
            }
        };
        let status = response.status();
        let response_headers = convert_headers(response.headers());
        if !status.is_success() {
            let body = response.bytes().await?;
            info!(
                protocol = protocol.as_path_label(),
                model,
                upstream_status = status.as_u16(),
                elapsed_ms = started.elapsed().as_millis(),
                "tool bridge upstream stream failed"
            );
            return Ok(BridgeReply::Raw {
                status: status_from_reqwest(status),
                headers: response_headers,
                body: Body::from(body),
            });
        }

        let (tx, rx) = mpsc::unbounded::<Bytes>();
        let keepalive_tx = tx.clone();
        let keepalive_interval = self.config.sse_keepalive_interval;
        // Send an SSE comment frame on idle so nginx/cloud LBs do not drop
        // the connection during long-running upstream turns. A leading `:`
        // is a standards-compliant SSE comment and clients MUST ignore it.
        let keepalive = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(keepalive_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately; consume it so the very first
            // keepalive only goes out after the interval has actually passed.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if keepalive_tx
                    .unbounded_send(Bytes::from_static(b": keepalive\n\n"))
                    .is_err()
                {
                    return;
                }
            }
        });
        tokio::spawn(async move {
            let result = stream_once(protocol, response, model, started, tools, tx.clone()).await;
            keepalive.abort();
            if let Err(error) = result {
                let _ = send_event_now(&tx, &json!({"error": {"message": error.to_string()}}));
            }
        });

        Ok(BridgeReply::Raw {
            status: StatusCode::OK,
            headers: make_sse_headers(),
            body: Body::wrap_stream(rx.map(Ok::<_, std::io::Error>)),
        })
    }
}

async fn stream_once(
    protocol: ApiProtocol,
    response: reqwest::Response,
    model: String,
    started: Instant,
    tools: Vec<ToolDefinition>,
    tx: UnboundedSender<Bytes>,
) -> AppResult<()> {
    let mut captured_calls: Vec<XmlToolCall> = Vec::new();
    let mut visible_text = String::new();
    let mut first_send_error: Option<AppError> = None;
    let mut emitter = NativeStreamEmitter::new(protocol, model.clone(), tx.clone());
    let mut thinking = ThinkingSplitter::default();

    let mut scanner = Scanner::new(tools);
    drive_successful_stream(response, protocol, |delta| {
        for event in scanner.feed(delta) {
            match event {
                ScanEvent::Emit(piece) => {
                    visible_text.push_str(&piece);
                    for piece in thinking.feed(&piece) {
                        if let Err(error) = emitter.emit_piece(piece)
                            && first_send_error.is_none()
                        {
                            first_send_error = Some(error);
                        }
                    }
                }
                ScanEvent::ToolCall(call) => {
                    captured_calls.push(call);
                }
            }
        }
    })
    .await?;
    info!(
        protocol = protocol.as_path_label(),
        model,
        elapsed_ms = started.elapsed().as_millis(),
        "tool bridge upstream stream completed"
    );

    for event in scanner.finish() {
        match event {
            ScanEvent::Emit(piece) => {
                visible_text.push_str(&piece);
                for piece in thinking.feed(&piece) {
                    emitter.emit_piece(piece)?;
                }
            }
            ScanEvent::ToolCall(call) => {
                captured_calls.push(call);
            }
        }
    }
    for piece in thinking.finish() {
        emitter.emit_piece(piece)?;
    }

    if !captured_calls.is_empty() {
        info!(
            protocol = protocol.as_path_label(),
            model,
            tool_count = captured_calls.len(),
            "synthesized native streaming tool calls"
        );
        for call in &captured_calls {
            info!(
                tool = call.name,
                arguments = %json_snippet(&call.arguments),
                "synthesized native streaming tool call"
            );
        }
        emitter.emit_tool_calls(&captured_calls)?;
    } else {
        info!(
            protocol = protocol.as_path_label(),
            model,
            upstream_text = %log_snippet(&visible_text),
            "tool bridge upstream stream returned no XML tool call"
        );
        emitter.finish_text()?;
    }
    if let Some(error) = first_send_error {
        return Err(error);
    }
    Ok(())
}

fn needs_tool_bridge(protocol: ApiProtocol, value: &Value) -> bool {
    request_has_tools(value) || contains_tool_result(protocol, value)
}

fn request_has_tools(value: &Value) -> bool {
    value
        .get("tools")
        .and_then(Value::as_array)
        .map(|a| !a.is_empty())
        .unwrap_or(false)
        || value
            .get("functions")
            .and_then(Value::as_array)
            .map(|a| !a.is_empty())
            .unwrap_or(false)
}

fn contains_tool_result(protocol: ApiProtocol, value: &Value) -> bool {
    match protocol {
        ApiProtocol::Chat => value
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| {
                messages
                    .iter()
                    .any(|m| m.get("role").and_then(Value::as_str) == Some("tool"))
            })
            .unwrap_or(false),
        ApiProtocol::Responses => value
            .get("input")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .any(|i| i.get("type").and_then(Value::as_str) == Some("function_call_output"))
            })
            .unwrap_or(false),
        ApiProtocol::Messages => value
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| {
                messages.iter().any(|m| {
                    m.get("content")
                        .and_then(Value::as_array)
                        .map(|parts| {
                            parts.iter().any(|p| {
                                p.get("type").and_then(Value::as_str) == Some("tool_result")
                            })
                        })
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false),
    }
}

fn prepare_tool_request(
    protocol: ApiProtocol,
    mut value: Value,
    tools: &[ToolDefinition],
) -> Value {
    if let Some(obj) = value.as_object_mut() {
        obj.remove("tools");
        obj.remove("functions");
        obj.remove("tool_choice");
        obj.remove("function_call");
    }
    rewrite_tool_results(protocol, &mut value);
    if !tools.is_empty() {
        let prompt = build_system_prompt(None, tools);
        inject_system_prompt(protocol, &mut value, &prompt);
    }
    value
}

fn rewrite_tool_results(protocol: ApiProtocol, value: &mut Value) {
    match protocol {
        ApiProtocol::Chat => rewrite_chat_tool_results(value),
        ApiProtocol::Responses => rewrite_responses_tool_results(value),
        ApiProtocol::Messages => rewrite_messages_tool_results(value),
    }
}

fn rewrite_chat_tool_results(value: &mut Value) {
    let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let mut call_names = Map::new();
    for message in messages.iter() {
        if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
            for call in tool_calls {
                if let (Some(id), Some(name)) = (
                    call.get("id").and_then(Value::as_str),
                    call.pointer("/function/name").and_then(Value::as_str),
                ) {
                    call_names.insert(id.to_string(), Value::String(name.to_string()));
                }
            }
        }
    }

    for message in messages.iter_mut() {
        if message.get("role").and_then(Value::as_str) == Some("assistant") {
            if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                let xml_calls = tool_calls
                    .iter()
                    .filter_map(|call| {
                        let name = call.pointer("/function/name").and_then(Value::as_str)?;
                        let arguments = call
                            .pointer("/function/arguments")
                            .and_then(Value::as_str)
                            .and_then(|text| serde_json::from_str::<Value>(text).ok())
                            .unwrap_or_else(|| json!({}));
                        Some(build_tool_call(name, arguments))
                    })
                    .collect::<Vec<_>>();
                if !xml_calls.is_empty() {
                    let existing = message
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let merged = if existing.is_empty() {
                        xml_calls.join("\n")
                    } else {
                        format!("{}\n{}", existing, xml_calls.join("\n"))
                    };
                    message["content"] = json!(merged);
                }
                if let Some(obj) = message.as_object_mut() {
                    obj.remove("tool_calls");
                }
            }
            if let Some(function_call) = message.get("function_call").cloned() {
                if let Some(name) = function_call.get("name").and_then(Value::as_str) {
                    let arguments = function_call
                        .get("arguments")
                        .and_then(Value::as_str)
                        .and_then(|text| serde_json::from_str::<Value>(text).ok())
                        .unwrap_or_else(|| json!({}));
                    let xml = build_tool_call(name, arguments);
                    let existing = message
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let merged = if existing.is_empty() {
                        xml
                    } else {
                        format!("{existing}\n{xml}")
                    };
                    message["content"] = json!(merged);
                }
                if let Some(obj) = message.as_object_mut() {
                    obj.remove("function_call");
                }
            }
        }

        if message.get("role").and_then(Value::as_str) == Some("tool") {
            let id = message
                .get("tool_call_id")
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let name = call_names
                .get(id)
                .and_then(Value::as_str)
                .unwrap_or(id)
                .to_string();
            let content = message.get("content").cloned().unwrap_or(Value::Null);
            let payload = tool_result_payload(message, content);
            let ok = !value_indicates_error(&payload);
            *message = json!({
                "role": "user",
                "content": build_tool_result(&name, ok, payload),
            });
        }
    }
}

fn rewrite_responses_tool_results(value: &mut Value) {
    let Some(input) = value.get_mut("input").and_then(Value::as_array_mut) else {
        return;
    };
    let mut call_names = Map::new();
    for item in input.iter() {
        if item.get("type").and_then(Value::as_str) == Some("function_call")
            && let (Some(id), Some(name)) = (
                item.get("call_id").and_then(Value::as_str),
                item.get("name").and_then(Value::as_str),
            )
        {
            call_names.insert(id.to_string(), Value::String(name.to_string()));
        }
    }

    for item in input.iter_mut() {
        if item.get("type").and_then(Value::as_str) == Some("function_call") {
            let name = item.get("name").and_then(Value::as_str).unwrap_or("tool");
            let arguments = item
                .get("arguments")
                .and_then(Value::as_str)
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .or_else(|| item.get("arguments").cloned())
                .unwrap_or_else(|| json!({}));
            *item = json!({
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": build_tool_call(name, arguments)}],
            });
            continue;
        }

        if item.get("type").and_then(Value::as_str) == Some("function_call_output") {
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let name = call_names
                .get(call_id)
                .and_then(Value::as_str)
                .unwrap_or(call_id);
            let output = item.get("output").cloned().unwrap_or(Value::Null);
            let payload = tool_result_payload(item, output);
            let ok = !value_indicates_error(&payload);
            *item = json!({
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": build_tool_result(name, ok, payload)}],
            });
        }
    }
}

fn rewrite_messages_tool_results(value: &mut Value) {
    let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let mut tool_names = Map::new();
    for message in messages.iter() {
        let Some(parts) = message.get("content").and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            if part.get("type").and_then(Value::as_str) == Some("tool_use")
                && let (Some(id), Some(name)) = (
                    part.get("id").and_then(Value::as_str),
                    part.get("name").and_then(Value::as_str),
                )
            {
                tool_names.insert(id.to_string(), Value::String(name.to_string()));
            }
        }
    }

    for message in messages.iter_mut() {
        let Some(parts) = message.get_mut("content").and_then(Value::as_array_mut) else {
            continue;
        };
        for part in parts.iter_mut() {
            if part.get("type").and_then(Value::as_str) == Some("tool_use") {
                let name = part.get("name").and_then(Value::as_str).unwrap_or("tool");
                let input = part.get("input").cloned().unwrap_or_else(|| json!({}));
                *part = json!({"type": "text", "text": build_tool_call(name, input)});
                continue;
            }
            if part.get("type").and_then(Value::as_str) == Some("tool_result") {
                let id = part
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .unwrap_or("tool");
                let name = tool_names.get(id).and_then(Value::as_str).unwrap_or(id);
                let content = part.get("content").cloned().unwrap_or(Value::Null);
                let payload = tool_result_payload(part, content);
                let ok = !value_indicates_error(&payload);
                *part = json!({"type": "text", "text": build_tool_result(name, ok, payload)});
            }
        }
    }
}

/// Capture every key from the client's tool result that isn't a routing or
/// envelope field. The upstream model often needs the side-channel data
/// (stdout/stderr/exit_code/citations/usage/etc.) to keep reasoning, and a
/// fixed allow-list silently drops anything new.
fn tool_result_payload(source: &Value, content: Value) -> Value {
    const ENVELOPE_KEYS: &[&str] = &[
        "type",
        "role",
        "tool_use_id",
        "tool_call_id",
        "call_id",
        "id",
        "name",
        "content",
        "output",
    ];
    let mut payload = Map::new();
    payload.insert("content".to_string(), content);
    if let Some(obj) = source.as_object() {
        for (key, value) in obj {
            if ENVELOPE_KEYS.contains(&key.as_str()) {
                continue;
            }
            payload.insert(key.clone(), value.clone());
        }
    }
    Value::Object(payload)
}

fn value_indicates_error(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.get("is_error").and_then(Value::as_bool) == Some(true)
                || map.get("error").and_then(Value::as_bool) == Some(true)
            {
                return true;
            }
            if map
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| matches!(status, "error" | "failed" | "failure"))
            {
                return true;
            }
            if map
                .get("exit_code")
                .or_else(|| map.get("exitCode"))
                .and_then(Value::as_i64)
                .is_some_and(|code| code != 0)
            {
                return true;
            }
            map.values().any(value_indicates_error)
        }
        Value::Array(items) => items.iter().any(value_indicates_error),
        _ => false,
    }
}

fn inject_system_prompt(protocol: ApiProtocol, body: &mut Value, prompt: &str) {
    match protocol {
        ApiProtocol::Chat => {
            let message = json!({"role": "system", "content": prompt});
            if let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) {
                let insert_at = messages
                    .iter()
                    .position(|message| {
                        message.get("role").and_then(Value::as_str) != Some("system")
                    })
                    .unwrap_or(messages.len());
                messages.insert(insert_at, message);
            }
        }
        ApiProtocol::Responses => {
            let existing = body
                .get("instructions")
                .and_then(Value::as_str)
                .unwrap_or_default();
            body["instructions"] = json!(if existing.is_empty() {
                prompt.to_string()
            } else {
                format!("{existing}\n\n{prompt}")
            });
        }
        ApiProtocol::Messages => {
            let existing = body.get("system").cloned().unwrap_or(Value::Null);
            body["system"] = match existing {
                Value::String(s) if !s.is_empty() => json!(format!("{s}\n\n{prompt}")),
                Value::Array(mut arr) if !arr.is_empty() => {
                    arr.push(json!({"type": "text", "text": prompt}));
                    Value::Array(arr)
                }
                _ => json!(prompt),
            };
        }
    }
}

fn log_snippet(text: &str) -> String {
    const LIMIT: usize = 500;
    let mut out = String::new();
    for ch in text.chars().take(LIMIT) {
        if ch.is_control() && ch != '\n' && ch != '\r' && ch != '\t' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    if text.chars().count() > LIMIT {
        out.push_str("...");
    }
    out
}

fn json_snippet(value: &Value) -> String {
    serde_json::to_string(value)
        .map(|text| log_snippet(&text))
        .unwrap_or_else(|_| "<unserializable>".to_string())
}

/// Strip every `<tool_call>...</tool_call>` block from `text`, returning only
/// the visible prose. Used to recover the assistant's commentary that
/// surrounds tool calls so it can be delivered to the client alongside the
/// native tool-call response.
fn remove_tool_call_blocks(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while cursor < text.len() {
        let Some(rel_start) = text[cursor..].find("<tool_call") else {
            out.push_str(&text[cursor..]);
            break;
        };
        let start = cursor + rel_start;
        let Some(rel_end) = text[start..].find("</tool_call>") else {
            out.push_str(&text[cursor..]);
            break;
        };
        let end = start + rel_end + "</tool_call>".len();
        out.push_str(&text[cursor..start]);
        cursor = end;
    }
    out
}

fn native_tool_calls_response(
    protocol: ApiProtocol,
    model: &str,
    calls: &[XmlToolCall],
    visible_text: &str,
    thinking: Option<&str>,
) -> Value {
    match protocol {
        ApiProtocol::Chat => {
            let tool_calls: Vec<Value> = calls
                .iter()
                .map(|call| {
                    let call_id = format!("call_{}", Uuid::new_v4().simple());
                    let arguments =
                        serde_json::to_string(&call.arguments).unwrap_or_else(|_| "{}".to_string());
                    json!({
                        "id": call_id,
                        "type": "function",
                        "function": {"name": call.name, "arguments": arguments},
                    })
                })
                .collect();
            let mut message = serde_json::Map::new();
            message.insert("role".to_string(), json!("assistant"));
            message.insert(
                "content".to_string(),
                if visible_text.is_empty() {
                    Value::Null
                } else {
                    json!(visible_text)
                },
            );
            if let Some(thinking) = thinking
                && !thinking.is_empty()
            {
                message.insert("reasoning_content".to_string(), json!(thinking));
            }
            message.insert("tool_calls".to_string(), Value::Array(tool_calls));
            json!({
                "id": format!("chatcmpl-{}", Uuid::new_v4()),
                "object": "chat.completion",
                "created": chrono::Utc::now().timestamp(),
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": Value::Object(message),
                    "finish_reason": "tool_calls",
                }],
            })
        }
        ApiProtocol::Responses => {
            let mut output = Vec::new();
            if let Some(thinking) = thinking
                && !thinking.is_empty()
            {
                output.push(json!({
                    "id": format!("rs_{}", Uuid::new_v4().simple()),
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": thinking}],
                }));
            }
            if !visible_text.is_empty() {
                output.push(json!({
                    "id": format!("msg_{}", Uuid::new_v4().simple()),
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": visible_text}],
                }));
            }
            for call in calls {
                let call_id = format!("call_{}", Uuid::new_v4().simple());
                let arguments =
                    serde_json::to_string(&call.arguments).unwrap_or_else(|_| "{}".to_string());
                output.push(json!({
                    "type": "function_call",
                    "id": format!("fc_{}", Uuid::new_v4().simple()),
                    "call_id": call_id,
                    "name": call.name,
                    "arguments": arguments,
                }));
            }
            json!({
                "id": format!("resp_{}", Uuid::new_v4().simple()),
                "object": "response",
                "created_at": chrono::Utc::now().timestamp(),
                "status": "completed",
                "model": model,
                "output": output,
            })
        }
        ApiProtocol::Messages => {
            let mut content = Vec::new();
            if let Some(thinking) = thinking
                && !thinking.is_empty()
            {
                content.push(json!({
                    "type": "thinking",
                    "thinking": thinking,
                    "signature": "",
                }));
            }
            if !visible_text.is_empty() {
                content.push(json!({"type": "text", "text": visible_text}));
            }
            for call in calls {
                let call_id = format!("call_{}", Uuid::new_v4().simple());
                content.push(json!({
                    "type": "tool_use",
                    "id": call_id,
                    "name": call.name,
                    "input": call.arguments,
                }));
            }
            json!({
                "id": format!("msg_{}", Uuid::new_v4().simple()),
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": content,
                "stop_reason": "tool_use",
                "stop_sequence": Value::Null,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            })
        }
    }
}

fn structure_response_body(protocol: ApiProtocol, body: Bytes) -> Bytes {
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };
    match protocol {
        ApiProtocol::Chat => {
            if let Some(content) = value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str)
                .map(str::to_string)
                && let Some(split) = split_leading_thinking(&content)
            {
                value["choices"][0]["message"]["content"] = json!(split.text);
                value["choices"][0]["message"]["reasoning_content"] = json!(split.thinking);
            }
        }
        ApiProtocol::Responses => {
            if let Some(text) = value
                .get("output_text")
                .and_then(Value::as_str)
                .map(str::to_string)
                && let Some(split) = split_leading_thinking(&text)
            {
                value["output_text"] = json!(split.text);
                prepend_response_reasoning(&mut value, &split.thinking);
            }
            let mut leaked: Option<String> = None;
            if let Some(output) = value.get_mut("output").and_then(Value::as_array_mut) {
                'outer: for item in output.iter_mut() {
                    let Some(parts) = item.get_mut("content").and_then(Value::as_array_mut) else {
                        continue;
                    };
                    for part in parts.iter_mut() {
                        let Some(text) =
                            part.get("text").and_then(Value::as_str).map(str::to_string)
                        else {
                            continue;
                        };
                        let Some(split) = split_leading_thinking(&text) else {
                            continue;
                        };
                        part["text"] = json!(split.text);
                        leaked = Some(split.thinking);
                        break 'outer;
                    }
                }
            }
            if let Some(thinking) = leaked {
                prepend_response_reasoning(&mut value, &thinking);
            }
        }
        ApiProtocol::Messages => {
            if let Some(parts) = value.get_mut("content").and_then(Value::as_array_mut) {
                let mut index = 0;
                while index < parts.len() {
                    let Some(text) = parts[index]
                        .get("text")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                    else {
                        index += 1;
                        continue;
                    };
                    let Some(split) = split_leading_thinking(&text) else {
                        index += 1;
                        continue;
                    };
                    parts[index]["text"] = json!(split.text);
                    parts.insert(
                        index,
                        json!({
                            "type": "thinking",
                            "thinking": split.thinking,
                            "signature": "",
                        }),
                    );
                    index += 2;
                }
            }
        }
    }
    serde_json::to_vec(&value).map(Bytes::from).unwrap_or(body)
}

fn prepend_response_reasoning(value: &mut Value, thinking: &str) {
    let item = json!({
        "id": format!("rs_{}", Uuid::new_v4().simple()),
        "type": "reasoning",
        "summary": [{"type": "summary_text", "text": thinking}],
    });
    if let Some(output) = value.get_mut("output").and_then(Value::as_array_mut) {
        let already_present = output
            .iter()
            .any(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"));
        if !already_present {
            output.insert(0, item);
        }
    } else if let Some(obj) = value.as_object_mut() {
        obj.insert("output".to_string(), Value::Array(vec![item]));
    }
}

fn apply_provider_headers(mut headers: HeaderMap, provider: &ProviderConfig) -> HeaderMap {
    for (name, value) in &provider.headers {
        insert_default_header(&mut headers, name, value);
    }
    if let Some(api_key) = provider
        .api_key
        .as_ref()
        .filter(|key| !key.trim().is_empty())
    {
        remove_common_auth_headers(&mut headers);
        let value = if provider.auth_scheme.trim().is_empty() {
            api_key.to_string()
        } else {
            format!("{} {}", provider.auth_scheme.trim(), api_key)
        };
        insert_header(&mut headers, &provider.auth_header, &value);
    }
    headers
}

fn remove_common_auth_headers(headers: &mut HeaderMap) {
    for name in ["authorization", "x-api-key", "api-key"] {
        headers.remove(name);
    }
}

fn insert_default_header(headers: &mut HeaderMap, name: &str, value: &str) {
    let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
        return;
    };
    if headers.contains_key(&name) {
        return;
    }
    let Ok(value) = HeaderValue::from_str(value) else {
        return;
    };
    headers.insert(name, value);
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) {
    let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
        return;
    };
    let Ok(value) = HeaderValue::from_str(value) else {
        return;
    };
    headers.insert(name, value);
}

struct NativeStreamEmitter {
    protocol: ApiProtocol,
    model: String,
    tx: UnboundedSender<Bytes>,
    /// `true` once the per-protocol start envelope has been emitted.
    started: bool,
    /// Currently open block (Messages) / item (Responses), if any.
    open_block: Option<OpenBlock>,
    /// Index of the next content block (Messages) or output item (Responses).
    /// Unused for Chat.
    next_index: usize,
    /// A stable response id is required so every Chat chunk shares one id.
    chat_id: String,
    chat_created: i64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum OpenBlock {
    Thinking,
    Text,
}

impl NativeStreamEmitter {
    fn new(protocol: ApiProtocol, model: String, tx: UnboundedSender<Bytes>) -> Self {
        Self {
            protocol,
            model,
            tx,
            started: false,
            open_block: None,
            next_index: 0,
            chat_id: format!("chatcmpl-{}", Uuid::new_v4()),
            chat_created: chrono::Utc::now().timestamp(),
        }
    }

    fn emit_piece(&mut self, piece: OutputPiece) -> AppResult<()> {
        match piece {
            OutputPiece::Thinking(text) => self.emit_thinking(&text),
            OutputPiece::Text(text) => self.emit_text(&text),
        }
    }

    fn emit_thinking(&mut self, text: &str) -> AppResult<()> {
        if text.is_empty() {
            return Ok(());
        }
        match self.protocol {
            ApiProtocol::Chat => self.send_chat_delta(json!({"reasoning_content": text})),
            ApiProtocol::Responses => {
                self.ensure_response_started()?;
                if self.open_block != Some(OpenBlock::Thinking) {
                    self.close_open_responses_item()?;
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "response.output_item.added",
                            "output_index": self.next_index,
                            "item": {
                                "id": format!("rs_{}", Uuid::new_v4().simple()),
                                "type": "reasoning",
                                "summary": [],
                            },
                        }),
                    )?;
                    self.open_block = Some(OpenBlock::Thinking);
                }
                send_event_now(
                    &self.tx,
                    &json!({
                        "type": "response.reasoning_summary_text.delta",
                        "output_index": self.next_index,
                        "summary_index": 0,
                        "delta": text,
                    }),
                )
            }
            ApiProtocol::Messages => {
                self.ensure_message_started()?;
                if self.open_block != Some(OpenBlock::Thinking) {
                    self.close_message_block()?;
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "content_block_start",
                            "index": self.next_index,
                            "content_block": {"type": "thinking", "thinking": "", "signature": ""},
                        }),
                    )?;
                    self.open_block = Some(OpenBlock::Thinking);
                }
                send_event_now(
                    &self.tx,
                    &json!({
                        "type": "content_block_delta",
                        "index": self.next_index,
                        "delta": {"type": "thinking_delta", "thinking": text},
                    }),
                )
            }
        }
    }

    fn emit_text(&mut self, text: &str) -> AppResult<()> {
        if text.is_empty() {
            return Ok(());
        }
        match self.protocol {
            ApiProtocol::Chat => self.send_chat_delta(json!({"content": text})),
            ApiProtocol::Responses => {
                self.ensure_response_started()?;
                if self.open_block != Some(OpenBlock::Text) {
                    self.close_open_responses_item()?;
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "response.output_item.added",
                            "output_index": self.next_index,
                            "item": {
                                "id": format!("msg_{}", Uuid::new_v4().simple()),
                                "type": "message",
                                "role": "assistant",
                                "content": [],
                            },
                        }),
                    )?;
                    self.open_block = Some(OpenBlock::Text);
                }
                send_event_now(
                    &self.tx,
                    &json!({
                        "type": "response.output_text.delta",
                        "output_index": self.next_index,
                        "content_index": 0,
                        "delta": text,
                    }),
                )
            }
            ApiProtocol::Messages => {
                self.ensure_message_started()?;
                if self.open_block != Some(OpenBlock::Text) {
                    self.close_message_block()?;
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "content_block_start",
                            "index": self.next_index,
                            "content_block": {"type": "text", "text": ""},
                        }),
                    )?;
                    self.open_block = Some(OpenBlock::Text);
                }
                send_event_now(
                    &self.tx,
                    &json!({
                        "type": "content_block_delta",
                        "index": self.next_index,
                        "delta": {"type": "text_delta", "text": text},
                    }),
                )
            }
        }
    }

    fn emit_tool_calls(&mut self, calls: &[XmlToolCall]) -> AppResult<()> {
        if calls.is_empty() {
            return Ok(());
        }
        match self.protocol {
            ApiProtocol::Chat => {
                for (i, call) in calls.iter().enumerate() {
                    let call_id = format!("call_{}", Uuid::new_v4().simple());
                    let arguments =
                        serde_json::to_string(&call.arguments).unwrap_or_else(|_| "{}".to_string());
                    self.send_chat_delta(json!({
                        "role": "assistant",
                        "tool_calls": [{
                            "index": i,
                            "id": call_id,
                            "type": "function",
                            "function": {"name": call.name, "arguments": arguments},
                        }],
                    }))?;
                }
                send_event_now(
                    &self.tx,
                    &json!({
                        "id": self.chat_id,
                        "object": "chat.completion.chunk",
                        "created": self.chat_created,
                        "model": self.model,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
                    }),
                )
            }
            ApiProtocol::Responses => {
                self.ensure_response_started()?;
                self.close_open_responses_item()?;
                for call in calls {
                    let call_id = format!("call_{}", Uuid::new_v4().simple());
                    let arguments =
                        serde_json::to_string(&call.arguments).unwrap_or_else(|_| "{}".to_string());
                    let item = json!({
                        "type": "function_call",
                        "id": format!("fc_{}", Uuid::new_v4().simple()),
                        "call_id": call_id,
                        "name": call.name,
                        "arguments": arguments,
                    });
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "response.output_item.added",
                            "output_index": self.next_index,
                            "item": item,
                        }),
                    )?;
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "response.output_item.done",
                            "output_index": self.next_index,
                            "item": item,
                        }),
                    )?;
                    self.next_index += 1;
                }
                send_event_now(&self.tx, &json!({"type": "response.completed"}))
            }
            ApiProtocol::Messages => {
                self.ensure_message_started()?;
                self.close_message_block()?;
                for call in calls {
                    let call_id = format!("call_{}", Uuid::new_v4().simple());
                    let block = json!({
                        "type": "tool_use",
                        "id": call_id,
                        "name": call.name,
                        "input": call.arguments,
                    });
                    send_event_now(
                        &self.tx,
                        &json!({
                            "type": "content_block_start",
                            "index": self.next_index,
                            "content_block": block,
                        }),
                    )?;
                    send_event_now(
                        &self.tx,
                        &json!({"type": "content_block_stop", "index": self.next_index}),
                    )?;
                    self.next_index += 1;
                }
                send_event_now(
                    &self.tx,
                    &json!({
                        "type": "message_delta",
                        "delta": {"stop_reason": "tool_use", "stop_sequence": Value::Null},
                        "usage": {"output_tokens": 0},
                    }),
                )?;
                send_event_now(&self.tx, &json!({"type": "message_stop"}))
            }
        }
    }

    fn finish_text(&mut self) -> AppResult<()> {
        match self.protocol {
            ApiProtocol::Chat => send_event_now(
                &self.tx,
                &json!({
                    "id": self.chat_id,
                    "object": "chat.completion.chunk",
                    "created": self.chat_created,
                    "model": self.model,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                }),
            ),
            ApiProtocol::Responses => {
                self.ensure_response_started()?;
                self.close_open_responses_item()?;
                send_event_now(&self.tx, &json!({"type": "response.completed"}))
            }
            ApiProtocol::Messages => {
                self.ensure_message_started()?;
                self.close_message_block()?;
                send_event_now(
                    &self.tx,
                    &json!({
                        "type": "message_delta",
                        "delta": {"stop_reason": "end_turn", "stop_sequence": Value::Null},
                        "usage": {"output_tokens": 0},
                    }),
                )?;
                send_event_now(&self.tx, &json!({"type": "message_stop"}))
            }
        }
    }

    fn send_chat_delta(&self, delta: Value) -> AppResult<()> {
        send_event_now(
            &self.tx,
            &json!({
                "id": self.chat_id,
                "object": "chat.completion.chunk",
                "created": self.chat_created,
                "model": self.model,
                "choices": [{
                    "index": 0,
                    "delta": delta,
                    "finish_reason": Value::Null,
                }],
            }),
        )
    }

    fn ensure_response_started(&mut self) -> AppResult<()> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        for event in start_tool_events(ApiProtocol::Responses, &self.model) {
            send_event_now(&self.tx, &event)?;
        }
        Ok(())
    }

    fn ensure_message_started(&mut self) -> AppResult<()> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        for event in start_tool_events(ApiProtocol::Messages, &self.model) {
            send_event_now(&self.tx, &event)?;
        }
        Ok(())
    }

    fn close_message_block(&mut self) -> AppResult<()> {
        if self.open_block.take().is_some() {
            send_event_now(
                &self.tx,
                &json!({"type": "content_block_stop", "index": self.next_index}),
            )?;
            self.next_index += 1;
        }
        Ok(())
    }

    fn close_open_responses_item(&mut self) -> AppResult<()> {
        if self.open_block.take().is_some() {
            // Responses streams use output_item.done to close an open item.
            send_event_now(
                &self.tx,
                &json!({
                    "type": "response.output_item.done",
                    "output_index": self.next_index,
                }),
            )?;
            self.next_index += 1;
        }
        Ok(())
    }
}

fn send_event_now(tx: &UnboundedSender<Bytes>, value: &Value) -> AppResult<()> {
    let payload = format!("data: {}\n\n", value);
    tx.unbounded_send(Bytes::from(payload))
        .map_err(|_| AppError::Upstream("client disconnected".to_string()))
}

fn make_sse_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        warp::http::header::CONTENT_TYPE,
        "text/event-stream".parse().unwrap(),
    );
    headers.insert(
        warp::http::header::CACHE_CONTROL,
        "no-cache".parse().unwrap(),
    );
    headers
}

fn status_from_reqwest(status: reqwest::StatusCode) -> StatusCode {
    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
}

fn convert_headers(headers: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let Ok(name) = HeaderName::from_bytes(name.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) else {
            continue;
        };
        out.append(name, value);
    }
    out
}

/// Hop-by-hop headers (RFC 7230 §6.1) plus transport-owned fields that must
/// not be copied across the proxy boundary. Returns `true` if the header
/// should NOT be forwarded.
fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

pub enum BridgeReply {
    Raw {
        status: StatusCode,
        headers: HeaderMap,
        body: Body,
    },
    Json(Value),
}

impl warp::reply::Reply for BridgeReply {
    fn into_response(self) -> warp::reply::Response {
        match self {
            BridgeReply::Raw {
                status,
                headers,
                body,
            } => {
                let mut builder = HttpResponse::builder().status(status);
                for (name, value) in &headers {
                    if !is_hop_by_hop_header(name.as_str()) {
                        builder = builder.header(name.clone(), value.clone());
                    }
                }
                builder.body(body).unwrap_or_else(|_| {
                    HttpResponse::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .body(Body::from("upstream response build failed"))
                        .unwrap()
                })
            }
            BridgeReply::Json(value) => warp::reply::json(&value).into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::sync::oneshot;
    use warp::Filter;

    use super::*;
    use crate::config::ProviderConfig;

    fn test_config(base_url: String, protocol: ApiProtocol) -> AppConfig {
        AppConfig {
            bind: "127.0.0.1:8787".parse().unwrap(),
            log_level: "off".to_string(),
            body_limit_bytes: 32 * 1024 * 1024,
            upstream_connect_timeout: Duration::from_secs(5),
            upstream_json_total_timeout: Duration::from_secs(30),
            sse_keepalive_interval: Duration::from_secs(15),
            providers: vec![ProviderConfig {
                name: "mock".to_string(),
                protocol,
                base_url,
                api_key: Some("provider-token".to_string()),
                auth_header: "Authorization".to_string(),
                auth_scheme: "Bearer".to_string(),
                headers: Default::default(),
                models: vec!["gpt-test".to_string()],
            }],
        }
    }

    #[tokio::test]
    async fn passthrough_routes_with_upstream_model_and_adds_default_provider_auth() {
        let (tx, rx) = oneshot::channel::<(HeaderMap, Bytes)>();
        let tx = Arc::new(Mutex::new(Some(tx)));
        let capture = warp::path!("v1" / "chat" / "completions")
            .and(warp::post())
            .and(warp::header::headers_cloned())
            .and(warp::body::bytes())
            .map(move |headers: HeaderMap, body: Bytes| {
                if let Some(tx) = tx.lock().unwrap().take() {
                    let _ = tx.send((headers, body.clone()));
                }
                warp::reply::with_header(body.to_vec(), "x-upstream", "ok")
            });
        let (addr, server) = warp::serve(capture).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(test_config(format!("http://{addr}/v1"), ApiProtocol::Chat));
        let raw_body = Bytes::from_static(
            br#"{"model":"mock/gpt-test","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
        );
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        let reply = bridge
            .handle(ApiProtocol::Chat, headers, raw_body.clone())
            .await
            .expect("passthrough should succeed");
        match reply {
            BridgeReply::Raw { status, .. } => assert_eq!(status, StatusCode::OK),
            BridgeReply::Json(_) => panic!("passthrough should not synthesize JSON"),
        }

        let (seen_headers, seen_body) = rx.await.expect("mock upstream should receive request");
        let seen_json: Value = serde_json::from_slice(&seen_body).unwrap();
        assert_eq!(seen_json["model"], "gpt-test");
        assert_ne!(seen_body, raw_body);
        assert_eq!(
            seen_headers
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer provider-token")
        );
    }

    #[tokio::test]
    async fn provider_auth_header_wins_over_client_header() {
        let headers = {
            let mut h = HeaderMap::new();
            h.insert("authorization", HeaderValue::from_static("Bearer client"));
            h.insert("x-api-key", HeaderValue::from_static("wrong-client-key"));
            apply_provider_headers(
                h,
                &test_config("http://x".to_string(), ApiProtocol::Chat).providers[0],
            )
        };
        assert_eq!(
            headers.get("authorization").and_then(|v| v.to_str().ok()),
            Some("Bearer provider-token")
        );
        assert!(headers.get("x-api-key").is_none());
    }

    #[tokio::test]
    async fn provider_protocol_must_match_request_endpoint() {
        let bridge = Bridge::new(test_config(
            "http://127.0.0.1:9/v1".to_string(),
            ApiProtocol::Responses,
        ));
        let body = Bytes::from_static(br#"{"model":"mock/gpt-test","messages":[]}"#);
        let error = match bridge
            .handle(ApiProtocol::Chat, HeaderMap::new(), body)
            .await
        {
            Ok(_) => panic!("chat request must not route to responses provider"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("configured for `responses`"));
    }

    #[test]
    fn xml_call_becomes_native_chat_tool_call() {
        let call = XmlToolCall {
            name: "search".to_string(),
            arguments: json!({"q": "rust"}),
        };
        let value =
            native_tool_calls_response(ApiProtocol::Chat, "mock/gpt-test", &[call], "", None);
        assert_eq!(
            value
                .pointer("/choices/0/finish_reason")
                .and_then(Value::as_str),
            Some("tool_calls")
        );
        assert_eq!(
            value
                .pointer("/choices/0/message/tool_calls/0/function/name")
                .and_then(Value::as_str),
            Some("search")
        );
    }

    #[test]
    fn chat_native_response_includes_multiple_tool_calls_and_visible_text() {
        let calls = vec![
            XmlToolCall {
                name: "a".to_string(),
                arguments: json!({"x": 1}),
            },
            XmlToolCall {
                name: "b".to_string(),
                arguments: json!({"x": 2}),
            },
        ];
        let value = native_tool_calls_response(
            ApiProtocol::Chat,
            "mock/gpt-test",
            &calls,
            "I'll do both.",
            None,
        );
        assert_eq!(
            value
                .pointer("/choices/0/message/tool_calls")
                .and_then(Value::as_array)
                .map(|a| a.len()),
            Some(2)
        );
        assert_eq!(
            value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str),
            Some("I'll do both.")
        );
    }

    #[test]
    fn messages_native_response_includes_thinking_text_and_tool_uses() {
        let calls = vec![XmlToolCall {
            name: "Read".to_string(),
            arguments: json!({"path": "a"}),
        }];
        let value = native_tool_calls_response(
            ApiProtocol::Messages,
            "claude-test",
            &calls,
            "calling read.",
            Some("planning the read"),
        );
        let parts = value
            .get("content")
            .and_then(Value::as_array)
            .expect("messages.content array");
        assert_eq!(
            parts[0].get("type").and_then(Value::as_str),
            Some("thinking")
        );
        assert_eq!(parts[1].get("type").and_then(Value::as_str), Some("text"));
        assert_eq!(
            parts[2].get("type").and_then(Value::as_str),
            Some("tool_use")
        );
        assert_eq!(value["stop_reason"], "tool_use");
    }

    #[test]
    fn responses_native_response_includes_reasoning_message_and_function_calls() {
        let calls = vec![XmlToolCall {
            name: "WebSearch".to_string(),
            arguments: json!({"q": "rust"}),
        }];
        let value = native_tool_calls_response(
            ApiProtocol::Responses,
            "gpt-test",
            &calls,
            "Searching...",
            Some("planning the search"),
        );
        let output = value
            .get("output")
            .and_then(Value::as_array)
            .expect("responses.output array");
        assert_eq!(
            output[0].get("type").and_then(Value::as_str),
            Some("reasoning")
        );
        assert_eq!(
            output[1].get("type").and_then(Value::as_str),
            Some("message")
        );
        assert_eq!(
            output[2].get("type").and_then(Value::as_str),
            Some("function_call")
        );
    }

    #[test]
    fn chat_response_moves_leaked_thinking_to_reasoning_content() {
        let body = Bytes::from_static(
            br#"{"choices":[{"message":{"role":"assistant","content":"Thinking...\n> hidden\n\nvisible"}}]}"#,
        );
        let rewritten = structure_response_body(ApiProtocol::Chat, body);
        let value: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(
            value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str),
            Some("visible")
        );
        assert_eq!(
            value
                .pointer("/choices/0/message/reasoning_content")
                .and_then(Value::as_str),
            Some("hidden")
        );
    }

    #[test]
    fn responses_response_lifts_leaked_thinking_from_output_part_text() {
        let body = Bytes::from_static(
            br#"{"output":[{"content":[{"type":"output_text","text":"Thinking...\n> hidden\n\nvisible"}]}]}"#,
        );
        let rewritten = structure_response_body(ApiProtocol::Responses, body);
        let value: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(
            value.pointer("/output/0/type").and_then(Value::as_str),
            Some("reasoning")
        );
        assert_eq!(
            value
                .pointer("/output/0/summary/0/text")
                .and_then(Value::as_str),
            Some("hidden")
        );
        assert_eq!(
            value
                .pointer("/output/1/content/0/text")
                .and_then(Value::as_str),
            Some("visible")
        );
    }

    #[test]
    fn messages_response_moves_leaked_thinking_to_thinking_block() {
        let body = Bytes::from_static(
            br#"{"content":[{"type":"text","text":"Thinking...\n> hidden\n\nvisible"}]}"#,
        );
        let rewritten = structure_response_body(ApiProtocol::Messages, body);
        let value: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(
            value.pointer("/content/0/type").and_then(Value::as_str),
            Some("thinking")
        );
        assert_eq!(
            value.pointer("/content/0/thinking").and_then(Value::as_str),
            Some("hidden")
        );
        assert_eq!(
            value.pointer("/content/1/text").and_then(Value::as_str),
            Some("visible")
        );
    }

    #[test]
    fn messages_system_array_injection_uses_text_blocks() {
        let mut value = json!({
            "model": "mock/gpt-test",
            "system": [{"type": "text", "text": "existing"}],
            "messages": [{"role": "user", "content": "hi"}],
        });
        inject_system_prompt(ApiProtocol::Messages, &mut value, "bridge prompt");
        assert_eq!(
            value.pointer("/system/0/type").and_then(Value::as_str),
            Some("text")
        );
        assert_eq!(
            value.pointer("/system/0/text").and_then(Value::as_str),
            Some("existing")
        );
        assert_eq!(
            value.pointer("/system/1/type").and_then(Value::as_str),
            Some("text")
        );
        assert_eq!(
            value.pointer("/system/1/text").and_then(Value::as_str),
            Some("bridge prompt")
        );
    }

    #[test]
    fn messages_history_tool_use_is_rewritten_to_xml_before_upstream() {
        let mut value = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": "call_1",
                        "name": "Bash",
                        "input": {"cmd": "date"}
                    }]
                },
                {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "call_1",
                        "content": "Fri Jun 26"
                    }]
                }
            ]
        });
        rewrite_tool_results(ApiProtocol::Messages, &mut value);
        assert_eq!(
            value
                .pointer("/messages/0/content/0/type")
                .and_then(Value::as_str),
            Some("text")
        );
        let call_text = value
            .pointer("/messages/0/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(call_text.contains("<tool_call>"));
        assert!(call_text.contains("<name>Bash</name>"));
        assert!(call_text.contains("\"cmd\":\"date\""));

        let result_text = value
            .pointer("/messages/1/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(result_text.contains("<tool_result>"));
        assert!(result_text.contains("<name>Bash</name>"));
    }

    #[test]
    fn messages_tool_result_preserves_arbitrary_metadata() {
        let mut value = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": "call_1",
                        "name": "Search",
                        "input": {"q": "rust"}
                    }]
                },
                {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "call_1",
                        "content": {"hits": 2},
                        "citations": ["https://a", "https://b"],
                        "model_used": "search-1",
                        "usage": {"tokens": 42}
                    }]
                }
            ]
        });
        rewrite_tool_results(ApiProtocol::Messages, &mut value);
        let result_text = value
            .pointer("/messages/1/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(result_text.contains("\"citations\":["));
        assert!(result_text.contains("\"model_used\":\"search-1\""));
        assert!(result_text.contains("\"usage\":{\"tokens\":42}"));
        assert!(result_text.contains("\"hits\":2"));
    }

    #[test]
    fn messages_tool_result_preserves_error_and_stdio_fields() {
        let mut value = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": "call_1",
                        "name": "Bash",
                        "input": {"command": "bad-command"}
                    }]
                },
                {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "call_1",
                        "is_error": true,
                        "content": {
                            "stdout": "",
                            "stderr": "command not found",
                            "stdin": "bad-command",
                            "exit_code": 127
                        }
                    }]
                }
            ]
        });
        rewrite_tool_results(ApiProtocol::Messages, &mut value);
        let result_text = value
            .pointer("/messages/1/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(result_text.contains("\"ok\":false"));
        assert!(result_text.contains("\"is_error\":true"));
        assert!(result_text.contains("\"stderr\":\"command not found\""));
        assert!(result_text.contains("\"stdin\":\"bad-command\""));
        assert!(result_text.contains("\"exit_code\":127"));
    }

    #[test]
    fn responses_tool_result_preserves_failed_status() {
        let mut value = json!({
            "input": [
                {"type": "function_call", "call_id": "call_1", "name": "Bash", "arguments": "{\"command\":\"bad-command\"}"},
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "status": "failed",
                    "output": {"stdout": "", "stderr": "command not found", "exit_code": 127}
                }
            ]
        });
        rewrite_tool_results(ApiProtocol::Responses, &mut value);
        let result_text = value
            .pointer("/input/1/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(result_text.contains("\"ok\":false"));
        assert!(result_text.contains("\"status\":\"failed\""));
        assert!(result_text.contains("\"stderr\":\"command not found\""));
        assert!(result_text.contains("\"exit_code\":127"));
    }

    #[test]
    fn chat_history_tool_calls_are_rewritten_to_xml_before_upstream() {
        let mut value = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "Bash", "arguments": "{\"cmd\":\"date\"}"}
                    }]
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "Fri Jun 26"}
            ]
        });
        rewrite_tool_results(ApiProtocol::Chat, &mut value);
        assert!(value.pointer("/messages/0/tool_calls").is_none());
        let call_text = value
            .pointer("/messages/0/content")
            .and_then(Value::as_str)
            .unwrap();
        assert!(call_text.contains("<tool_call>"));
        assert!(call_text.contains("<name>Bash</name>"));
        assert_eq!(
            value.pointer("/messages/1/role").and_then(Value::as_str),
            Some("user")
        );
        assert!(
            value
                .pointer("/messages/1/content")
                .and_then(Value::as_str)
                .unwrap()
                .contains("<tool_result>")
        );
    }

    #[test]
    fn chat_history_preserves_assistant_text_alongside_tool_calls() {
        let mut value = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": "I'll run a command first.",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "Bash", "arguments": "{\"command\":\"date\"}"}
                    }]
                }
            ]
        });
        rewrite_tool_results(ApiProtocol::Chat, &mut value);
        let content = value
            .pointer("/messages/0/content")
            .and_then(Value::as_str)
            .unwrap();
        assert!(content.starts_with("I'll run a command first."));
        assert!(content.contains("<tool_call>"));
        assert!(content.contains("<name>Bash</name>"));
    }

    #[test]
    fn remove_tool_call_blocks_keeps_surrounding_prose() {
        let combined = "intro <tool_call><name>A</name></tool_call> middle <tool_call><name>B</name></tool_call> end";
        let visible = remove_tool_call_blocks(combined);
        assert_eq!(visible, "intro  middle  end");
    }

    // ------------------------------------------------------------------
    // End-to-end: a mock upstream serves XML tool_call responses, the
    // bridge converts them into native protocol responses, and we assert
    // the client-visible payload is the right native shape.
    // ------------------------------------------------------------------

    fn echo_tool_in_request_body(body: &Bytes) {
        let req: Value = serde_json::from_slice(body).expect("upstream got valid json");
        // The bridge MUST strip native tool fields before forwarding.
        assert!(req.get("tools").is_none(), "tools must be stripped: {req}");
        assert!(
            req.get("tool_choice").is_none(),
            "tool_choice must be stripped: {req}"
        );
    }

    fn xml_tool_call_block(name: &str, args_json: &str) -> String {
        format!(
            "<tool_call>\n  <name>{name}</name>\n  <arguments><![CDATA[{args_json}]]></arguments>\n</tool_call>"
        )
    }

    #[tokio::test]
    async fn e2e_chat_single_tool_call_with_visible_text() {
        // Upstream returns prose + ONE XML tool_call as raw assistant content.
        let captured = Arc::new(Mutex::new(None::<Bytes>));
        let captured_filter = captured.clone();
        let route = warp::path!("v1" / "chat" / "completions")
            .and(warp::post())
            .and(warp::body::bytes())
            .map(move |body: Bytes| {
                echo_tool_in_request_body(&body);
                // System prompt injected — confirm bridge wrote the marker.
                let req: Value = serde_json::from_slice(&body).unwrap();
                let sys = req["messages"][0]["content"].as_str().unwrap();
                assert!(sys.contains("TOOL BRIDGE INSTRUCTION"));
                *captured_filter.lock().unwrap() = Some(body.clone());
                let content = format!(
                    "I'll check.\n{}",
                    xml_tool_call_block("echo", r#"{"text":"hello"}"#)
                );
                warp::reply::json(&json!({
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "created": 0,
                    "model": "gpt-test",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop"
                    }]
                }))
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(test_config(format!("http://{addr}/v1"), ApiProtocol::Chat));
        let body = Bytes::from(
            json!({
                "model": "mock/gpt-test",
                "messages": [{"role": "user", "content": "echo hello"}],
                "tools": [{
                    "type": "function",
                    "function": {
                        "name": "echo",
                        "description": "echo back the text",
                        "parameters": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"]
                        }
                    }
                }]
            })
            .to_string(),
        );

        let reply = bridge
            .handle(ApiProtocol::Chat, HeaderMap::new(), body)
            .await
            .expect("bridge handle should succeed");
        let value = match reply {
            BridgeReply::Json(value) => value,
            BridgeReply::Raw { .. } => panic!("expected JSON reply with tool calls"),
        };

        assert_eq!(
            value
                .pointer("/choices/0/finish_reason")
                .and_then(Value::as_str),
            Some("tool_calls")
        );
        assert_eq!(
            value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str),
            Some("I'll check."),
        );
        let tool_calls = value
            .pointer("/choices/0/message/tool_calls")
            .and_then(Value::as_array)
            .expect("tool_calls array");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0]["function"]["name"], "echo");
        let args: Value =
            serde_json::from_str(tool_calls[0]["function"]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(args, json!({"text": "hello"}));
        assert!(captured.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn e2e_chat_multiple_parallel_tool_calls() {
        // Upstream emits two tool_call blocks back-to-back. Both must reach
        // the client as parallel native tool_calls.
        let route = warp::path!("v1" / "chat" / "completions")
            .and(warp::post())
            .and(warp::body::bytes())
            .map(move |body: Bytes| {
                echo_tool_in_request_body(&body);
                let content = format!(
                    "Doing two reads.\n{}\n{}",
                    xml_tool_call_block("Read", r#"{"path":"a"}"#),
                    xml_tool_call_block("Read", r#"{"path":"b"}"#),
                );
                warp::reply::json(&json!({
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "created": 0,
                    "model": "gpt-test",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop"
                    }]
                }))
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(test_config(format!("http://{addr}/v1"), ApiProtocol::Chat));
        let body = Bytes::from(
            json!({
                "model": "mock/gpt-test",
                "messages": [{"role": "user", "content": "read a and b"}],
                "tools": [{
                    "type": "function",
                    "function": {
                        "name": "Read",
                        "description": "read a file",
                        "parameters": {
                            "type": "object",
                            "properties": {"path": {"type": "string"}},
                            "required": ["path"]
                        }
                    }
                }]
            })
            .to_string(),
        );
        let reply = bridge
            .handle(ApiProtocol::Chat, HeaderMap::new(), body)
            .await
            .expect("bridge handle should succeed");
        let value = match reply {
            BridgeReply::Json(value) => value,
            BridgeReply::Raw { .. } => panic!("expected JSON reply"),
        };
        let tool_calls = value
            .pointer("/choices/0/message/tool_calls")
            .and_then(Value::as_array)
            .expect("tool_calls array");
        assert_eq!(tool_calls.len(), 2, "must keep BOTH parallel tool calls");
        assert_eq!(tool_calls[0]["function"]["name"], "Read");
        assert_eq!(tool_calls[1]["function"]["name"], "Read");
        let a: Value =
            serde_json::from_str(tool_calls[0]["function"]["arguments"].as_str().unwrap()).unwrap();
        let b: Value =
            serde_json::from_str(tool_calls[1]["function"]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(a["path"], "a");
        assert_eq!(b["path"], "b");
    }

    #[tokio::test]
    async fn e2e_chat_tool_result_is_rewritten_to_xml_before_upstream() {
        // The client posts a tool_result follow-up. The bridge must rewrite
        // both the assistant tool_calls AND the tool role message into XML
        // before forwarding to upstream.
        let captured = Arc::new(Mutex::new(None::<Bytes>));
        let captured_filter = captured.clone();
        let route = warp::path!("v1" / "chat" / "completions")
            .and(warp::post())
            .and(warp::body::bytes())
            .map(move |body: Bytes| {
                *captured_filter.lock().unwrap() = Some(body.clone());
                warp::reply::json(&json!({
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "created": 0,
                    "model": "gpt-test",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": "Got it: the date was Fri."},
                        "finish_reason": "stop"
                    }]
                }))
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(test_config(format!("http://{addr}/v1"), ApiProtocol::Chat));
        let body = Bytes::from(
            json!({
                "model": "mock/gpt-test",
                "messages": [
                    {"role": "user", "content": "what's the date"},
                    {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": "call_x",
                            "type": "function",
                            "function": {"name": "Bash", "arguments": "{\"command\":\"date\"}"}
                        }]
                    },
                    {"role": "tool", "tool_call_id": "call_x", "content": "Fri Jun 27 00:00:00 PDT 2026"}
                ],
                "tools": [{
                    "type": "function",
                    "function": {
                        "name": "Bash",
                        "description": "run shell",
                        "parameters": {
                            "type": "object",
                            "properties": {"command": {"type": "string"}},
                            "required": ["command"]
                        }
                    }
                }]
            })
            .to_string(),
        );

        let reply = bridge
            .handle(ApiProtocol::Chat, HeaderMap::new(), body)
            .await
            .expect("bridge handle should succeed");
        // Upstream returned no tool_call, so the bridge passes the upstream
        // body through (re-wrapped in raw form) for the client to consume.
        let body = match reply {
            BridgeReply::Raw { body, .. } => body,
            BridgeReply::Json(_) => panic!("no tool_call in reply means raw passthrough"),
        };
        use futures_util::TryStreamExt;
        let bytes = body
            .map_ok(|b| b.to_vec())
            .try_concat()
            .await
            .expect("collect upstream body");
        let value: Value = serde_json::from_slice(&bytes).expect("upstream returned json");
        assert!(
            value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("Got it"),
            "client should see the assistant text reply: {value}"
        );

        // Inspect what reached upstream: tool_calls must be gone, every
        // history message must use the XML protocol.
        let sent = captured
            .lock()
            .unwrap()
            .clone()
            .expect("upstream got request");
        let sent: Value = serde_json::from_slice(&sent).unwrap();
        let messages = sent["messages"].as_array().unwrap();
        assert_eq!(messages[0]["role"], "system");
        assert!(
            messages[0]["content"]
                .as_str()
                .unwrap()
                .contains("TOOL BRIDGE INSTRUCTION")
        );
        let assistant = messages
            .iter()
            .find(|m| m["role"] == "assistant")
            .expect("assistant turn");
        assert!(
            assistant.get("tool_calls").is_none(),
            "tool_calls must be flattened to content XML"
        );
        let assistant_content = assistant["content"].as_str().unwrap();
        assert!(assistant_content.contains("<tool_call>"));
        assert!(assistant_content.contains("<name>Bash</name>"));
        let tool_turn = messages
            .iter()
            .find(|m| {
                m["role"] == "user"
                    && m["content"]
                        .as_str()
                        .unwrap_or("")
                        .contains("<tool_result>")
            })
            .expect("tool result rewritten to user/xml");
        let tool_content = tool_turn["content"].as_str().unwrap();
        assert!(tool_content.contains("<tool_result>"));
        assert!(tool_content.contains("<name>Bash</name>"));
        assert!(tool_content.contains("Fri Jun 27"));
        assert!(tool_content.contains("\"ok\":true"));
    }

    #[tokio::test]
    async fn e2e_messages_single_tool_call_synthesizes_anthropic_tool_use() {
        let route = warp::path!("v1" / "messages")
            .and(warp::post())
            .and(warp::body::bytes())
            .map(move |body: Bytes| {
                echo_tool_in_request_body(&body);
                let req: Value = serde_json::from_slice(&body).unwrap();
                // The bridge injects its prompt into Anthropic's `system` field.
                let sys = req["system"].clone();
                let sys_text = match sys {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                assert!(sys_text.contains("TOOL BRIDGE INSTRUCTION"));
                let content_text = format!(
                    "I'll search.\n{}",
                    xml_tool_call_block("Search", r#"{"q":"rust"}"#)
                );
                warp::reply::json(&json!({
                    "id": "msg_fake",
                    "type": "message",
                    "role": "assistant",
                    "model": "gpt-test",
                    "content": [{"type": "text", "text": content_text}],
                    "stop_reason": "end_turn",
                    "stop_sequence": Value::Null,
                    "usage": {"input_tokens": 1, "output_tokens": 1}
                }))
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(test_config(
            format!("http://{addr}/v1"),
            ApiProtocol::Messages,
        ));
        let body = Bytes::from(
            json!({
                "model": "mock/gpt-test",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "search rust"}],
                "tools": [{
                    "name": "Search",
                    "description": "search the web",
                    "input_schema": {
                        "type": "object",
                        "properties": {"q": {"type": "string"}},
                        "required": ["q"]
                    }
                }]
            })
            .to_string(),
        );
        let reply = bridge
            .handle(ApiProtocol::Messages, HeaderMap::new(), body)
            .await
            .expect("bridge handle should succeed");
        let value = match reply {
            BridgeReply::Json(value) => value,
            BridgeReply::Raw { .. } => panic!("expected JSON reply"),
        };
        assert_eq!(value["stop_reason"], "tool_use");
        let content = value["content"].as_array().expect("content array");
        // Visible text + tool_use, no tool_call XML leaked to client.
        let tool_use = content
            .iter()
            .find(|p| p["type"] == "tool_use")
            .expect("tool_use part");
        assert_eq!(tool_use["name"], "Search");
        assert_eq!(tool_use["input"]["q"], "rust");
        let text_part = content
            .iter()
            .find(|p| p["type"] == "text")
            .expect("text part");
        assert_eq!(text_part["text"], "I'll search.");
    }

    #[tokio::test]
    async fn e2e_chat_streaming_synthesizes_native_tool_call_chunks() {
        // Upstream is a streaming SSE source. The bridge must intercept the
        // tool_call XML and emit a synthesized tool_calls chunk + the
        // finish_reason chunk on the client SSE.
        let route = warp::path!("v1" / "chat" / "completions")
            .and(warp::post())
            .and(warp::body::bytes())
            .map(move |_body: Bytes| {
                let id = "chatcmpl-fake";
                let make_chunk = |delta: Value| {
                    let frame = json!({
                        "id": id,
                        "object": "chat.completion.chunk",
                        "created": 0,
                        "model": "gpt-test",
                        "choices": [{"index": 0, "delta": delta, "finish_reason": Value::Null}]
                    });
                    format!("data: {}\n\n", frame)
                };
                let chunks = [
                    make_chunk(json!({"role": "assistant", "content": ""})),
                    make_chunk(json!({"content": "Looking up.\n"})),
                    make_chunk(json!({"content": "<tool_call>\n  <name>echo</name>\n  <arguments><![CDATA["})),
                    make_chunk(json!({"content": "{\"text\":\"hi\"}]]></arguments>\n</tool_call>"})),
                    "data: [DONE]\n\n".to_string(),
                ];
                let body_text = chunks.concat();
                warp::http::Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(body_text)
                    .unwrap()
            });
        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(test_config(format!("http://{addr}/v1"), ApiProtocol::Chat));
        let body = Bytes::from(
            json!({
                "model": "mock/gpt-test",
                "stream": true,
                "messages": [{"role": "user", "content": "echo hi"}],
                "tools": [{
                    "type": "function",
                    "function": {
                        "name": "echo",
                        "description": "echo",
                        "parameters": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"]
                        }
                    }
                }]
            })
            .to_string(),
        );
        let reply = bridge
            .handle(ApiProtocol::Chat, HeaderMap::new(), body)
            .await
            .expect("bridge handle should succeed");
        let body = match reply {
            BridgeReply::Raw { body, .. } => body,
            BridgeReply::Json(_) => panic!("streaming reply must be raw SSE"),
        };
        // Drain the SSE body.
        use futures_util::TryStreamExt;
        let bytes = body
            .map_ok(|b| b.to_vec())
            .try_concat()
            .await
            .expect("collect SSE body");
        let text = String::from_utf8(bytes).expect("utf-8 SSE body");

        // Parse SSE frames into JSON values for inspection.
        let mut frames: Vec<Value> = Vec::new();
        for frame in text.split("\n\n") {
            for line in frame.lines() {
                if let Some(payload) = line.strip_prefix("data: ") {
                    if payload.trim() == "[DONE]" {
                        continue;
                    }
                    if let Ok(value) = serde_json::from_str::<Value>(payload) {
                        frames.push(value);
                    }
                }
            }
        }
        let text_delta: String = frames
            .iter()
            .filter_map(|f| {
                f.pointer("/choices/0/delta/content")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        assert!(
            text_delta.contains("Looking up."),
            "visible content must be forwarded: got {text_delta:?} from {frames:?}",
        );
        assert!(
            !text_delta.contains("<tool_call>"),
            "XML must NOT leak to client: got {text_delta:?}",
        );
        let tool_call_chunk = frames
            .iter()
            .find(|f| f.pointer("/choices/0/delta/tool_calls").is_some())
            .expect("at least one tool_calls delta chunk");
        let tc = tool_call_chunk
            .pointer("/choices/0/delta/tool_calls/0")
            .unwrap();
        assert_eq!(tc["function"]["name"], "echo");
        let finish = frames
            .iter()
            .find_map(|f| {
                f.pointer("/choices/0/finish_reason")
                    .and_then(Value::as_str)
            })
            .expect("final finish_reason");
        assert_eq!(finish, "tool_calls");
    }
}
