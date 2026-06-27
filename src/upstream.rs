use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::Value;
use warp::http::HeaderMap;

use crate::{
    error::{AppError, AppResult},
    protocol::{ApiProtocol, extract_stream_delta, extract_text},
};

/// Hop-by-hop headers (RFC 7230 §6.1) plus a few that reqwest must own.
const STRIPPED: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

fn is_stripped(name: &str) -> bool {
    STRIPPED.iter().any(|h| h.eq_ignore_ascii_case(name))
}

/// Build a `reqwest` request that mirrors the client request transparently.
/// Hop-by-hop and transport-owned headers are intentionally left to the HTTP
/// client because forwarding them across a proxy boundary is invalid.
pub fn forward_request(
    client: &reqwest::Client,
    url: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> reqwest::RequestBuilder {
    let mut builder = client.post(url).body(body);

    for (name, value) in headers.iter() {
        if is_stripped(name.as_str()) {
            continue;
        }
        if let Ok(value) = reqwest::header::HeaderValue::from_bytes(value.as_bytes()) {
            builder = builder.header(name.as_str(), value);
        }
    }

    builder
}

pub struct JsonTurn {
    pub status: reqwest::StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Bytes,
    pub text: Option<String>,
}

/// Fetch a single non-streaming upstream turn. Returns the raw JSON body and
/// the extracted assistant text (used for tool-call detection).
pub async fn complete_turn_json(
    client: &reqwest::Client,
    url: &str,
    headers: &HeaderMap,
    body: Bytes,
    protocol: ApiProtocol,
) -> AppResult<JsonTurn> {
    let response = forward_request(client, url, headers, body).send().await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await?;

    if !status.is_success() {
        return Ok(JsonTurn {
            status,
            headers,
            body,
            text: None,
        });
    }

    let value = serde_json::from_slice::<Value>(&body).map_err(|err| {
        AppError::Upstream(format!(
            "invalid upstream JSON body: {err}: {}",
            String::from_utf8_lossy(&body)
        ))
    })?;
    let text = extract_text(protocol, &value)?;
    Ok(JsonTurn {
        status,
        headers,
        body,
        text: Some(text),
    })
}

pub async fn drive_successful_stream<F>(
    response: reqwest::Response,
    protocol: ApiProtocol,
    mut on_delta: F,
) -> AppResult<String>
where
    F: FnMut(&str),
{
    let mut buffer = String::new();
    let mut text = String::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_string();
            buffer = buffer[index + 2..].to_string();
            if let Some(delta) = parse_sse_frame(protocol, &frame)?
                && !delta.is_empty()
            {
                text.push_str(&delta);
                on_delta(&delta);
            }
        }
    }

    if !buffer.trim().is_empty()
        && let Some(delta) = parse_sse_frame(protocol, &buffer)?
        && !delta.is_empty()
    {
        text.push_str(&delta);
        on_delta(&delta);
    }

    if text.is_empty() {
        return Err(AppError::Upstream(
            "upstream stream completed without text".to_string(),
        ));
    }
    Ok(text)
}

fn parse_sse_frame(protocol: ApiProtocol, frame: &str) -> AppResult<Option<String>> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }

    let value: Value = serde_json::from_str(&data)
        .map_err(|err| AppError::Upstream(format!("invalid upstream SSE JSON: {err}: {data}")))?;
    Ok(extract_stream_delta(protocol, &value))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitThinking {
    pub thinking: String,
    pub text: String,
}

/// Split the common leaked transcript shape used by some non-tool upstreams:
///
/// Thinking...
/// > hidden chain text
///
/// final user-visible answer
///
/// The proxy must not drop the hidden text. It is moved into the protocol's
/// reasoning/thinking surface while the final answer remains normal content.
pub(crate) fn split_leading_thinking(text: &str) -> Option<SplitThinking> {
    let rest = text
        .strip_prefix("Thinking...\r\n")
        .or_else(|| text.strip_prefix("Thinking...\n"))?;
    if !rest.starts_with('>') {
        return None;
    }

    let (thinking_raw, final_text) = split_once_blank_line(rest)?;
    let thinking = thinking_raw
        .lines()
        .map(|line| {
            line.strip_prefix('>')
                .map(|line| line.strip_prefix(' ').unwrap_or(line))
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string();
    if thinking.is_empty() {
        return None;
    }

    Some(SplitThinking {
        thinking,
        text: final_text.trim_start().to_string(),
    })
}

fn split_once_blank_line(text: &str) -> Option<(&str, &str)> {
    let lf = text.find("\n\n");
    let crlf = text.find("\r\n\r\n");
    match (lf, crlf) {
        (Some(a), Some(b)) if a < b => Some((&text[..a], &text[a + 2..])),
        (Some(_), Some(b)) => Some((&text[..b], &text[b + 4..])),
        (Some(a), None) => Some((&text[..a], &text[a + 2..])),
        (None, Some(b)) => Some((&text[..b], &text[b + 4..])),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_leaked_thinking_without_dropping_it() {
        let split = split_leading_thinking("Thinking...\n> line one\n> line two\n\nVisible answer")
            .unwrap();
        assert_eq!(split.thinking, "line one\nline two");
        assert_eq!(split.text, "Visible answer");
    }

    #[test]
    fn leaves_normal_text_alone() {
        assert!(split_leading_thinking("Thinking is useful.\n\nAnswer").is_none());
        assert!(split_leading_thinking("Plain answer").is_none());
    }
}
