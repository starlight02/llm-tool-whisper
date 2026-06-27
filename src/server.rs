use std::sync::Arc;

use bytes::Bytes;
use serde_json::json;
use tracing::Instrument;
use uuid::Uuid;
use warp::{
    Filter,
    http::{HeaderMap, HeaderValue},
    reply::Reply,
};

use crate::{bridge::Bridge, error, protocol::ApiProtocol};

pub fn routes(
    bridge: Bridge,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone + Send + Sync + 'static
{
    let body_limit = bridge.body_limit_bytes();
    let bridge = Arc::new(bridge);
    let bridge_filter = warp::any().map(move || Arc::clone(&bridge));

    let health = warp::path("health")
        .and(warp::get())
        .map(|| warp::reply::json(&json!({"ok": true})));

    let models = warp::path!("v1" / "models")
        .and(warp::get())
        .and(bridge_filter.clone())
        .map(|bridge: Arc<Bridge>| warp::reply::json(&bridge.models_response()));

    // Each chat-style endpoint forwards to the matching upstream path verbatim.
    // The path selects the protocol; the bridge forwards to {base_url}/{path}.
    let chat = warp::path!("v1" / "chat" / "completions")
        .and(warp::post())
        .and(capture(body_limit))
        .and(bridge_filter.clone())
        .and_then(|_, headers, body, bridge| handle(ApiProtocol::Chat, headers, body, bridge));

    let responses = warp::path!("v1" / "responses")
        .and(warp::post())
        .and(capture(body_limit))
        .and(bridge_filter.clone())
        .and_then(|_, headers, body, bridge| handle(ApiProtocol::Responses, headers, body, bridge));

    let messages = warp::path!("v1" / "messages")
        .and(warp::post())
        .and(capture(body_limit))
        .and(bridge_filter)
        .and_then(|_, headers, body, bridge| handle(ApiProtocol::Messages, headers, body, bridge));

    health.or(models).or(chat).or(responses).or(messages)
}

/// Capture the raw request path, headers and body bytes for transparent forwarding.
fn capture(
    body_limit: u64,
) -> impl Filter<Extract = (String, HeaderMap, Bytes), Error = warp::Rejection> + Clone {
    warp::any()
        .and(warp::path::full().map(|full: warp::path::FullPath| full.as_str().to_string()))
        .and(warp::header::headers_cloned())
        .and(warp::body::content_length_limit(body_limit))
        .and(warp::body::bytes())
}

async fn handle(
    protocol: ApiProtocol,
    headers: HeaderMap,
    body: Bytes,
    bridge: Arc<Bridge>,
) -> Result<warp::reply::Response, warp::Rejection> {
    // Accept upstream/load-balancer-supplied request IDs (`x-request-id`)
    // when they look reasonable; otherwise mint one. The same id is echoed
    // back in the response header so the client can correlate failures
    // against server logs.
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= 128 && s.chars().all(is_request_id_char))
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    let span = tracing::info_span!(
        "request",
        request_id = %request_id,
        protocol = protocol.as_path_label(),
    );
    let result = bridge
        .handle(protocol, headers, body)
        .instrument(span)
        .await;
    let mut response = match result {
        Ok(reply) => reply.into_response(),
        Err(err) => error::render(&err),
    };
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    Ok(response)
}

/// Allow the same characters Cloudflare / AWS / GCP use for request ids:
/// alphanumerics, dash, underscore. Reject everything else to keep weird or
/// malicious inputs from polluting structured logs and response headers.
fn is_request_id_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use warp::http::StatusCode;
    use warp::test::request;

    use super::*;
    use crate::config::{AppConfig, ProviderConfig};
    use crate::error;

    fn config_pointing_at(base_url: String) -> AppConfig {
        AppConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            log_level: "off".to_string(),
            body_limit_bytes: 32 * 1024 * 1024,
            upstream_connect_timeout: std::time::Duration::from_secs(5),
            upstream_json_total_timeout: std::time::Duration::from_secs(30),
            sse_keepalive_interval: std::time::Duration::from_secs(15),
            providers: vec![ProviderConfig {
                name: "mock".to_string(),
                protocol: ApiProtocol::Chat,
                base_url,
                api_key: Some("k".to_string()),
                auth_header: "Authorization".to_string(),
                auth_scheme: "Bearer".to_string(),
                headers: Default::default(),
                models: vec!["fake".to_string()],
            }],
        }
    }

    #[tokio::test]
    async fn routes_serve_health_and_models_endpoints() {
        let bridge = Bridge::new(config_pointing_at("http://127.0.0.1:1".to_string()));
        let routes = routes(bridge).recover(error::recover);

        let resp = request().method("GET").path("/health").reply(&routes).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["ok"], true);

        let resp = request()
            .method("GET")
            .path("/v1/models")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["data"][0]["id"], "mock/fake");
    }

    #[tokio::test]
    async fn routes_e2e_chat_tool_bridge_through_warp() {
        // Stand up a mock upstream, point the bridge at it, then drive the
        // bridge through the same warp routes that main.rs serves. This is
        // the closest possible test to a real client/proxy/upstream loop
        // without binding a real port.
        let upstream = warp::path!("v1" / "chat" / "completions")
            .and(warp::post())
            .map(|| {
                let content = "Working.\n<tool_call>\n  <name>echo</name>\n  <arguments><![CDATA[{\"text\":\"hi\"}]]></arguments>\n</tool_call>";
                warp::reply::json(&serde_json::json!({
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "created": 0,
                    "model": "fake",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop"
                    }]
                }))
            });
        let (addr, server) = warp::serve(upstream).bind_ephemeral(([127, 0, 0, 1], 0));
        tokio::spawn(server);

        let bridge = Bridge::new(config_pointing_at(format!("http://{addr}/v1")));
        let routes = routes(bridge).recover(error::recover);

        let body = serde_json::json!({
            "model": "mock/fake",
            "messages": [{"role": "user", "content": "echo hi"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "echo",
                    "description": "echo back",
                    "parameters": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"]
                    }
                }
            }]
        })
        .to_string();
        let resp = request()
            .method("POST")
            .path("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(body)
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let value: Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(
            value
                .pointer("/choices/0/finish_reason")
                .and_then(Value::as_str),
            Some("tool_calls")
        );
        let tc = value
            .pointer("/choices/0/message/tool_calls/0")
            .expect("tool_calls[0]");
        assert_eq!(tc["function"]["name"], "echo");
        assert_eq!(
            value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str),
            Some("Working."),
        );
        // Every response from a tool-bridge route MUST carry x-request-id so
        // operators can grep server logs by the id the client got back.
        assert!(
            resp.headers().contains_key("x-request-id"),
            "x-request-id must be echoed on the response"
        );
    }

    #[tokio::test]
    async fn routes_echo_client_supplied_request_id_when_well_formed() {
        let bridge = Bridge::new(config_pointing_at("http://127.0.0.1:1".to_string()));
        let routes = routes(bridge).recover(error::recover);
        // A reasonable-looking id from the client must be reused as-is.
        let resp = request()
            .method("POST")
            .path("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("x-request-id", "req-abc-123_XYZ")
            .body(r#"{"model":"unknown/x","messages":[]}"#)
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
            Some("req-abc-123_XYZ"),
        );
    }

    #[tokio::test]
    async fn routes_reject_garbage_client_request_id_and_mint_their_own() {
        let bridge = Bridge::new(config_pointing_at("http://127.0.0.1:1".to_string()));
        let routes = routes(bridge).recover(error::recover);
        // Newline/space/control bytes must NOT make it into a header value
        // or a tracing field; we mint a fresh id instead.
        let resp = request()
            .method("POST")
            .path("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("x-request-id", "bad id with space")
            .body(r#"{"model":"unknown/x","messages":[]}"#)
            .reply(&routes)
            .await;
        let got = resp
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .expect("x-request-id present");
        assert_ne!(got, "bad id with space");
        assert!(!got.is_empty());
        assert!(got.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
