use std::{collections::HashMap, net::SocketAddr, time::Duration};

use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;

use crate::{
    error::{AppError, AppResult},
    protocol::ApiProtocol,
};

#[derive(Clone, Debug, Deserialize)]
pub struct RawConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub upstream: UpstreamConfig,
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    /// Maximum request body size accepted by the bridge, in megabytes.
    /// A high default keeps long Anthropic tool histories from being
    /// rejected; lower it in fronted deployments if you want to push back
    /// on abusive payloads.
    #[serde(default = "default_body_limit_mb")]
    pub body_limit_mb: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            body_limit_mb: default_body_limit_mb(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpstreamConfig {
    /// Cap on how long establishing a TCP/TLS connection to the upstream
    /// may take. Short-circuits hung DNS / firewall drops without forcing
    /// the operator to wait the full request timeout.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Cap on a non-streaming upstream turn end-to-end. Streaming turns
    /// are NOT bounded by this — they live as long as the client SSE.
    #[serde(default = "default_json_total_timeout_secs")]
    pub json_total_timeout_secs: u64,
    /// Idle interval for SSE keepalive comment frames. Set below the
    /// idle timeout of any LB / nginx in front of this proxy.
    #[serde(default = "default_sse_keepalive_secs")]
    pub sse_keepalive_secs: u64,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: default_connect_timeout_secs(),
            json_total_timeout_secs: default_json_total_timeout_secs(),
            sse_keepalive_secs: default_sse_keepalive_secs(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    /// Client-facing protocol this provider is exposed under.
    pub protocol: ApiProtocol,
    /// Upstream protocol/path used when the provider exposes a different API
    /// shape than the client-facing endpoint. Defaults to `protocol`.
    #[serde(default)]
    pub upstream_protocol: Option<ApiProtocol>,
    pub base_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_auth_header")]
    pub auth_header: String,
    #[serde(default = "default_auth_scheme")]
    pub auth_scheme: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub models: Vec<String>,
}

/// Custom Debug that NEVER renders the API key or upstream header values.
/// Without this, any future `tracing::debug!(?config)` or panic backtrace
/// could leak provider credentials into logs and crash dumps.
impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("name", &self.name)
            .field("protocol", &self.protocol)
            .field("upstream_protocol", &self.upstream_protocol)
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("auth_header", &self.auth_header)
            .field("auth_scheme", &self.auth_scheme)
            .field("header_keys", &self.headers.keys().collect::<Vec<_>>())
            .field("models", &self.models)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub bind: SocketAddr,
    pub log_level: String,
    pub body_limit_bytes: u64,
    pub upstream_connect_timeout: Duration,
    pub upstream_json_total_timeout: Duration,
    pub sse_keepalive_interval: Duration,
    pub providers: Vec<ProviderConfig>,
}

fn default_bind() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8787))
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_auth_header() -> String {
    "Authorization".to_string()
}

fn default_auth_scheme() -> String {
    "Bearer".to_string()
}

fn default_body_limit_mb() -> u64 {
    32
}

fn default_connect_timeout_secs() -> u64 {
    20
}

fn default_json_total_timeout_secs() -> u64 {
    300
}

fn default_sse_keepalive_secs() -> u64 {
    15
}

impl AppConfig {
    pub fn load(path: &str) -> AppResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|err| {
            AppError::Config(format!("failed to read config file `{path}`: {err}"))
        })?;
        let raw: RawConfig = toml::from_str(&content).map_err(|err| {
            AppError::Config(format!("failed to parse config file `{path}`: {err}"))
        })?;

        Self::validate(&raw)?;
        Ok(Self {
            bind: raw.server.bind,
            log_level: raw.log.level,
            body_limit_bytes: raw.server.body_limit_mb.saturating_mul(1024 * 1024),
            upstream_connect_timeout: Duration::from_secs(raw.upstream.connect_timeout_secs),
            upstream_json_total_timeout: Duration::from_secs(raw.upstream.json_total_timeout_secs),
            sse_keepalive_interval: Duration::from_secs(raw.upstream.sse_keepalive_secs),
            providers: raw.providers,
        })
    }

    fn validate(raw: &RawConfig) -> AppResult<()> {
        if raw.providers.is_empty() {
            return Err(AppError::Config(
                "config must define at least one [[providers]] entry".to_string(),
            ));
        }
        if raw.server.body_limit_mb == 0 {
            return Err(AppError::Config(
                "[server].body_limit_mb must be greater than zero".to_string(),
            ));
        }
        if raw.upstream.connect_timeout_secs == 0 {
            return Err(AppError::Config(
                "[upstream].connect_timeout_secs must be greater than zero".to_string(),
            ));
        }
        if raw.upstream.json_total_timeout_secs == 0 {
            return Err(AppError::Config(
                "[upstream].json_total_timeout_secs must be greater than zero".to_string(),
            ));
        }
        if raw.upstream.sse_keepalive_secs == 0 {
            return Err(AppError::Config(
                "[upstream].sse_keepalive_secs must be greater than zero".to_string(),
            ));
        }

        let mut seen_providers = std::collections::HashSet::new();
        for provider in &raw.providers {
            if provider.name.trim().is_empty() {
                return Err(AppError::Config(
                    "provider `name` must not be empty".to_string(),
                ));
            }
            if provider.base_url.trim().is_empty() {
                return Err(AppError::Config(format!(
                    "provider `{}` must define `base_url`",
                    provider.name
                )));
            }
            if provider.auth_header.trim().is_empty() {
                return Err(AppError::Config(format!(
                    "provider `{}` has an empty `auth_header`",
                    provider.name
                )));
            }
            if HeaderName::from_bytes(provider.auth_header.as_bytes()).is_err() {
                return Err(AppError::Config(format!(
                    "provider `{}` has invalid `auth_header` `{}`",
                    provider.name, provider.auth_header
                )));
            }
            if let Some(api_key) = provider
                .api_key
                .as_ref()
                .filter(|key| !key.trim().is_empty())
            {
                let header_value = if provider.auth_scheme.trim().is_empty() {
                    api_key.to_string()
                } else {
                    format!("{} {}", provider.auth_scheme.trim(), api_key)
                };
                if HeaderValue::from_str(&header_value).is_err() {
                    return Err(AppError::Config(format!(
                        "provider `{}` builds an invalid `{}` header value",
                        provider.name, provider.auth_header
                    )));
                }
            }
            for (name, value) in &provider.headers {
                if HeaderName::from_bytes(name.as_bytes()).is_err() {
                    return Err(AppError::Config(format!(
                        "provider `{}` has invalid header name `{}`",
                        provider.name, name
                    )));
                }
                if HeaderValue::from_str(value).is_err() {
                    return Err(AppError::Config(format!(
                        "provider `{}` has invalid value for header `{}`",
                        provider.name, name
                    )));
                }
            }
            if provider.models.is_empty() {
                return Err(AppError::Config(format!(
                    "provider `{}` must define at least one model",
                    provider.name
                )));
            }
            for model in &provider.models {
                if model.trim().is_empty() {
                    return Err(AppError::Config(format!(
                        "provider `{}` has an empty model id",
                        provider.name
                    )));
                }
            }
            if !seen_providers.insert(provider.name.clone()) {
                return Err(AppError::Config(format!(
                    "duplicate provider name `{}`",
                    provider.name
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_with_provider(provider: ProviderConfig) -> RawConfig {
        RawConfig {
            server: ServerConfig::default(),
            log: LogConfig::default(),
            upstream: UpstreamConfig::default(),
            providers: vec![provider],
        }
    }

    fn provider() -> ProviderConfig {
        ProviderConfig {
            name: "mock".to_string(),
            protocol: ApiProtocol::Chat,
            upstream_protocol: None,
            base_url: "https://example.com/v1".to_string(),
            api_key: Some("token".to_string()),
            auth_header: "Authorization".to_string(),
            auth_scheme: "Bearer".to_string(),
            headers: HashMap::new(),
            models: vec!["model".to_string()],
        }
    }

    #[test]
    fn rejects_invalid_provider_auth_header_name() {
        let mut provider = provider();
        provider.auth_header = "bad header".to_string();

        let error = AppConfig::validate(&raw_with_provider(provider)).unwrap_err();

        assert!(error.to_string().contains("invalid `auth_header`"));
    }

    #[test]
    fn rejects_invalid_provider_auth_header_value() {
        let mut provider = provider();
        provider.api_key = Some("bad\nkey".to_string());

        let error = AppConfig::validate(&raw_with_provider(provider)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("invalid `Authorization` header value")
        );
    }

    #[test]
    fn rejects_invalid_static_provider_header() {
        let mut provider = provider();
        provider
            .headers
            .insert("x-good".to_string(), "bad\nvalue".to_string());

        let error = AppConfig::validate(&raw_with_provider(provider)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("invalid value for header `x-good`")
        );
    }
}
