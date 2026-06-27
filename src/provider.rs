use std::sync::Arc;

use serde_json::{Value, json};

use crate::{
    config::{AppConfig, ProviderConfig},
    error::{AppError, AppResult},
    protocol::ApiProtocol,
};

/// A resolved routing decision: which upstream provider serves a given client model id.
#[derive(Clone, Debug)]
pub struct Route {
    pub provider: Arc<ProviderConfig>,
    pub upstream_model: String,
    pub upstream_protocol: ApiProtocol,
}

/// Split a client model id of the form `provider/model` into its parts.
/// A provider name never contains `/`; everything after the first `/` is the
/// upstream model id (so upstream model ids may themselves contain `/`).
pub fn split_model(model: &str) -> Option<(&str, &str)> {
    let idx = model.find('/')?;
    let provider = &model[..idx];
    let upstream = &model[idx + 1..];
    if provider.is_empty() || upstream.is_empty() {
        return None;
    }
    Some((provider, upstream))
}

pub fn resolve(config: &AppConfig, protocol: ApiProtocol, model: &str) -> AppResult<Route> {
    let (provider_name, upstream_model) = split_model(model).ok_or_else(|| {
        AppError::BadRequest(format!("model `{model}` must be in `provider/model` form"))
    })?;

    let provider = config
        .providers
        .iter()
        .find(|p| p.name == provider_name)
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "unknown provider `{provider_name}` for model `{model}`"
            ))
        })?;

    let upstream_model = upstream_model.to_string();
    if !provider.models.iter().any(|m| m == &upstream_model) {
        return Err(AppError::BadRequest(format!(
            "model `{upstream_model}` is not configured for provider `{provider_name}`"
        )));
    }
    if provider.protocol != protocol {
        return Err(AppError::BadRequest(format!(
            "provider `{provider_name}` is configured for `{}` client requests, not `{}`; call `/v1/{}` for this provider or configure a separate provider with protocol `{}`",
            provider.protocol.as_path_label(),
            protocol.as_path_label(),
            provider.protocol.upstream_path(),
            protocol.as_path_label()
        )));
    }

    let upstream_protocol = provider.upstream_protocol.unwrap_or(provider.protocol);

    Ok(Route {
        provider: Arc::new(provider.clone()),
        upstream_model,
        upstream_protocol,
    })
}

/// Build the OpenAI-compatible `/v1/models` response from configuration.
pub fn models_response(config: &AppConfig, created: i64) -> Value {
    let data: Vec<Value> = config
        .providers
        .iter()
        .flat_map(|provider| {
            provider.models.iter().map(move |model| {
                json!({
                    "id": format!("{}/{}", provider.name, model),
                    "object": "model",
                    "created": created,
                    "owned_by": provider.name,
                })
            })
        })
        .collect();

    json!({
        "object": "list",
        "data": data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_provider_and_model() {
        assert_eq!(split_model("openai/gpt-4o"), Some(("openai", "gpt-4o")));
        // upstream model ids may contain slashes
        assert_eq!(
            split_model("openai/path/to/model"),
            Some(("openai", "path/to/model"))
        );
        assert_eq!(split_model("noprovider"), None);
        assert_eq!(split_model("/missing"), None);
        assert_eq!(split_model("missing/"), None);
    }
}
