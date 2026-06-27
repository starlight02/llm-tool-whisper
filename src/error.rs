use std::convert::Infallible;

use serde_json::json;
use thiserror::Error;
use warp::{Rejection, Reply, http::StatusCode};

#[derive(Debug, Error)]
pub enum AppError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("upstream error: {0}")]
    Upstream(String),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Upstream(_) => StatusCode::BAD_GATEWAY,
            AppError::Http(_) => StatusCode::BAD_GATEWAY,
            AppError::Json(_) => StatusCode::BAD_REQUEST,
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;

/// Build a structured JSON error response from an `AppError`. The per-request
/// handler renders this itself (rather than using a warp rejection) so the
/// failure response can carry the same `x-request-id` header as success
/// responses.
pub fn render(error: &AppError) -> warp::reply::Response {
    let status = error.status();
    let body = warp::reply::json(&json!({
        "error": {
            "message": error.to_string(),
            "type": status.as_str(),
        }
    }));
    warp::reply::with_status(body, status).into_response()
}

/// Catch-all for routing-level rejections (404 / method mismatch / body too
/// large). Application errors are rendered inside the handler — they do not
/// reach this filter.
pub async fn recover(_rejection: Rejection) -> Result<warp::reply::Response, Infallible> {
    let body = warp::reply::json(&json!({
        "error": {
            "message": "not found",
            "type": "404",
        }
    }));
    Ok(warp::reply::with_status(body, StatusCode::NOT_FOUND).into_response())
}
