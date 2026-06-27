mod bridge;
mod config;
mod error;
mod protocol;
mod provider;
mod server;
mod stream;
mod upstream;
mod xml_protocol;

use std::process::ExitCode;

use config::AppConfig;
use tokio::signal;
use tracing::level_filters::LevelFilter;
use tracing::{info, warn};
use warp::Filter;

#[tokio::main]
async fn main() -> ExitCode {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    let config = match AppConfig::load(&config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("failed to load config `{config_path}`: {error}");
            return ExitCode::FAILURE;
        }
    };

    let log_level = parse_log_level(&config.log_level);
    tracing_subscriber::fmt().with_max_level(log_level).init();

    let addr = config.bind;
    let provider_count = config.providers.len();
    let model_count = config
        .providers
        .iter()
        .map(|p| p.models.len())
        .sum::<usize>();
    let bridge = bridge::Bridge::new(config);
    let routes = server::routes(bridge).recover(error::recover);

    info!(
        addr = %addr,
        providers = provider_count,
        models = model_count,
        "starting xml-tool-bridge"
    );
    match warp::serve(routes).try_bind_with_graceful_shutdown(addr, shutdown_signal()) {
        Ok((bound, server)) => {
            info!(addr = %bound, "listening");
            server.await;
            info!("xml-tool-bridge stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            warn!(addr = %addr, error = %error, "failed to bind listen address");
            ExitCode::FAILURE
        }
    }
}

/// Wait for SIGTERM (k8s/systemd) or SIGINT (ctrl-c) before resolving. The
/// returned future is consumed by `try_bind_with_graceful_shutdown`, which
/// then stops accepting new connections while letting in-flight requests
/// drain to completion.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = signal::ctrl_c().await {
            warn!(error = %error, "failed to install ctrl_c handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(error) => {
                warn!(error = %error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT, shutting down"),
        _ = terminate => info!("received SIGTERM, shutting down"),
    }
}

fn parse_log_level(level: &str) -> LevelFilter {
    match level.to_ascii_lowercase().as_str() {
        "off" => LevelFilter::OFF,
        "error" => LevelFilter::ERROR,
        "warn" | "warning" => LevelFilter::WARN,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => LevelFilter::INFO,
    }
}
