// metrics.rs: Track and expose metric information
// Sasaki, Naoki <nsasaki@sal.co.jp> June 21, 2024
//

#[cfg(feature = "telemetry")]
use axum::{response::IntoResponse, routing::get};
#[cfg(feature = "telemetry")]
use chrono::{DateTime, Utc};
#[cfg(feature = "telemetry")]
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

#[cfg(feature = "telemetry")]
use crate::settings::Settings;

#[cfg(feature = "telemetry")]
pub async fn create_server() -> Result<
    (
        axum::serve::Serve<tokio::net::TcpListener, axum::Router, axum::Router>,
        std::net::SocketAddr,
    ),
    anyhow::Error,
> {
    let recorder_handle = setup_metrics_recorder()?;
    let app = axum::Router::new().route(
        "/metrics",
        get(move || std::future::ready(recorder_handle.render())),
    );

    let sock_addr = format!(
        "{}:{}",
        Settings::global().server.metrics_address,
        Settings::global().server.metrics_port,
    )
    .parse::<std::net::SocketAddr>()?;

    let listener = tokio::net::TcpListener::bind(sock_addr).await?;

    Ok((axum::serve(listener, app), sock_addr))
}

#[cfg(feature = "telemetry")]
fn setup_metrics_recorder() -> Result<PrometheusHandle, anyhow::Error> {
    const API_EXPONENTIAL_SECONDS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    const SESSION_EXPONENTIAL_SECONDS: &[f64] =
        &[0.1, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 600.0, 1800.0, 3600.0];

    Ok(PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("http_requests_duration_seconds".to_string()),
            API_EXPONENTIAL_SECONDS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("flight_requests_duration_seconds".to_string()),
            API_EXPONENTIAL_SECONDS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("session_context_duration_seconds".to_string()),
            SESSION_EXPONENTIAL_SECONDS,
        )?
        .install_recorder()?)
}

#[cfg(feature = "telemetry")]
pub async fn track_http(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<axum::extract::MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", status),
    ];

    metrics::counter!("http_requests_total", &labels).increment(1);
    metrics::histogram!("http_requests_duration_seconds", &labels).record(latency);

    response
}

#[cfg(all(feature = "telemetry", feature = "flight"))]
pub async fn track_flight<F, Fut, B, R>(
    method: &'static str,
    req: tonic::Request<B>,
    next: F,
) -> Result<tonic::Response<R>, tonic::Status>
where
    F: FnOnce(tonic::Request<B>) -> Fut,
    Fut: std::future::Future<Output = Result<tonic::Response<R>, tonic::Status>>,
{
    let start = std::time::Instant::now();

    let response = next(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.as_ref().map_or("error", |_| "ok");

    let labels = [
        ("method", method.to_string()),
        ("status", status.to_string()),
    ];

    metrics::counter!("flight_requests_total", &labels).increment(1);
    metrics::histogram!("flight_requests_duration_seconds", &labels).record(latency);

    response
}

#[cfg(not(feature = "telemetry"))]
#[cfg(feature = "flight")]
pub async fn track_flight<F, Fut, B, R>(
    _method: &'static str,
    req: tonic::Request<B>,
    next: F,
) -> Result<tonic::Response<R>, tonic::Status>
where
    F: FnOnce(tonic::Request<B>) -> Fut,
    Fut: std::future::Future<Output = Result<tonic::Response<R>, tonic::Status>>,
{
    next(req).await
}

#[cfg(feature = "telemetry")]
pub fn track_session_context_duration(session_start: DateTime<Utc>) {
    let now: DateTime<Utc> = Utc::now();
    let duration = now.signed_duration_since(session_start);

    #[allow(clippy::cast_precision_loss)]
    metrics::histogram!("session_context_duration_seconds")
        .record(duration.num_milliseconds() as f64 / 1000_f64);
}

#[cfg(feature = "telemetry")]
pub fn track_session_contexts_total() {
    metrics::counter!("session_contexts_total").increment(1);
}

#[cfg(feature = "telemetry")]
pub fn track_data_sources_total(scheme: &str, format: &str) {
    let labels = [
        ("scheme", scheme.to_string()),
        ("format", format.to_string()),
    ];
    metrics::counter!("data_source_registrations_total", &labels).increment(1);
}
