// routes.rs - Register routes
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use crate::context::session_manager::SessionManager;
use crate::response::handler::{
    arrow_csv, arrow_parquet, data_source, dataframe, json_csv, processor, session, sys_info,
};
use crate::settings::Settings;
use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;

pub fn register<S: SessionManager>(session_mgr: Arc<tokio::sync::Mutex<S>>) -> Router {
    let arrow_route = Router::new()
        .route("/csv/:file", get(arrow_csv::csv_responder))
        .route("/parquet/:file", get(arrow_parquet::parquet_responder));

    let json_route = Router::new().route("/csv/:file", get(json_csv::csv_responder));
    let df_route = Router::new()
        .route("/query", post(dataframe::query_responder))
        .with_state(session_mgr.clone());

    let session_route = Router::new()
        .route("/", get(session::index))
        .route("/create", get(session::create))
        .route("/:session_id", get(session::detail))
        .route("/:session_id", delete(session::remove))
        .route("/:session_id/query", post(session::query))
        .route("/:session_id/datasource", get(data_source::index))
        .route("/:session_id/datasource", post(data_source::create))
        .route("/:session_id/datasource/save", post(data_source::save))
        .route("/:session_id/datasource/:name", get(data_source::detail))
        .route("/:session_id/datasource/:name", delete(data_source::remove))
        .route(
            "/:session_id/datasource/:name/refresh",
            get(data_source::refresh),
        )
        .route("/:session_id/processor", post(processor::processing))
        .with_state(session_mgr);

    let base_url = get_base_url();

    Router::new()
        .nest(&format!("{base_url}/arrow"), arrow_route)
        .nest(&format!("{base_url}/json"), json_route)
        .nest(&format!("{base_url}/dataframe"), df_route)
        .nest(&format!("{base_url}/session"), session_route)
        .route(&format!("{base_url}/healthz"), get(hc_handler))
        .route(&format!("{base_url}/sysinfo"), get(sys_info::handler))
}

fn get_base_url() -> String {
    let mut base_url = Settings::global().server.base_url.clone();

    if !base_url.starts_with('/') {
        base_url.insert(0, '/');
    }

    if base_url.ends_with('/') {
        base_url.remove(base_url.len() - 1);
    }

    base_url
}

#[allow(clippy::unused_async)] // requires `async` in axum
async fn hc_handler() -> impl IntoResponse {
    log::info!("Accessing health condition endpoint");
    StatusCode::NO_CONTENT
}
