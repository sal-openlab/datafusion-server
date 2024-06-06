// routes.rs - Register routes
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use tokio::sync::Mutex;

use crate::context::session_manager::SessionManager;
use crate::response::handler::{data_source, dataframe, processor, session, sys_info};
use crate::settings::Settings;

pub fn register<S: SessionManager>(session_mgr: &Arc<Mutex<S>>) -> Router {
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
        .with_state(session_mgr.clone());

    let session_upload_route = Router::new()
        .route("/:session_id/datasource/upload", post(data_source::upload))
        .layer(DefaultBodyLimit::max(
            Settings::global().session.upload_limit_size * 1024 * 1024,
        ))
        .with_state(session_mgr.clone());

    let base_url = get_base_url();

    Router::new()
        .nest(&format!("{base_url}/dataframe"), df_route)
        .nest(&format!("{base_url}/session"), session_route)
        .nest(&format!("{base_url}/session"), session_upload_route)
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
