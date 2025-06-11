// variable.rs - Variable handler
// Sasaki, Naoki <nsasaki@sal.co.jp> May 24, 2025
//

use crate::context::session_manager::SessionManager;
use crate::request::body::Variables;
use crate::response::http_error::ResponseError;
use axum::response::IntoResponse;
use axum::{
    extract::{self, Path},
    http::StatusCode,
};
use std::sync::Arc;

pub async fn register<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path(session_id): Path<String>,
    extract::Json(payload): extract::Json<Variables>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing processor handler");

    session_mgr
        .lock()
        .await
        .append_variables(&session_id, &payload)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}
