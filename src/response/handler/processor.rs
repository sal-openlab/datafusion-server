// processor - Processor handler
// Sasaki, Naoki <nsasaki@sal.co.jp> August 4, 2023
//

use crate::context::session_manager::SessionManager;
use crate::request::body::Processor;
use crate::response::http_error::ResponseError;
use axum::response::IntoResponse;
use axum::{
    extract::{self, Path},
    http::StatusCode,
};
use std::sync::Arc;

pub async fn processing<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path(session_id): Path<String>,
    extract::Json(payload): extract::Json<Processor>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing processor handler");

    if let Some(merge_processors) = &payload.merge_processors {
        session_mgr
            .lock()
            .await
            .execute_merge_processors(&session_id, merge_processors)
            .await?;
    } else {
        return Err(ResponseError::request_validation(
            "Processors not specified",
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}
