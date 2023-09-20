// response/handler/data_source.rs - Session context / data source handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 15, 2023
//

use crate::context::session_manager::SessionManager;
use crate::data_source::schema::DataSourceSchema;
use crate::request::body::DataSources;
use crate::response::http_error::ResponseError;
use axum::extract;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct DataSourceDetail {
    pub name: String,
    pub location: Option<String>,
    pub schema: DataSourceSchema,
}

pub async fn index<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path(session_id): Path<String>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing index of session data sources responder");

    let session_mgr = session_mgr.lock().await;

    Ok(axum::Json(
        session_mgr.data_source_names(&session_id).await?,
    ))
}

pub async fn detail<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path((session_id, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing session data source detail responder");

    Ok(axum::Json(
        session_mgr
            .lock()
            .await
            .data_source(&session_id, &name)
            .await?,
    ))
}

pub async fn create<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path(session_id): Path<String>,
    extract::Json(payload): extract::Json<DataSources>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing register data sources to session context handler");

    session_mgr
        .lock()
        .await
        .append_data_sources(&session_id, &payload.data_sources)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn save<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path(session_id): Path<String>,
    extract::Json(payload): extract::Json<DataSources>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing save data sources to local file handler");

    session_mgr
        .lock()
        .await
        .save_data_sources(&session_id, &payload.data_sources)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path((session_id, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing data source remove handler");

    session_mgr
        .lock()
        .await
        .remove_data_source(&session_id, &name)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn refresh<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    Path((session_id, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing data source refresh handler");

    session_mgr
        .lock()
        .await
        .refresh_data_source(&session_id, &name)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}
