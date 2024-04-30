// response/handler/data_source.rs - Session context / data source handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 15, 2023
//

use std::sync::Arc;

use axum::{extract, http::StatusCode, response::IntoResponse};
use serde::Serialize;

use crate::context::session_manager::SessionManager;
use crate::data_source::schema::DataSourceSchema;
use crate::request::{
    body::{DataSourceFormat, DataSources},
    format,
};
use crate::response::http_error::ResponseError;

#[derive(Serialize)]
#[allow(clippy::module_name_repetitions)]
pub struct DataSourceDetail {
    pub name: String,
    pub location: Option<String>,
    pub schema: DataSourceSchema,
}

pub async fn index<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Path(session_id): extract::Path<String>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing index of session data sources responder");

    let session_mgr = session_mgr.lock().await;

    Ok(axum::Json(
        session_mgr.data_source_names(&session_id).await?,
    ))
}

pub async fn detail<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Path((session_id, name)): extract::Path<(String, String)>,
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
    extract::Path(session_id): extract::Path<String>,
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

pub async fn upload<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Path(session_id): extract::Path<String>,
    mut multipart: extract::Multipart,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing upload tables to session context handler");

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| ResponseError::request_validation(format!("Invalid multipart content: {e}")))?
    {
        let format = format::resolve_from(field.content_type(), field.file_name());

        let name = field
            .name()
            .map(String::from)
            .ok_or(ResponseError::request_validation(
                "Field name is must required",
            ))?;

        let mut bytes_buffer = bytes::BytesMut::new();

        while let Some(chunk) = field.chunk().await.map_err(|e| {
            ResponseError::payload_too_large(format!(
                "Invalid multipart chunk, May be exceeds payload size limit: {e}"
            ))
        })? {
            bytes_buffer.extend_from_slice(&chunk);
        }

        log::debug!(
            "multipart field: format={format:?}, name={name:?}, length={}",
            bytes_buffer.len()
        );

        let locked_session_mgr = session_mgr.lock().await;

        match format {
            Some(DataSourceFormat::Parquet) => {
                locked_session_mgr
                    .append_parquet_bytes(&session_id, &name, bytes_buffer.freeze())
                    .await?;
            }
            Some(DataSourceFormat::Json) => {
                locked_session_mgr
                    .append_json_bytes(&session_id, &name, bytes_buffer.freeze())
                    .await?;
            }
            Some(DataSourceFormat::Csv) => {
                locked_session_mgr
                    .append_csv_bytes(&session_id, &name, bytes_buffer.freeze())
                    .await?;
            }
            _ => {
                return Err(ResponseError::unsupported_format(
                    "content-type of the multipart field must be either \
                            'text/csv', 'application/json', or 'application/vnd.apache.parquet'",
                ));
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn save<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Path(session_id): extract::Path<String>,
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
    extract::Path((session_id, name)): extract::Path<(String, String)>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing remove data source handler");

    session_mgr
        .lock()
        .await
        .remove_data_source(&session_id, &name)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn refresh<S: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Path((session_id, name)): extract::Path<(String, String)>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing refresh data sources handler");

    session_mgr
        .lock()
        .await
        .refresh_data_source(&session_id, &name)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}
