// response/handler/session.rs - Session context handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 15, 2023
//

use crate::context::session_manager::SessionManager;
#[cfg(feature = "plugin")]
use crate::request::body::PluginOption;
use crate::request::body::SessionQuery;
use crate::response::{http_error::ResponseError, http_response};
#[cfg(feature = "plugin")]
use crate::PluginManager;
use axum::extract::{self, Path, Query};
use axum::response::IntoResponse;
use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize)]
pub struct Session {
    pub id: String,
    pub created: String,
    pub ttl: i64,
}

pub async fn index<E: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing index of session responder");

    let mut response: Vec<Session> = Vec::new();
    {
        let session_mgr = session_mgr.lock().await;
        let session_ids = session_mgr.session_ids().await;

        for session_id in session_ids {
            response.push(session_mgr.session(&session_id).await?);
        }
    }

    Ok(axum::Json(response))
}

pub async fn create<E: SessionManager>(
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing create session handler");

    let keep_alive = match params.get("keepAlive") {
        Some(v) => {
            if v.parse::<i64>().is_ok() {
                Some(v.parse::<i64>().unwrap())
            } else {
                None
            }
        }
        None => None,
    };

    let session_mgr = session_mgr.lock().await;
    let new_session_id = session_mgr.create_new_session(None, keep_alive).await;

    Ok(axum::Json(session_mgr.session(&new_session_id).await?))
}

pub async fn remove<E: SessionManager>(
    Path(session_id): Path<String>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing create session handler");
    session_mgr
        .lock()
        .await
        .destroy_session(&session_id)
        .await?;
    Ok(())
}

pub async fn detail<E: SessionManager>(
    Path(session_id): Path<String>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing session detail responder");
    Ok(axum::Json(
        session_mgr.lock().await.session(&session_id).await?,
    ))
}

pub async fn query<E: SessionManager>(
    Path(session_id): Path<String>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
    extract::Json(payload): extract::Json<SessionQuery>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing session query responder");

    #[cfg(feature = "plugin")]
    let mut record_batches: Vec<RecordBatch>;
    #[cfg(not(feature = "plugin"))]
    let record_batches: Vec<RecordBatch>;
    {
        record_batches = session_mgr
            .lock()
            .await
            .execute_sql(&session_id, &payload.query_lang.sql)
            .await?;
    }

    #[cfg(feature = "plugin")]
    if let Some(processors) = payload.query_lang.post_processors {
        for processor in processors {
            if let Some(module) = processor.module {
                let plugin_options = match &processor.plugin_options {
                    Some(options) => options.clone(),
                    None => PluginOption::new(),
                };

                record_batches = PluginManager::global().py_processor_exec(
                    &record_batches,
                    &module,
                    &plugin_options.options,
                )?;
            } else {
                return Err(ResponseError::request_validation(
                    "Must be defined processor module",
                ));
            }
        }
    }

    Ok(http_response::stream_responder(
        &record_batches,
        &payload.response,
    ))
}
