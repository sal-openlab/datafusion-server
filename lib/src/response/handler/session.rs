// response/handler/session.rs - Session context handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 15, 2023
//

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{self, Path, Query},
    response::IntoResponse,
};
use axum_extra::{either::Either, TypedHeader};
use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;

use crate::context::session_manager::SessionManager;
#[cfg(feature = "plugin")]
use crate::plugin::exec_processor;
use crate::request::body::ResponseFormat;
use crate::request::{body::SessionQuery, header};
use crate::response::{http_error::ResponseError, http_response, record_batch_stream};

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

    let keep_alive = params.get("keepAlive").and_then(|v| v.parse::<i64>().ok());

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
    accept_header: Option<TypedHeader<header::Accept>>,
    Path(session_id): Path<String>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
    extract::Json(payload): extract::Json<SessionQuery>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing session query responder");

    let (query_lang, format, options) = match payload {
        SessionQuery::Query(query) => (
            query,
            http_response::response_format(&None, &accept_header)?,
            None,
        ),
        SessionQuery::QueryWithFormat(query_with_format) => (
            query_with_format.query_lang,
            http_response::response_format(&query_with_format.response, &accept_header)?,
            query_with_format
                .response
                .and_then(|response| response.options),
        ),
    };

    #[cfg(feature = "plugin")]
    let buffered = query_lang.post_processors.is_some();
    #[cfg(not(feature = "plugin"))]
    let buffered = true;

    if buffered || format != ResponseFormat::Arrow {
        #[cfg(feature = "plugin")]
        let mut record_batches: Vec<RecordBatch>;
        #[cfg(not(feature = "plugin"))]
        let record_batches: Vec<RecordBatch>;
        {
            record_batches = session_mgr
                .lock()
                .await
                .execute_sql(&session_id, &query_lang.sql)
                .await?;
        }

        #[cfg(feature = "plugin")]
        if let Some(processors) = query_lang.post_processors {
            record_batches = exec_processor::post_processors(processors, record_batches)?;
        }

        Ok(Either::E1(http_response::buffered_stream_responder(
            &record_batches,
            &format,
            &options,
        )))
    } else {
        let batch_stream = session_mgr
            .lock()
            .await
            .execute_sql_stream(&session_id, &query_lang.sql)
            .await?;

        Ok(Either::E2(record_batch_stream::to_response(batch_stream)?))
    }
}
