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
use crate::request::{
    body::{ResponseFormat, SessionQuery},
    header,
};
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
    content_type: TypedHeader<header::ContentType>,
    accept_header: Option<TypedHeader<header::Accept>>,
    Path(session_id): Path<String>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<E>>>,
    payload: bytes::Bytes,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing session query responder");

    if let Ok(content_type) = header::request_format(&content_type) {
        match &*content_type {
            "application/json" => {
                let body: SessionQuery = serde_json::from_slice(&payload)?;
                Ok(Either::E1(
                    query_by_json(accept_header, body, &session_mgr, &session_id).await?,
                ))
            }
            "application/sql" => {
                let sql = String::from_utf8(payload.to_vec()).map_err(|e| {
                    ResponseError::request_validation(format!("Incorrect request body: {e}"))
                })?;
                Ok(Either::E2(
                    query_by_sql(accept_header, &sql, &session_mgr, &session_id).await?,
                ))
            }
            _ => Err(ResponseError::unsupported_format(format!(
                "Unsupported content-type: {content_type}"
            ))),
        }
    } else {
        Err(ResponseError::unsupported_format(
            "Incorrect content-type header",
        ))
    }
}

async fn query_by_json<E: SessionManager>(
    accept_header: Option<TypedHeader<header::Accept>>,
    body: SessionQuery,
    session_mgr: &tokio::sync::Mutex<E>,
    session_id: &str,
) -> Result<impl IntoResponse, ResponseError> {
    let (query_lang, format, options) = match body {
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
        let mut batches: Vec<RecordBatch>;
        #[cfg(not(feature = "plugin"))]
        let batches: Vec<RecordBatch>;
        {
            batches = session_mgr
                .lock()
                .await
                .execute_sql(session_id, &query_lang.sql)
                .await?;
        }

        #[cfg(feature = "plugin")]
        if let Some(processors) = query_lang.post_processors {
            batches = exec_processor::post_processors(processors, batches)?;
        }

        Ok(Either::E1(http_response::buffered_stream_responder(
            &batches, &format, &options,
        )?))
    } else {
        let stream = session_mgr
            .lock()
            .await
            .execute_sql_stream(session_id, &query_lang.sql)
            .await?;

        Ok(Either::E2(record_batch_stream::to_response(stream)?))
    }
}

async fn query_by_sql<E: SessionManager>(
    accept_header: Option<TypedHeader<header::Accept>>,
    sql: &str,
    session_mgr: &tokio::sync::Mutex<E>,
    session_id: &str,
) -> Result<impl IntoResponse, ResponseError> {
    let format = http_response::response_format(&None, &accept_header)?;

    Ok(if format == ResponseFormat::Arrow {
        let stream = session_mgr
            .lock()
            .await
            .execute_sql_stream(session_id, sql)
            .await?;
        Either::E1(record_batch_stream::to_response(stream)?)
    } else {
        let batches = session_mgr
            .lock()
            .await
            .execute_sql(session_id, sql)
            .await?;
        Either::E2(http_response::buffered_stream_responder(
            &batches, &format, &None,
        )?)
    })
}
