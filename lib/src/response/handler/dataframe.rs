// data_frame - Query result handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::collections::HashMap;
use std::sync::Arc;

use axum::{extract, response::IntoResponse};
use axum_extra::{either::Either, TypedHeader};
use datafusion::arrow::record_batch::RecordBatch;

use crate::context::session_manager::SessionManager;
#[cfg(feature = "plugin")]
use crate::plugin::exec_processor;
use crate::request::{body::DataFrameQuery, body::ResponseFormat, header};
use crate::response::{http_error::ResponseError, http_response, record_batch_stream};

pub async fn query_responder<S: SessionManager>(
    accept_header: Option<TypedHeader<header::Accept>>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Query(params): extract::Query<HashMap<String, String>>,
    extract::Json(payload): extract::Json<DataFrameQuery>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing request query body to arrow responder");
    log::debug!("Accept Header: {:?}", accept_header);
    log::trace!("Request Body: {:?}", payload);

    let keep_alive = params
        .get("keepAlive")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(60);

    let session_mgr = session_mgr.lock().await;
    let session_id = session_mgr
        .create_new_session(None, Some(keep_alive), None)
        .await?;

    session_mgr
        .append_data_sources(&session_id, &payload.data_sources)
        .await?;

    if let Some(processor) = &payload.processor {
        if let Some(merge_processors) = &processor.merge_processors {
            session_mgr
                .execute_merge_processors(&session_id, merge_processors)
                .await?;
        }
    }

    let (format, options) = (
        http_response::response_format(payload.response.as_ref(), accept_header.as_ref())?,
        payload.response.and_then(|response| response.options),
    );

    #[cfg(feature = "plugin")]
    let buffered = payload.query_lang.post_processors.is_some();
    #[cfg(not(feature = "plugin"))]
    let buffered = true;

    if buffered || format != ResponseFormat::Arrow {
        #[cfg(feature = "plugin")]
        let mut record_batches: Vec<RecordBatch>;
        #[cfg(not(feature = "plugin"))]
        let record_batches: Vec<RecordBatch>;
        {
            record_batches = session_mgr
                .execute_sql(&session_id, &payload.query_lang.sql)
                .await?;
        }

        #[cfg(feature = "plugin")]
        if let Some(processors) = payload.query_lang.post_processors {
            record_batches = exec_processor::post_processors(processors, record_batches)?;
        }

        session_mgr.destroy_session(&session_id).await?;

        Ok(Either::E1(http_response::buffered_stream_responder(
            &record_batches,
            &format,
            options.as_ref(),
        )))
    } else {
        let batch_stream = session_mgr
            .execute_sql_stream(&session_id, &payload.query_lang.sql)
            .await?;

        Ok(Either::E2(record_batch_stream::to_response(batch_stream)?))

        // Session will be destroyed automatically after about `keep_alive` seconds
    }
}
