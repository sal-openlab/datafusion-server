// data_frame - Query result handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::sync::Arc;

use axum::{extract, response::IntoResponse};
use axum_extra::TypedHeader;
use datafusion::arrow::record_batch::RecordBatch;

use crate::context::session_manager::SessionManager;
use crate::request::{body::DataFrameQuery, header};
use crate::response::{http_error::ResponseError, http_response};

pub async fn query_responder<S: SessionManager>(
    accept_header: Option<TypedHeader<header::Accept>>,
    extract::State(session_mgr): extract::State<Arc<tokio::sync::Mutex<S>>>,
    extract::Json(payload): extract::Json<DataFrameQuery>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing request query body to arrow responder");
    log::debug!("Accept Header: {:?}", accept_header);
    log::trace!("Request Body: {:?}", payload);

    let record_batches: Vec<RecordBatch>;
    {
        let session_mgr = session_mgr.lock().await;
        let session_id = session_mgr.create_new_session(None, None).await;

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

        record_batches = session_mgr
            .execute_sql(&session_id, &payload.query_lang.sql)
            .await?;

        session_mgr.destroy_session(&session_id).await?;
    }

    Ok(http_response::stream_responder(
        &record_batches,
        &payload.response,
        &accept_header,
    ))
}
