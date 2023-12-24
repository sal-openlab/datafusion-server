// http_response.rs - Raw bytes to response stream
// Sasaki, Naoki <nsasaki@sal.co.jp> November 19, 2022
//

use crate::request::body::{QueryResponse, ResponseFormat, ResponseFormatOption};
use crate::response::{
    format::{arrow_stream, csv_stream, json_array_stream},
    http_error::ResponseError,
};
use axum::{
    body::Body,
    http::{header, Response},
    response::IntoResponse,
};
use datafusion::arrow::record_batch::RecordBatch;

pub fn stream_responder(
    record_batches: &[RecordBatch],
    query_response: &Option<QueryResponse>,
) -> Result<impl IntoResponse, ResponseError> {
    let response = if let Some(response) = &query_response {
        response.clone()
    } else {
        QueryResponse::new()
    };

    Ok(match response.format {
        ResponseFormat::Arrow => from_byte_stream(
            arrow_stream::make_stream(record_batches)
                .map_err(ResponseError::arrow_stream_serialization)?,
            "application/vnd.apache.arrow.stream",
        ),
        ResponseFormat::Json => from_byte_stream(
            json_array_stream::make_stream(record_batches)
                .map_err(ResponseError::json_stream_serialization)?,
            "application/json",
        ),
        ResponseFormat::Csv => {
            let options = response.options.unwrap_or(ResponseFormatOption::new());
            from_byte_stream(
                csv_stream::make_stream(record_batches, &options)
                    .map_err(ResponseError::json_stream_serialization)?,
                "text/csv; charset=utf-8",
            )
        }
    })
}

#[inline]
pub fn from_byte_stream(bytes: Vec<u8>, content_type: &'static str) -> impl IntoResponse {
    let mut res = Response::new(Body::from(bytes));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    res
}