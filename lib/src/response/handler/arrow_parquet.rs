// arrow_parquet.rs - Arrow with parquet file handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 2, 2023
//

use crate::data_source::parquet;
use crate::response::{format::arrow_stream, http_error::ResponseError, http_response};
use crate::settings::Settings;
use axum::{extract::Path, response::IntoResponse};
use std::path::PathBuf;

#[allow(clippy::unused_async)] // requires `async` in axum
pub async fn parquet_responder(
    Path(file_name): Path<String>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!("Accessing parquet to arrow responder");

    let mut file_path = PathBuf::from(&Settings::global().server.data_dir);
    file_path.push(file_name);
    log::debug!("Open parquet file {:?}", file_path.to_str().unwrap());

    let record_batches = parquet::to_record_batch(file_path.to_str().unwrap())?;

    Ok(http_response::from_byte_stream(
        arrow_stream::make_buffered_stream(&record_batches)
            .map_err(ResponseError::arrow_stream_serialization)?,
        "application/vnd.apache.arrow.stream",
    ))
}
