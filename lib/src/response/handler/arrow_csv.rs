// arrow_csv.rs - Arrow with CSV file handler
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use crate::data_source::csv_file;
use crate::data_source::schema::DataSourceSchema;
use crate::request::body::DataSourceOption;
use crate::request::query_param;
use crate::response::{format::arrow_stream, http_error::ResponseError, http_response};
use crate::settings::Settings;
use axum::{
    extract::{Path, Query},
    response::IntoResponse,
};
use std::collections::HashMap;
use std::path::PathBuf;

#[allow(clippy::unused_async)] // requires `async` in axum
pub async fn csv_responder(
    Path(file_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ResponseError> {
    log::info!(
        "Accessing CSV to arrow responder with query param {:?}",
        params
    );

    let mut file_path = PathBuf::from(&Settings::global().server.data_dir);
    file_path.push(file_name);
    log::debug!("Open CSV file {:?}", file_path.to_str().unwrap());

    let mut options = DataSourceOption::new();
    options.infer_schema_rows = Some(query_param::usize_or_default(
        params.get("inferSchemaRows"),
        100,
    ));
    options.has_header = Some(query_param::bool_or_default(params.get("hasHeader"), true));

    let schema: Option<DataSourceSchema> = None;
    let record_batches = csv_file::to_record_batch(file_path.to_str().unwrap(), &schema, &options)?;

    Ok(http_response::from_byte_stream(
        arrow_stream::make_stream(&record_batches)
            .map_err(ResponseError::arrow_stream_serialization)?,
        "application/vnd.apache.arrow.stream",
    ))
}
