// infer_schema.rs - Infer schema from data sources
// Sasaki, Naoki <nsasaki@sal.co.jp> March 28, 2023
//

use datafusion::arrow;
use serde_json::Value;

use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub fn from_json_value(
    json_rows: &[Value],
    options: &DataSourceOption,
) -> anyhow::Result<arrow::datatypes::Schema> {
    let infer_schema_rows =
        std::cmp::min(options.infer_schema_rows.unwrap_or(100), json_rows.len());
    infer_schema(json_rows, infer_schema_rows)
}

fn infer_schema(
    json_rows: &[Value],
    max_infer_rows: usize,
) -> anyhow::Result<arrow::datatypes::Schema> {
    Ok(arrow::json::reader::infer_json_schema_from_iterator(
        json_rows[0..max_infer_rows].iter().map(|v| Ok(v.clone())),
    )
    .map_err(ResponseError::record_batch_creation)?)
}
