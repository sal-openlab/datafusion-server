// infer_schema.rs - Infer schema from data sources
// Sasaki, Naoki <nsasaki@sal.co.jp> March 28, 2023
//

use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow;
use serde_json::Value;
use std::io::BufRead;

pub fn from_json_value(
    json_rows: &Vec<Value>,
    options: &DataSourceOption,
) -> anyhow::Result<arrow::datatypes::Schema> {
    let infer_schema_rows =
        std::cmp::min(options.infer_schema_rows.unwrap_or(100), json_rows.len());
    infer_schema(json_rows, infer_schema_rows)
}

pub fn from_raw_json(
    raw_json_text: &str,
    options: &DataSourceOption,
) -> anyhow::Result<arrow::datatypes::Schema> {
    let max_infer_rows = options.infer_schema_rows.unwrap_or(100);
    let mut cursor = std::io::Cursor::new(raw_json_text);
    let mut buf = String::new();
    let mut json_rows: Vec<Value> = vec![];

    loop {
        let num_of_bytes = cursor.read_line(&mut buf)?;

        if num_of_bytes == 0 {
            break;
        }

        buf = buf.trim().parse()?;

        if buf.is_empty() {
            continue;
        }

        json_rows.push(serde_json::from_str(&buf)?);

        buf.clear();

        if json_rows.len() >= max_infer_rows {
            break;
        }
    }

    infer_schema(&json_rows, json_rows.len())
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
