// json_rest - JSON to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 7, 2023
//

use crate::data_source::reader::build_record_batch;
use crate::data_source::{schema::DataSourceSchema, with_jsonpath};
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::record_batch::RecordBatch;

pub async fn to_record_batch(
    uri: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let response = reqwest::get(uri)
        .await
        .map_err(ResponseError::http_request)?
        .text()
        .await?;

    Ok(if options.json_path.is_none() {
        build_record_batch::from_json(&response, schema, options)?
    } else {
        with_jsonpath::to_record_batch(&response, schema, options)?
    })
}
