// raw_json_rest - Raw JSON to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> March 27, 2023
//

use crate::data_source::{infer_schema, schema::DataSourceSchema};
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::{datatypes::SchemaRef, json, record_batch::RecordBatch};

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

    let schema_ref = SchemaRef::new(if let Some(schema) = schema {
        schema.to_datafusion_schema()
    } else {
        infer_schema::from_raw_json(&response, options)?
    });

    let mut record_batches = Vec::<RecordBatch>::new();

    let builder = json::ReaderBuilder::new(schema_ref);

    let reader = builder
        .build(std::io::Cursor::new(response))
        .map_err(ResponseError::record_batch_creation)?;

    for record_batch in reader {
        record_batches.push(record_batch.map_err(ResponseError::record_batch_extraction)?);
    }

    Ok(record_batches)
}
