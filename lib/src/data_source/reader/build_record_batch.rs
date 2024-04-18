// build_record_batch.rs - Build  RecordBatch from native JSON text
// Sasaki, Naoki <nsasaki@sal.co.jp> March 29, 2023
//

use crate::data_source::{infer_schema, reader::json_decoder, schema::DataSourceSchema};
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use serde_json::Value;

pub fn from_json(
    json_text: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> anyhow::Result<Vec<RecordBatch>> {
    let json: Value = serde_json::from_str(json_text).map_err(|e| {
        ResponseError::json_parsing(format!("Can not parse a JSON from data source: {e}"))
    })?;

    let json_rows = json
        .as_array()
        .ok_or_else(|| ResponseError::json_parsing("Parsed JSON is not array"))?;

    log::debug!("number of parsed JSON objects: {}", json_rows.len());

    let df_schema = if schema.is_some() {
        schema.clone().unwrap().to_arrow_schema()
    } else {
        infer_schema::from_json_value(json_rows, options)?
    };

    log::debug!("detected: {:?}", df_schema);

    let mut record_batches = Vec::<RecordBatch>::new();
    {
        let mut values: Box<dyn Iterator<Item = datafusion::arrow::error::Result<Value>>> =
            Box::new(json_rows.clone().into_iter().map(Ok));

        let decoder = json_decoder::Decoder::new(
            SchemaRef::new(df_schema),
            json_decoder::DecoderOptions::new(),
        );

        while let Some(batch) = decoder
            .next_batch(&mut values)
            .map_err(ResponseError::record_batch_extraction)?
        {
            record_batches.push(batch);
        }
    }

    Ok(record_batches)
}
