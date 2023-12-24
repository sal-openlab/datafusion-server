// raw_json_file.rs - Raw JSON file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> March 25, 2023
//

use crate::data_source::schema::DataSourceSchema;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::{
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    {json, json::reader::infer_json_schema_from_seekable},
};
use std::fs::File;
use std::io::BufReader;

pub fn to_record_batch(
    file_path: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let mut file_reader = BufReader::new(File::open(file_path)?);

    let builder = json::ReaderBuilder::new(SchemaRef::new(if let Some(schema) = schema {
        schema.to_datafusion_schema()
    } else {
        let (inferred_schema, _) = infer_json_schema_from_seekable(&mut file_reader, options.infer_schema_rows)?;
        inferred_schema
    }));

    let reader = builder
        .build(file_reader)
        .map_err(ResponseError::record_batch_creation)?;

    let mut record_batches = Vec::<RecordBatch>::new();

    for record_batch in reader {
        record_batches.push(record_batch?);
    }

    Ok(record_batches)
}
