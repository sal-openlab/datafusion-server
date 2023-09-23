// csv_file.rs - CSV file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use crate::data_source::schema::DataSourceSchema;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::{csv, record_batch::RecordBatch};
use std::fs::File;
use std::io::Seek;
use std::sync::Arc;

pub fn to_record_batch(
    file_path: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let mut file = File::open(file_path)?;

    let has_header = options.has_header.unwrap_or(true);
    let delimiter = options.delimiter.unwrap_or(',') as u8;

    let df_schema = if let Some(schema) = schema {
        SchemaRef::new(schema.to_datafusion_schema())
    } else {
        let format = csv::reader::Format::default()
            .with_header(has_header)
            .with_delimiter(delimiter);
        let (schema, _) =
            format.infer_schema(&mut file, Some(options.infer_schema_rows.unwrap_or(100)))?;
        file.rewind()?;
        Arc::new(schema)
    };

    let builder = csv::ReaderBuilder::new(df_schema)
        .has_header(has_header)
        .with_delimiter(delimiter);

    let reader = builder
        .build(file)
        .map_err(ResponseError::record_batch_creation)?;

    let mut record_batches = Vec::<RecordBatch>::new();

    for record_batch in reader {
        record_batches.push(record_batch?);
    }

    log::debug!("created number of record batches {}", record_batches.len());

    Ok(record_batches)
}
