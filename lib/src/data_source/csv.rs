// csv_file.rs - CSV file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::fs::File;
use std::io::{Read, Seek};
use std::sync::Arc;

use datafusion::arrow::{csv, datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};

use crate::data_source::schema::DataSourceSchema;
use crate::data_source::transport::http;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub fn from_file_to_record_batch(
    file_path: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let file = File::open(file_path)?;
    to_record_batch(file, schema, options)
}

pub async fn from_response_to_record_batch(
    uri: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    from_bytes_to_record_batch(
        match http::get(uri, options, http::ResponseDataType::Binary).await? {
            http::ResponseData::Binary(data) => data,
            http::ResponseData::Text(_) => bytes::Bytes::new(),
        },
        schema,
        options,
    )
}

pub fn from_bytes_to_record_batch(
    data: bytes::Bytes,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let cursor = std::io::Cursor::new(data);
    to_record_batch(cursor, schema, options)
}

fn to_record_batch<R: Read + Seek>(
    mut reader: R,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let has_header = options.has_header.unwrap_or(true);
    let delimiter = options.delimiter.unwrap_or(',') as u8;

    let df_schema = if let Some(schema) = schema {
        SchemaRef::new(schema.to_arrow_schema())
    } else {
        let format = csv::reader::Format::default()
            .with_header(has_header)
            .with_delimiter(delimiter);
        let (schema, _) =
            format.infer_schema(&mut reader, Some(options.infer_schema_rows.unwrap_or(100)))?;
        reader.rewind()?;
        Arc::new(schema)
    };

    let builder = csv::ReaderBuilder::new(df_schema)
        .with_header(has_header)
        .with_delimiter(delimiter);

    let reader = builder
        .build(reader)
        .map_err(ResponseError::record_batch_creation)?;

    let batches: Result<Vec<RecordBatch>, ArrowError> = reader.collect();

    Ok(batches?)
}
