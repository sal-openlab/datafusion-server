// nd_json_file.rs - ndJSON file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> March 25, 2023
//

use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Seek};

use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    {json, json::reader::infer_json_schema_from_seekable},
};

use crate::data_source::schema::DataSourceSchema;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub fn from_file_to_record_batch(
    file_path: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let file = BufReader::new(File::open(file_path)?);
    to_record_batch(file, schema, options)
}

pub async fn from_response_to_record_batch(
    uri: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let response = reqwest::get(uri)
        .await
        .map_err(ResponseError::http_request)?
        .bytes()
        .await?;
    to_record_batch(Cursor::new(response), schema, options)
}

#[cfg(feature = "plugin")]
pub fn from_bytes_to_record_batch(
    data: bytes::Bytes,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    to_record_batch(Cursor::new(data), schema, options)
}

fn to_record_batch<R: BufRead + Seek>(
    mut reader: R,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let df_schema = SchemaRef::new(if let Some(schema) = schema {
        schema.to_arrow_schema()
    } else {
        let (inferred_schema, _) =
            infer_json_schema_from_seekable(&mut reader, options.infer_schema_rows)?;
        inferred_schema
    });

    let builder = json::ReaderBuilder::new(df_schema);

    let reader = builder
        .build(reader)
        .map_err(ResponseError::record_batch_creation)?;

    let batches: Result<Vec<RecordBatch>, ArrowError> = reader.collect();

    Ok(batches?)
}
