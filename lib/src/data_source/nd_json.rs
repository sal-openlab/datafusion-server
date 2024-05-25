// nd_json_file.rs - ndJSON file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> March 25, 2023
//

use std::io::{BufRead, Cursor, Seek};

use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    {json, json::reader::infer_json_schema_from_seekable},
};

use crate::data_source::schema::DataSourceSchema;
use crate::data_source::transport::http;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub async fn from_response_to_record_batch(
    uri: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let response = match http::get(uri, options, http::ResponseDataType::Binary).await? {
        http::ResponseData::Text(_) => bytes::Bytes::new(),
        http::ResponseData::Binary(data) => data,
    };
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
