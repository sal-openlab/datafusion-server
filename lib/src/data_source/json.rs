// json_file.rs - JSON file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::fs::File;
use std::io::{BufReader, Read};

use datafusion::arrow::record_batch::RecordBatch;

use crate::data_source::{
    decoder::build_record_batch, schema::DataSourceSchema, transport::http, with_jsonpath,
};
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub fn from_file_to_record_batch(
    file_path: &str,
    schema: Option<&DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let mut file_reader = BufReader::new(File::open(file_path)?);
    let mut json_text = String::new();
    file_reader.read_to_string(&mut json_text)?;

    Ok(if options.json_path.is_none() {
        build_record_batch::from_json(&json_text, schema, options)?
    } else {
        with_jsonpath::to_record_batch(&json_text, schema, options)?
    })
}

pub async fn from_response_to_record_batch(
    uri: &str,
    schema: Option<&DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let response = match http::get(uri, options, http::ResponseDataType::Text).await? {
        http::ResponseData::Text(data) => data,
        http::ResponseData::Binary(_) => String::new(),
    };

    Ok(if options.json_path.is_none() {
        build_record_batch::from_json(&response, schema, options)?
    } else {
        with_jsonpath::to_record_batch(&response, schema, options)?
    })
}

pub fn from_bytes_to_record_batch(
    data: &bytes::Bytes,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    Ok(build_record_batch::from_json(
        std::str::from_utf8(data).map_err(|e| {
            ResponseError::request_validation(format!("Collapsed bytes buffer: {e}"))
        })?,
        None,
        options,
    )?)
}
