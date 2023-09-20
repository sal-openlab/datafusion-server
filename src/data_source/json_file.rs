// json_file.rs - JSON file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use crate::data_source::reader::build_record_batch;
use crate::data_source::{schema::DataSourceSchema, with_jsonpath};
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::{BufReader, Read};

pub fn to_record_batch(
    file_path: &str,
    schema: &Option<DataSourceSchema>,
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
