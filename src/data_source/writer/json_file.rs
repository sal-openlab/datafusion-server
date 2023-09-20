// write/json_file.rs - Native JSON file writer
// Sasaki, Naoki <nsasaki@sal.co.jp> May 4, 2023
//

use crate::response::http_error::ResponseError;
use datafusion::arrow::{
    json::writer::{JsonArray, Writer},
    record_batch::RecordBatch,
};
use std::fs::File;

pub fn write(record_batches: &Vec<RecordBatch>, file_name: &str) -> Result<(), ResponseError> {
    if !record_batches.is_empty() {
        let file = File::create(file_name)?;
        let mut writer = Writer::<File, JsonArray>::new(file);

        // TODO: changed `&Vec<batch>` to `&[&batch]` in Arrow v40, but still `&Vec<batch>` in DataFusion v26
        // May be removed this conversion near the future
        let mut batches = vec![];
        for batch in record_batches {
            batches.push(batch);
        }

        writer.write_batches(batches.as_slice())?;
        writer.finish()?;
    }

    Ok(())
}
