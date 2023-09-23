// parquet.rs - Parquet file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use crate::response::http_error::ResponseError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

pub fn to_record_batch(file_path: &str) -> Result<Vec<RecordBatch>, ResponseError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(ResponseError::parquet_deserialization)?;
    let reader = builder
        .build()
        .map_err(ResponseError::parquet_deserialization)?;

    // TODO: reader.into_iter().map(|batch| batch?).collect();
    let mut record_batches = Vec::<RecordBatch>::new();

    for record_batch in reader {
        record_batches.push(record_batch?);
    }

    Ok(record_batches)
}
