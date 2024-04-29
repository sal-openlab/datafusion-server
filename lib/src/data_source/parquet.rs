// parquet.rs - Parquet file to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::fs::File;

use datafusion::{
    arrow::{error::ArrowError, record_batch::RecordBatch},
    parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::ChunkReader},
};

use crate::response::http_error::ResponseError;

pub fn from_file_to_record_batch(file_path: &str) -> Result<Vec<RecordBatch>, ResponseError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(ResponseError::parquet_deserialization)?;

    to_record_batch(builder)
}

pub fn from_bytes_to_record_batch(data: bytes::Bytes) -> Result<Vec<RecordBatch>, ResponseError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)
        .map_err(ResponseError::parquet_deserialization)?;

    to_record_batch(builder)
}

fn to_record_batch<T>(
    builder: ParquetRecordBatchReaderBuilder<T>,
) -> Result<Vec<RecordBatch>, ResponseError>
where
    T: ChunkReader + 'static,
{
    let reader = builder
        .build()
        .map_err(ResponseError::parquet_deserialization)?;

    let batches: Result<Vec<RecordBatch>, ArrowError> = reader.collect();

    Ok(batches?)
}
