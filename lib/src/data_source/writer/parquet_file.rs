// write/parquet_file.rs - Parquet file writer
// Sasaki, Naoki <nsasaki@sal.co.jp> May 4, 2023
//

use crate::response::http_error::ResponseError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::{
    arrow::arrow_writer::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use std::fs::File;

pub fn write(record_batches: &Vec<RecordBatch>, file_name: &str) -> Result<(), ResponseError> {
    if !record_batches.is_empty() {
        let file = File::create(file_name)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_created_by(format!("datafusion-server v{}", env!("CARGO_PKG_VERSION")))
            .build();

        log::debug!(
            "writing to {:?} with {} record batches",
            file,
            record_batches.len()
        );

        let mut writer = ArrowWriter::try_new(file, record_batches[0].schema(), Some(props))
            .map_err(ResponseError::parquet_serialization)?;

        for record_batch in record_batches {
            writer
                .write(record_batch)
                .map_err(ResponseError::parquet_serialization)?;
        }

        writer
            .close()
            .map_err(ResponseError::parquet_serialization)?;
    }

    Ok(())
}
