// write/csv_file.rs - CSV file writer
// Sasaki, Naoki <nsasaki@sal.co.jp> May 4, 2023
//

use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use datafusion::arrow::{csv::WriterBuilder, record_batch::RecordBatch};
use std::fs::File;

pub fn write(
    record_batches: &Vec<RecordBatch>,
    file_name: &str,
    options: &DataSourceOption,
) -> Result<(), ResponseError> {
    if !record_batches.is_empty() {
        let file = File::create(file_name)?;

        // TODO: to be specified date time format - https://docs.rs/arrow-csv/38.0.0/arrow_csv/writer/struct.WriterBuilder.html
        let mut writer = WriterBuilder::new()
            .with_header(options.has_header.unwrap_or(true))
            .build(file);

        for record_batch in record_batches {
            writer.write(record_batch)?;
        }
    }

    Ok(())
}
