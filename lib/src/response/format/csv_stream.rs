// response/format/csv_stream.rs - Creates CSV for HTTP response
// Sasaki, Naoki <nsasaki@sal.co.jp> September 3, 2023
//

use crate::request::body::ResponseFormatOption;
use datafusion::arrow::record_batch::RecordBatchWriter;
use datafusion::arrow::{csv::WriterBuilder, error::ArrowError, record_batch::RecordBatch};

pub fn make_buffered_stream(
    record_batches: &[RecordBatch],
    options: &ResponseFormatOption,
) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();

    let builder = WriterBuilder::new()
        .with_header(options.has_headers.unwrap_or(true))
        .with_delimiter(options.delimiter.unwrap_or(',') as u8);

    let mut writer = builder.build(&mut buf);

    for batch in &record_batches.to_vec() {
        writer.write(batch)?;
    }

    writer.close()?;

    Ok(buf)
}
