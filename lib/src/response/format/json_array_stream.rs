// response/format/json_array_stream.rs - Creates JSON for HTTP response
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use datafusion::arrow::{error::ArrowError, json::ArrayWriter, record_batch::RecordBatch};

pub fn make_buffered_stream(record_batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    let mut writer = ArrayWriter::new(&mut buf);

    let record_batch_refs: Vec<&RecordBatch> = record_batches.iter().collect();
    writer.write_batches(&record_batch_refs)?;
    writer.finish()?;

    Ok(buf)
}
