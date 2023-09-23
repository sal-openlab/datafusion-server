// response/format/json_array_stream.rs - Creates JSON for HTTP response
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use datafusion::arrow::{error::ArrowError, json::ArrayWriter, record_batch::RecordBatch};

pub fn make_stream(record_batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    let mut writer = ArrayWriter::new(&mut buf);

    // TODO: changed `&[batch]` to `&[&batch]` in Arrow v40, but still `&[batch]` in DataFusion v28
    // writer.write_batches(record_batches)?;
    // May be removed this conversion near the future
    for batch in &record_batches.to_vec() {
        writer.write_batches(&[batch])?;
    }

    writer.finish()?;

    Ok(buf)
}
