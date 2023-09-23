// response/arrow_stream - Creates arrow for HTTP response
// Sasaki, Naoki <nsasaki@sal.co.jp> November 19, 2022
//

use datafusion::arrow::{error::ArrowError, ipc::writer::StreamWriter, record_batch::RecordBatch};

pub fn make_stream(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();

    if !batches.is_empty() {
        let schema = batches[0].schema();
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
    }

    Ok(buf)
}
