// builds arrow record batch

use arrow::{error::ArrowError, ipc::reader::StreamReader, record_batch::RecordBatch};

pub fn create(data: Vec<u8>) -> Result<Vec<RecordBatch>, ArrowError> {
    let cursor = std::io::Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;

    let mut record_batches = Vec::new();

    for batch in reader {
        record_batches.push(batch?);
    }

    Ok(record_batches)
}
