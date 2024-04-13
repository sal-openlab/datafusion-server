// record_batch_stream.rs

use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::response::http_error::ResponseError;
use axum::{
    body::HttpBody,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::StreamExt;
use http_body::Frame;
use tokio::sync::mpsc;

struct ByteStream {
    receiver: mpsc::Receiver<Result<Bytes, hyper::Error>>,
}

impl HttpBody for ByteStream {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, hyper::Error>>> {
        let self_mut = self.get_mut();

        match self_mut.receiver.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => Poll::Ready(Some(Ok(Frame::data(data)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}

pub fn to_response(stream: SendableRecordBatchStream) -> Result<impl IntoResponse, ResponseError> {
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        let schema = stream.schema();

        stream
            .for_each(move |batch_result| {
                let tx = tx.clone();
                let schema = schema.clone();
                async move {
                    let batch = batch_result.unwrap();
                    let mut buffer = Cursor::new(Vec::new());
                    {
                        let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
                        writer.write(&batch).unwrap();
                        writer.finish().unwrap();
                    }

                    let bytes = buffer.into_inner();
                    tx.send(Ok(Bytes::from(bytes))).await.unwrap();
                }
            })
            .await;
    });

    Ok(Response::new(
        Response::builder()
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .body(ByteStream { receiver: rx })?,
    ))
}
