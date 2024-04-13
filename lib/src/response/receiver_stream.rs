// receiver_stream.rs

use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;

#[derive(Debug)]
pub struct Receive<T> {
    inner: Receiver<T>,
}

impl<T> Receive<T> {
    pub fn new(recv: Receiver<T>) -> Self {
        Self { inner: recv }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<T> {
        self.inner
    }

    #[allow(dead_code)]
    pub fn close(&mut self) {
        self.inner.close();
    }
}

impl<T> Stream for Receive<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl<T> AsRef<Receiver<T>> for Receive<T> {
    fn as_ref(&self) -> &Receiver<T> {
        &self.inner
    }
}

impl<T> AsMut<Receiver<T>> for Receive<T> {
    fn as_mut(&mut self) -> &mut Receiver<T> {
        &mut self.inner
    }
}

impl<T> From<Receiver<T>> for Receive<T> {
    fn from(recv: Receiver<T>) -> Self {
        Self::new(recv)
    }
}
