//! A [`SourceStream`] adapter for any source that implements [`AsyncRead`].

use std::convert::Infallible;
use std::io;
use std::pin::Pin;

use crate::source::StreamMsg;

use futures_util::Stream;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

use crate::source::SourceStream;
use crate::storage::ContentLength;

/// Parameters for creating an [`AsyncReadStream`].
#[derive(Debug)]
pub struct AsyncReadStreamParams<T> {
    stream: T,
    content_length: ContentLength,
}

impl<T> AsyncReadStreamParams<T> {
    /// Creates a new [`AsyncReadStreamParams`] instance.
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            content_length: ContentLength::Unknown,
        }
    }

    /// Sets the content length of the stream.
    /// A generic [`AsyncRead`] source has no way of knowing the content length automatically, so it
    /// must be set explicitly or it will default to [`None`].
    #[must_use]
    pub fn content_length<L>(self, content_length: L) -> Self
    where
        L: Into<ContentLength>,
    {
        Self {
            content_length: content_length.into(),
            ..self
        }
    }
}

/// An implementation of the [`SourceStream`] trait for any stream implementing [`AsyncRead`].
#[derive(Debug)]
pub struct AsyncReadStream<T> {
    stream: ReaderStream<T>,
    content_length: ContentLength,
}

impl<T> AsyncReadStream<T>
where
    T: AsyncRead + Send + Sync + Unpin + 'static,
{
    /// Creates a new [`AsyncReadStream`].
    pub fn new<L>(stream: T, content_length: L) -> Self
    where
        L: Into<ContentLength>,
    {
        Self {
            stream: ReaderStream::new(stream),
            content_length: content_length.into(),
        }
    }
}

impl<T> SourceStream for AsyncReadStream<T>
where
    T: AsyncRead + Send + Sync + Unpin + 'static,
{
    type Params = AsyncReadStreamParams<T>;

    type StreamCreationError = Infallible;

    async fn create(params: Self::Params) -> Result<Self, Self::StreamCreationError> {
        Ok(Self::new(params.stream, params.content_length))
    }

    fn content_length(&self) -> ContentLength {
        self.content_length.clone()
    }

    fn supports_seek(&self) -> bool {
        false
    }

    async fn seek_range(&mut self, _start: u64, _end: Option<u64>) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "seek unsupported",
        ))
    }

    async fn reconnect(&mut self, _current_position: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "reconnect unsupported",
        ))
    }
}

impl<T> Stream for AsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = io::Result<StreamMsg>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                std::task::Poll::Ready(Some(Ok(StreamMsg::Data(bytes))))
            }
            std::task::Poll::Ready(Some(Err(e))) => std::task::Poll::Ready(Some(Err(e))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
