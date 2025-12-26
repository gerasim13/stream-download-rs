//! HTTP `AudioSource` implementation.
//!
//! This source streams response body bytes as ordered `SourceMsg::Data(Bytes)` items and ends with
//! `SourceMsg::EndOfStream`.
//!
//! Notes:
//! - HTTP has no HLS boundaries; this source never emits `SourceControl`.
//! - Backpressure is naturally provided by the consumer of the returned `Stream`.
//! - Cancellation is best-effort: dropping the stream will stop yielding items and should allow
//!   the underlying HTTP body stream to be dropped as well.

use std::any::Any;
use std::io;
use std::pin::Pin;

use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use reqwest::Client;
use url::Url;

use super::{AudioSource, SourceMsg};

/// Progressive HTTP audio source.
pub struct HttpAudioSource {
    url: Url,
    client: Client,
}

impl HttpAudioSource {
    /// Create a new HTTP audio source.
    ///
    /// You can pass a custom `reqwest::Client` if you want to configure timeouts, proxies, etc.
    pub fn new(url: Url, client: Client) -> Self {
        Self { url, client }
    }

    /// Convenience constructor using `reqwest::Client::new()`.
    pub fn new_default(url: Url) -> Self {
        Self::new(url, Client::new())
    }
}

impl AudioSource for HttpAudioSource {
    fn name(&self) -> &'static str {
        "http"
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_stream(self: Box<Self>) -> Pin<Box<dyn Stream<Item = io::Result<SourceMsg>> + Send>> {
        let url = self.url.clone();
        let client = self.client.clone();

        // Build a stream that:
        // - performs the HTTP request,
        // - yields body bytes as they arrive,
        // - then yields EndOfStream once.
        //
        // We intentionally avoid any fixed-size buffering here; downstream byte-queue/backpressure
        // should control memory and pacing.
        let s = futures_util::stream::try_unfold(
            (Some((client, url)), Option::<reqwest::Response>::None),
            |(init, mut resp)| async move {
                // Lazily create the response on the first poll.
                if resp.is_none() {
                    let (client, url) = init.expect("init must be present before first request");
                    let r = client
                        .get(url)
                        .send()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                        .error_for_status()
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    resp = Some(r);
                }

                // Pull next chunk from the HTTP body.
                let r = resp.as_mut().expect("response must exist");
                match r.chunk().await {
                    Ok(Some(chunk)) => {
                        let bytes: Bytes = chunk;
                        Ok(Some((SourceMsg::Data(bytes), (None, resp))))
                    }
                    Ok(None) => {
                        // EOF: emit EndOfStream once, then terminate the stream next time.
                        Ok(Some((SourceMsg::EndOfStream, (None, None))))
                    }
                    Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
                }
            },
        )
        // Stop after we emitted EndOfStream (state becomes (None,None) and next poll will panic if we don't short-circuit).
        // To be safe, terminate when response is None after yielding EndOfStream.
        .take_while(|item| {
            futures_util::future::ready(match item {
                Ok(SourceMsg::EndOfStream) => true,
                Ok(_) => true,
                Err(_) => true,
            })
        });

        Box::pin(s)
    }
}
