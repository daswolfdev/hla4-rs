//! WebSocket transport. One FedPro frame per WebSocket Binary message —
//! the WebSocket envelope already provides message framing, so each
//! [`Frame`] is encoded as a single `Message::Binary` payload.
//!
//! Per IEEE 1516.1-2025 the WebSocket transport is one of the standardized
//! FedPro options (alongside TCP and TLS).

use async_trait::async_trait;
use futures::SinkExt;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::codec::CodecError;
use crate::framing::{Frame, MessageHeader};
use crate::transport::{FrameSink, FrameSource};

/// Wrap the read half of a [`WebSocketStream`] as a [`FrameSource`].
/// Discards non-Binary frames (Ping/Pong are handled by tungstenite
/// automatically; we ignore Text and Close on the read side).
pub struct WsFrameSource<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    inner: SplitStream<WebSocketStream<S>>,
}

impl<S> WsFrameSource<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(inner: SplitStream<WebSocketStream<S>>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<S> FrameSource for WsFrameSource<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn recv_frame(&mut self) -> Result<Frame, CodecError> {
        loop {
            let msg = self
                .inner
                .next()
                .await
                .ok_or_else(|| {
                    CodecError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "websocket closed",
                    ))
                })?
                .map_err(|e| CodecError::Io(std::io::Error::other(format!("ws read: {e}"))))?;
            match msg {
                Message::Binary(bytes) => {
                    // tungstenite 0.24 hands us `bytes::Bytes` already; slice
                    // out the payload without copying.
                    let bytes: bytes::Bytes = bytes.into();
                    if bytes.len() < crate::framing::HEADER_SIZE {
                        return Err(CodecError::Frame(crate::framing::FrameError::Truncated {
                            needed: crate::framing::HEADER_SIZE,
                            had: bytes.len(),
                        }));
                    }
                    let header = MessageHeader::decode(&bytes[..crate::framing::HEADER_SIZE])?;
                    let payload = bytes.slice(crate::framing::HEADER_SIZE..);
                    return Ok(Frame { header, payload });
                }
                Message::Close(_) => {
                    return Err(CodecError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "websocket close",
                    )));
                }
                // Ping/Pong/Text/Frame: tungstenite handles control frames
                // internally for the most part; anything else is unexpected
                // here so we ignore and loop.
                _ => continue,
            }
        }
    }
}

/// Wrap the write half of a [`WebSocketStream`] as a [`FrameSink`].
pub struct WsFrameSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    inner: SplitSink<WebSocketStream<S>, Message>,
}

impl<S> WsFrameSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(inner: SplitSink<WebSocketStream<S>, Message>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<S> FrameSink for WsFrameSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn send_frame(&mut self, frame: &Frame) -> Result<(), CodecError> {
        // `Frame::encode` already returns `Bytes`; tungstenite accepts it
        // via `Into<Payload>`, so no further copy is needed.
        let bytes = frame.encode();
        self.inner
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(|e| CodecError::Io(std::io::Error::other(format!("ws write: {e}"))))?;
        Ok(())
    }
}

/// Convenience: split a WebSocketStream into matched `FrameSource` and
/// `FrameSink` halves.
pub fn split_ws<S>(ws: WebSocketStream<S>) -> (WsFrameSource<S>, WsFrameSink<S>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sink, source) = ws.split();
    (WsFrameSource::new(source), WsFrameSink::new(sink))
}
