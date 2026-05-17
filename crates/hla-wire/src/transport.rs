//! Frame-level transport abstraction.
//!
//! Decouples the FedPro session logic from the underlying byte stream so
//! TCP, TLS, WebSocket, and (in the future) Unix-domain sockets all flow
//! through one code path.
//!
//! The trait split (separate `FrameSink` and `FrameSource`) mirrors the
//! split halves of a duplex stream: a writer task owns a `FrameSink`, a
//! reader task owns a `FrameSource`. Both are object-safe — dispatch lives
//! behind `Box<dyn FrameSink>` / `Box<dyn FrameSource>` so the same code
//! handles all transports without monomorphization explosion.

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::codec::{CodecError, read_frame, write_frame};
use crate::framing::Frame;

#[async_trait]
pub trait FrameSource: Send {
    /// Read the next complete frame from the underlying transport.
    /// Returns `CodecError::Io(UnexpectedEof)` when the peer closes cleanly.
    async fn recv_frame(&mut self) -> Result<Frame, CodecError>;
}

#[async_trait]
pub trait FrameSink: Send {
    /// Send `frame` and flush the underlying transport.
    async fn send_frame(&mut self, frame: &Frame) -> Result<(), CodecError>;
}

/// Adapter over a byte-stream read half (TCP / TLS / pipe).
pub struct AsyncReadSource<R: AsyncRead + Unpin + Send>(pub R);

#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameSource for AsyncReadSource<R> {
    async fn recv_frame(&mut self) -> Result<Frame, CodecError> {
        read_frame(&mut self.0).await
    }
}

/// Adapter over a byte-stream write half (TCP / TLS / pipe).
pub struct AsyncWriteSink<W: AsyncWrite + Unpin + Send>(pub W);

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> FrameSink for AsyncWriteSink<W> {
    async fn send_frame(&mut self, frame: &Frame) -> Result<(), CodecError> {
        write_frame(&mut self.0, frame).await
    }
}
