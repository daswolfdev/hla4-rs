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
//!
//! ## Cancel-safety
//!
//! [`FrameSource::recv_frame`] **is** cancel-safe for the byte-stream
//! [`AsyncReadSource`] impl: it is backed by
//! [`tokio_util::codec::FramedRead`], whose internal `BytesMut` survives
//! across drops of the polling future. This is the contract the
//! `RtiNode` accept loop's `tokio::select!` relies on.
//!
//! [`FrameSink::send_frame`] is **NOT** cancel-safe. `FramedWrite` may
//! buffer bytes from the dropped frame in its internal `BytesMut`; the
//! next `send_frame` call will flush them along with the new frame, so
//! as long as access stays serial nothing is corrupted — but concurrent
//! senders or `select!`-driven cancellation can interleave frame data
//! and desync the receiver. Drive `send_frame` strictly serially from a
//! dedicated writer task.

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::codec::{CodecError, FedProCodec};
use crate::framing::Frame;

#[async_trait]
pub trait FrameSource: Send {
    /// Read the next complete frame from the underlying transport.
    /// Returns `CodecError::Io(UnexpectedEof)` when the peer closes cleanly.
    ///
    /// Implementations backed by a byte-stream `AsyncRead` (see
    /// [`AsyncReadSource`]) are cancel-safe: dropping the future before
    /// it resolves does not consume bytes from the transport.
    async fn recv_frame(&mut self) -> Result<Frame, CodecError>;
}

#[async_trait]
pub trait FrameSink: Send {
    /// Send `frame` and flush the underlying transport.
    ///
    /// **NOT cancel-safe** in general. Implementations are permitted to
    /// hold partial-write state across `.await` points; the next call
    /// may pick up leftover bytes from a dropped previous call. Drive
    /// from a dedicated writer task, never `select!`.
    async fn send_frame(&mut self, frame: &Frame) -> Result<(), CodecError>;
}

/// Cancel-safe `FrameSource` over an `AsyncRead` byte stream.
///
/// Wraps the reader in a [`tokio_util::codec::FramedRead`] using
/// [`FedProCodec`]; the codec's internal `BytesMut` survives across drops
/// of the polling future, so [`Self::recv_frame`] is cancel-safe and
/// safe to use inside a `tokio::select!` branch.
pub struct AsyncReadSource<R: AsyncRead + Unpin + Send> {
    framed: FramedRead<R, FedProCodec>,
}

impl<R: AsyncRead + Unpin + Send> AsyncReadSource<R> {
    pub fn new(reader: R) -> Self {
        Self {
            framed: FramedRead::new(reader, FedProCodec),
        }
    }
}

#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameSource for AsyncReadSource<R> {
    async fn recv_frame(&mut self) -> Result<Frame, CodecError> {
        match self.framed.next().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(e)) => Err(e),
            None => Err(CodecError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "transport closed",
            ))),
        }
    }
}

/// `FrameSink` over an `AsyncWrite` byte stream.
///
/// Internally buffers via [`tokio_util::codec::FramedWrite`] and flushes
/// each frame. **Not cancel-safe** — see the trait-level documentation
/// on [`FrameSink::send_frame`].
pub struct AsyncWriteSink<W: AsyncWrite + Unpin + Send> {
    framed: FramedWrite<W, FedProCodec>,
}

impl<W: AsyncWrite + Unpin + Send> AsyncWriteSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            framed: FramedWrite::new(writer, FedProCodec),
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> FrameSink for AsyncWriteSink<W> {
    async fn send_frame(&mut self, frame: &Frame) -> Result<(), CodecError> {
        // `Sink::send` calls `start_send` + `flush`. The flush here is
        // important: `FramedWrite` buffers internally and would otherwise
        // hold writes until the next call.
        self.framed.send(frame).await?;
        Ok(())
    }
}
