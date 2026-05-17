//! Async length-prefixed FedPro frame codec.
//!
//! Two layers:
//!
//! 1. [`FedProCodec`] — a stateful [`tokio_util::codec::Decoder`] /
//!    [`Encoder`] implementation. This is the cancel-safe primitive: a
//!    [`tokio_util::codec::Framed`] wrapping a byte stream lets the
//!    server's main `select!` loop poll the next frame without losing
//!    bytes if a competing branch fires mid-read.
//! 2. [`read_frame`] / [`write_frame`] — convenience helpers used by
//!    the synchronous client handshake helpers and the test suite.
//!    These are *not* cancel-safe; do not drive them from inside a
//!    `tokio::select!` with sibling branches that may fire.

use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};

use crate::framing::{Frame, FrameError, HEADER_SIZE, MessageHeader};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("packet too large: {0} bytes (max {max})", max = MAX_PACKET_SIZE)]
    PacketTooLarge(u32),
}

/// 32 MiB cap on a single FedPro packet. Mostly a DoS guard against bad
/// peers; real HLA payloads are typically tens of KB.
pub const MAX_PACKET_SIZE: u32 = 32 * 1024 * 1024;

/// Cancel-safe length-prefixed FedPro frame codec, intended to be paired with
/// [`tokio_util::codec::Framed`], [`FramedRead`], or [`FramedWrite`].
///
/// Read protocol: peek the 4-byte length prefix; reserve enough room in the
/// buffer for the rest of the packet; once the buffer holds the full packet,
/// decode the header in place and hand the payload to the caller as a
/// refcounted [`bytes::Bytes`] slice into the same allocation — no copy.
///
/// [`FramedRead`]: tokio_util::codec::FramedRead
/// [`FramedWrite`]: tokio_util::codec::FramedWrite
#[derive(Default, Debug, Clone)]
pub struct FedProCodec;

impl Decoder for FedProCodec {
    type Item = Frame;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, CodecError> {
        if src.len() < 4 {
            // Need the length prefix before we know how much to wait for.
            src.reserve(4 - src.len());
            return Ok(None);
        }

        let mut size_buf = [0u8; 4];
        size_buf.copy_from_slice(&src[..4]);
        let packet_size = u32::from_be_bytes(size_buf);

        // Any framing error terminates the stream: `FramedRead` marks the
        // codec as errored on `Err` and returns `None` thereafter, which
        // `AsyncReadSource::recv_frame` translates into `UnexpectedEof`.
        // Connection-tear-down on the first protocol error is the right
        // policy for an HLA RTI handling untrusted peers; there is no
        // recovery path that could safely re-frame.
        if (packet_size as usize) < HEADER_SIZE {
            return Err(CodecError::Frame(FrameError::PacketTooSmall(packet_size)));
        }
        if packet_size > MAX_PACKET_SIZE {
            return Err(CodecError::PacketTooLarge(packet_size));
        }

        let packet_size = packet_size as usize;
        if src.len() < packet_size {
            // Hint capacity so the next read can populate the rest in one shot.
            src.reserve(packet_size - src.len());
            return Ok(None);
        }

        // Take ownership of exactly `packet_size` bytes; the rest stays in `src`
        // for the next call. `split_to` returns a `BytesMut`, the suffix becomes
        // immutable `Bytes` via `.freeze()`.
        let mut packet = src.split_to(packet_size);
        let header = MessageHeader::decode(&packet[..HEADER_SIZE])?;
        let payload = packet.split_off(HEADER_SIZE).freeze();
        Ok(Some(Frame { header, payload }))
    }
}

impl Encoder<&Frame> for FedProCodec {
    type Error = CodecError;

    fn encode(&mut self, frame: &Frame, dst: &mut BytesMut) -> Result<(), CodecError> {
        dst.reserve(frame.header.packet_size as usize);
        let mut header_bytes = [0u8; HEADER_SIZE];
        frame.header.encode(&mut header_bytes);
        dst.extend_from_slice(&header_bytes);
        dst.extend_from_slice(&frame.payload);
        Ok(())
    }
}

/// Read one complete FedPro frame from `reader`.
///
/// **Not cancel-safe.** Dropping the returned future mid-await may
/// consume bytes from the stream without producing a frame, leaving the
/// transport's framing permanently desynchronized. Callers driving this
/// from inside a `tokio::select!` with other branches must use
/// [`FedProCodec`] via [`tokio_util::codec::FramedRead`] instead — that
/// path keeps a stable buffer across polls.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame, CodecError> {
    let mut size_buf = [0u8; 4];
    reader.read_exact(&mut size_buf).await?;
    let packet_size = u32::from_be_bytes(size_buf);

    if (packet_size as usize) < HEADER_SIZE {
        return Err(CodecError::Frame(FrameError::PacketTooSmall(packet_size)));
    }
    if packet_size > MAX_PACKET_SIZE {
        return Err(CodecError::PacketTooLarge(packet_size));
    }

    let packet_size = packet_size as usize;
    let mut buf = BytesMut::zeroed(packet_size);
    buf[0..4].copy_from_slice(&size_buf);
    reader.read_exact(&mut buf[4..]).await?;

    let header = MessageHeader::decode(&buf[..HEADER_SIZE])?;
    let payload = buf.split_off(HEADER_SIZE).freeze();
    Ok(Frame { header, payload })
}

/// Write `frame` to `writer` and flush.
///
/// Header and payload are concatenated into a single `BytesMut` so the
/// transport sees one contiguous `write_all` per frame.
///
/// **Not cancel-safe.** Drop-then-retry is a recipe for partial writes.
/// Driver tasks should call this strictly serially, never inside a
/// `select!` with a competing branch.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
) -> Result<(), CodecError> {
    let mut buf = BytesMut::with_capacity(frame.header.packet_size as usize);
    let mut header_bytes = [0u8; HEADER_SIZE];
    frame.header.encode(&mut header_bytes);
    buf.extend_from_slice(&header_bytes);
    buf.extend_from_slice(&frame.payload);
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{
        MessageType, hla_call_request_frame, new_session_frame, resume_request_frame,
    };
    use futures::{SinkExt, StreamExt};
    use tokio::io::duplex;
    use tokio_util::codec::{FramedRead, FramedWrite};

    #[tokio::test]
    async fn roundtrip_new_session_frame() {
        let (mut client, mut server) = duplex(1024);
        let sent = new_session_frame();
        let sent_clone = sent.clone();
        let writer = tokio::spawn(async move {
            write_frame(&mut client, &sent_clone).await.unwrap();
        });
        let got = read_frame(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got, sent);
    }

    #[tokio::test]
    async fn roundtrip_resume_request_frame() {
        let (mut client, mut server) = duplex(1024);
        let sent = resume_request_frame(0xCAFE_BABE, 42, 7);
        let sent_clone = sent.clone();
        let writer = tokio::spawn(async move {
            write_frame(&mut client, &sent_clone).await.unwrap();
        });
        let got = read_frame(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got, sent);
        assert_eq!(got.header.message_type, MessageType::CtrlResumeRequest);
    }

    #[tokio::test]
    async fn roundtrip_large_hla_call_request() {
        let (mut client, mut server) = duplex(64 * 1024);
        let body = vec![0xAB; 10_000];
        let sent = hla_call_request_frame(1, 0xDEAD_BEEF, 0, body);
        let sent_clone = sent.clone();
        let writer = tokio::spawn(async move {
            write_frame(&mut client, &sent_clone).await.unwrap();
        });
        let got = read_frame(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got, sent);
        assert_eq!(got.payload.len(), 10_000);
    }

    #[tokio::test]
    async fn multiple_frames_streamed_back_to_back() {
        let (mut client, mut server) = duplex(8 * 1024);
        let f1 = new_session_frame();
        let f2 = resume_request_frame(1, 10, 5);
        let f3 = hla_call_request_frame(2, 1, 10, vec![1, 2, 3, 4]);
        let frames = vec![f1.clone(), f2.clone(), f3.clone()];
        let frames_clone = frames.clone();
        let writer = tokio::spawn(async move {
            for f in &frames_clone {
                write_frame(&mut client, f).await.unwrap();
            }
        });
        let r1 = read_frame(&mut server).await.unwrap();
        let r2 = read_frame(&mut server).await.unwrap();
        let r3 = read_frame(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(r1, f1);
        assert_eq!(r2, f2);
        assert_eq!(r3, f3);
    }

    #[tokio::test]
    async fn rejects_oversized_packet_without_consuming_payload() {
        let (mut client, mut server) = duplex(64);
        let bogus = (MAX_PACKET_SIZE + 1).to_be_bytes();
        let writer = tokio::spawn(async move {
            tokio::io::AsyncWriteExt::write_all(&mut client, &bogus)
                .await
                .unwrap();
        });
        let err = read_frame(&mut server).await.unwrap_err();
        writer.await.unwrap();
        assert!(matches!(err, CodecError::PacketTooLarge(_)));
    }

    /// `FedProCodec` round-trips through `FramedRead`/`FramedWrite` over an
    /// in-memory duplex. This is the cancel-safe path that the RTI server
    /// uses inside its `select!` accept loop.
    #[tokio::test]
    async fn framed_roundtrip_three_frames() {
        let (client, server) = duplex(8 * 1024);
        let mut writer = FramedWrite::new(client, FedProCodec);
        let mut reader = FramedRead::new(server, FedProCodec);

        let f1 = new_session_frame();
        let f2 = resume_request_frame(0x11, 1, 0);
        let f3 = hla_call_request_frame(2, 0x11, 1, vec![0xDE, 0xAD, 0xBE, 0xEF]);

        let send = tokio::spawn({
            let f1 = f1.clone();
            let f2 = f2.clone();
            let f3 = f3.clone();
            async move {
                writer.send(&f1).await.unwrap();
                writer.send(&f2).await.unwrap();
                writer.send(&f3).await.unwrap();
            }
        });
        let r1 = reader.next().await.unwrap().unwrap();
        let r2 = reader.next().await.unwrap().unwrap();
        let r3 = reader.next().await.unwrap().unwrap();
        send.await.unwrap();
        assert_eq!(r1, f1);
        assert_eq!(r2, f2);
        assert_eq!(r3, f3);
    }
}
