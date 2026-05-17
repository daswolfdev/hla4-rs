//! Async length-prefixed FedPro frame codec.
//!
//! `read_frame` and `write_frame` work over any `AsyncRead`/`AsyncWrite` —
//! a TCP socket, a TLS stream, or an `io::duplex()` pair in tests.

use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::framing::{Frame, FrameError, HEADER_SIZE, MessageHeader};

#[derive(Debug, Error)]
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

/// Read one complete FedPro frame from `reader` with a single allocation
/// and zero post-read copies.
///
/// Layout:
///   1. Read the 4-byte length prefix into a stack buffer.
///   2. Allocate one `BytesMut` sized to the full packet, write the prefix
///      into it, and `read_exact` the remainder directly into the same
///      buffer.
///   3. Decode the header in place and `split_off(HEADER_SIZE).freeze()`
///      to hand the payload to the caller as a refcounted `Bytes` slice
///      into the same allocation — no copy.
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
/// transport sees one contiguous `write_all` per frame; splitting the
/// 24-byte header into a separate write provoked a syscall-per-frame
/// stall on un-buffered TCP streams.
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
    use tokio::io::duplex;

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
        // Write only an absurd 4-byte length prefix; nothing else.
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
}
