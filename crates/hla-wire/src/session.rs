//! Federate Protocol session lifecycle.
//!
//! Cadences mirror FedProClient `doc/Components.md`:
//!   * heartbeat every 60s
//!   * RTI missing → drop at 180s
//!   * resume window 600s
//!
//! Wire-side framing constants live in [`crate::framing`]; this module owns
//! the session-level state machine and the client/server handshake helpers.

use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::codec::{CodecError, read_frame, write_frame};
use crate::framing::{
    FEDERATE_PROTOCOL_VERSION, MessageType, NewSessionPayload, NewSessionStatusPayload,
    NewSessionStatusReason, new_session_frame,
};
use crate::transport::{FrameSink, FrameSource};

pub const DEFAULT_PORT: u16 = 15164;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
pub const RTI_MISSING_TIMEOUT: Duration = Duration::from_secs(180);
pub const RECONNECT_WINDOW: Duration = Duration::from_secs(600);

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("session not running (state = {0:?})")]
    NotRunning(SessionState),
    #[error("RTI timed out")]
    RtiTimeout,
    #[error("RTI dropped session")]
    Dropped,
    #[error("unexpected message type: got {got:?}, expected {expected:?}")]
    UnexpectedMessageType {
        got: MessageType,
        expected: MessageType,
    },
    #[error(
        "HLA_CALL_RESPONSE sequence mismatch: expected response to request {expected}, got response to {got}"
    )]
    ResponseMismatch { expected: i32, got: i32 },
    #[error("RTI rejected session: {0:?}")]
    SessionRejected(NewSessionStatusReason),
    #[error("unsupported federate protocol version: {0}")]
    UnsupportedProtocolVersion(i32),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionState {
    New,
    Starting,
    Running,
    Terminating,
    Terminated,
    Dropped,
    Resuming,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Transport {
    Tcp,
    Tls,
    WebSocket,
    WebSocketSecure,
}

#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub transport: Transport,
    pub host: String,
    pub port: u16,
    pub heartbeat: Duration,
    pub rti_missing_timeout: Duration,
    pub reconnect_window: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            transport: Transport::Tcp,
            host: "localhost".into(),
            port: DEFAULT_PORT,
            heartbeat: HEARTBEAT_INTERVAL,
            rti_missing_timeout: RTI_MISSING_TIMEOUT,
            reconnect_window: RECONNECT_WINDOW,
        }
    }
}

/// Federate-side session handle.
pub struct Session {
    state: SessionState,
    session_id: u64,
    config: SessionConfig,
}

impl Session {
    pub async fn open(config: SessionConfig) -> Result<Self, SessionError> {
        Ok(Self {
            state: SessionState::New,
            session_id: 0,
            config,
        })
    }

    pub fn state(&self) -> SessionState {
        self.state
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    pub async fn close(self) -> Result<(), SessionError> {
        Ok(())
    }
}

/// Result of a successful client-side `CTRL_NEW_SESSION` handshake.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NewSessionAck {
    pub session_id: u64,
    pub state: SessionState,
}

/// Perform the federate-side `CTRL_NEW_SESSION` / `CTRL_NEW_SESSION_STATUS`
/// exchange over an already-connected stream (TCP, TLS, in-memory duplex).
///
/// On success, the session has transitioned to `Running` and the caller holds
/// the RTI-assigned `session_id` for use in subsequent message headers.
pub async fn client_open_session<S>(stream: &mut S) -> Result<NewSessionAck, SessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_frame(stream, &new_session_frame()).await?;
    let response = read_frame(stream).await?;
    parse_new_session_status(response)
}

fn parse_new_session_status(
    response: crate::framing::Frame,
) -> Result<NewSessionAck, SessionError> {
    if response.header.message_type != MessageType::CtrlNewSessionStatus {
        return Err(SessionError::UnexpectedMessageType {
            got: response.header.message_type,
            expected: MessageType::CtrlNewSessionStatus,
        });
    }
    let status = NewSessionStatusPayload::decode(&response.payload)
        .map_err(|e| SessionError::Codec(CodecError::Frame(e)))?;
    if status.reason != NewSessionStatusReason::Success {
        return Err(SessionError::SessionRejected(status.reason));
    }
    Ok(NewSessionAck {
        session_id: response.header.session_id,
        state: SessionState::Running,
    })
}

/// `FrameSource`/`FrameSink` variant of `client_open_session` — works
/// over any transport (TCP/TLS bytes, WebSocket messages, anything that
/// implements the trait pair).
pub async fn client_open_session_frames<R, W>(
    source: &mut R,
    sink: &mut W,
) -> Result<NewSessionAck, SessionError>
where
    R: FrameSource + ?Sized,
    W: FrameSink + ?Sized,
{
    sink.send_frame(&new_session_frame()).await?;
    let response = source.recv_frame().await?;
    parse_new_session_status(response)
}

/// `FrameSource`/`FrameSink` variant of `server_accept_session`.
pub async fn server_accept_session_frames<R, W>(
    source: &mut R,
    sink: &mut W,
    assigned_session_id: u64,
) -> Result<(), SessionError>
where
    R: FrameSource + ?Sized,
    W: FrameSink + ?Sized,
{
    use crate::framing::{Frame, MessageHeader, NO_SEQUENCE_NUMBER};

    let request = source.recv_frame().await?;
    if request.header.message_type != MessageType::CtrlNewSession {
        return Err(SessionError::UnexpectedMessageType {
            got: request.header.message_type,
            expected: MessageType::CtrlNewSession,
        });
    }
    let payload = NewSessionPayload::decode(&request.payload)
        .map_err(|e| SessionError::Codec(CodecError::Frame(e)))?;
    let (reason, result) = if payload.protocol_version == FEDERATE_PROTOCOL_VERSION {
        (NewSessionStatusReason::Success, Ok(()))
    } else {
        (
            NewSessionStatusReason::UnsupportedProtocolVersion,
            Err(SessionError::UnsupportedProtocolVersion(
                payload.protocol_version,
            )),
        )
    };
    let status_payload = NewSessionStatusPayload { reason };
    let header = MessageHeader::with_payload_size(
        NewSessionStatusPayload::SIZE as u32,
        NO_SEQUENCE_NUMBER,
        assigned_session_id,
        request.header.sequence_number,
        MessageType::CtrlNewSessionStatus,
    );
    sink.send_frame(&Frame::new(header, status_payload.encode().to_vec()))
        .await?;
    result
}

/// Server-side handler for an incoming `CTRL_NEW_SESSION`.
///
/// Reads the federate's `CTRL_NEW_SESSION`, validates protocol version, and
/// writes the appropriate `CTRL_NEW_SESSION_STATUS`. On version mismatch the
/// status carries `UnsupportedProtocolVersion` and the function returns
/// `Err(UnsupportedProtocolVersion)`.
pub async fn server_accept_session<S>(
    stream: &mut S,
    assigned_session_id: u64,
) -> Result<(), SessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::framing::{Frame, MessageHeader, NO_SEQUENCE_NUMBER};

    let request = read_frame(stream).await?;
    if request.header.message_type != MessageType::CtrlNewSession {
        return Err(SessionError::UnexpectedMessageType {
            got: request.header.message_type,
            expected: MessageType::CtrlNewSession,
        });
    }
    let payload = NewSessionPayload::decode(&request.payload)
        .map_err(|e| SessionError::Codec(CodecError::Frame(e)))?;

    let (reason, result) = if payload.protocol_version == FEDERATE_PROTOCOL_VERSION {
        (NewSessionStatusReason::Success, Ok(()))
    } else {
        (
            NewSessionStatusReason::UnsupportedProtocolVersion,
            Err(SessionError::UnsupportedProtocolVersion(
                payload.protocol_version,
            )),
        )
    };

    // The server's session-status reply uses NO_SEQUENCE_NUMBER (it's a
    // control acknowledgement) and echoes back the client's seq in the
    // lastReceivedSequenceNumber field so the client can confirm receipt.
    let status_payload = NewSessionStatusPayload { reason };
    let header = MessageHeader::with_payload_size(
        NewSessionStatusPayload::SIZE as u32,
        NO_SEQUENCE_NUMBER,
        assigned_session_id,
        request.header.sequence_number,
        MessageType::CtrlNewSessionStatus,
    );
    let response_frame = Frame::new(header, status_payload.encode().to_vec());
    write_frame(stream, &response_frame).await?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{
        Frame, MessageHeader, NO_SEQUENCE_NUMBER, NO_SESSION_ID, NewSessionStatusPayload,
        NewSessionStatusReason,
    };
    use tokio::io::duplex;

    /// End-to-end handshake over an in-memory duplex pipe. Mirrors what
    /// `RtiNode` and `client_open_session` will do over a real TCP socket.
    #[tokio::test]
    async fn duplex_handshake_succeeds() {
        let (mut client, mut server) = duplex(1024);
        let server_task = tokio::spawn(async move {
            server_accept_session(&mut server, 0xABCD).await.unwrap();
        });
        let ack = client_open_session(&mut client).await.unwrap();
        server_task.await.unwrap();
        assert_eq!(ack.session_id, 0xABCD);
        assert_eq!(ack.state, SessionState::Running);
    }

    /// Server detects a wrong protocol version and signals it back. Client
    /// sees `SessionRejected(UnsupportedProtocolVersion)`.
    #[tokio::test]
    async fn handshake_rejects_wrong_protocol_version() {
        let (mut client, mut server) = duplex(1024);
        let server_task = tokio::spawn(async move {
            let err = server_accept_session(&mut server, 1).await.unwrap_err();
            assert!(matches!(err, SessionError::UnsupportedProtocolVersion(999)));
        });

        // Hand-craft a NEW_SESSION frame with a bogus protocol version.
        let header = MessageHeader::with_payload_size(
            NewSessionPayload::SIZE as u32,
            0,
            NO_SESSION_ID,
            NO_SEQUENCE_NUMBER,
            MessageType::CtrlNewSession,
        );
        let frame = Frame::new(header, 999i32.to_be_bytes().to_vec());
        write_frame(&mut client, &frame).await.unwrap();

        // Read back the rejection.
        let response = read_frame(&mut client).await.unwrap();
        server_task.await.unwrap();
        assert_eq!(
            response.header.message_type,
            MessageType::CtrlNewSessionStatus
        );
        let status = NewSessionStatusPayload::decode(&response.payload).unwrap();
        assert_eq!(
            status.reason,
            NewSessionStatusReason::UnsupportedProtocolVersion
        );
    }

    /// Server gets a non-NEW_SESSION first frame and bails.
    #[tokio::test]
    async fn handshake_rejects_unexpected_first_message() {
        let (mut client, mut server) = duplex(1024);
        let server_task = tokio::spawn(async move {
            let err = server_accept_session(&mut server, 1).await.unwrap_err();
            assert!(matches!(err, SessionError::UnexpectedMessageType { .. }));
        });
        let frame = crate::framing::resume_request_frame(1, 0, 0);
        write_frame(&mut client, &frame).await.unwrap();
        server_task.await.unwrap();
    }
}
