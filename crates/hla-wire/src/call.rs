//! Client-side single-request HLA call helper.
//!
//! MVP semantics: serialize one request at a time. A real federate runtime
//! will need a pending-responses map keyed by sequence number and a
//! background pump task; here we keep it deliberately small so the
//! end-to-end create/destroy/list flow can be exercised in a test.

use tokio::io::{AsyncRead, AsyncWrite};

use crate::codec::{CodecError, read_frame, write_frame};
use crate::framing::{
    Frame, HlaCallResponsePayload, MessageHeader, MessageType, NO_SESSION_ID,
    hla_call_request_frame,
};
use crate::session::SessionError;

/// State a client must carry across calls: which session is this, what's the
/// next outbound sequence number, and what's the most recent inbound seq# we
/// should echo as `lastReceivedSequenceNumber`.
#[derive(Debug)]
pub struct ClientSeqState {
    pub session_id: u64,
    pub next_outbound: i32,
    pub last_received: i32,
}

impl ClientSeqState {
    pub fn new(session_id: u64) -> Self {
        // First HLA call uses seq 1 (NEW_SESSION used seq 0).
        Self {
            session_id,
            next_outbound: 1,
            last_received: 0,
        }
    }
}

/// Send an already-protobuf-encoded `CallRequest` body and await the matching
/// `HLA_CALL_RESPONSE`. Returns the protobuf body of the response (caller
/// decodes it as `fedpro::CallResponse`).
///
/// Errors if the next frame is not an `HLA_CALL_RESPONSE` or its
/// `responseToSequenceNumber` doesn't match the request we just sent.
pub async fn send_hla_call<S>(
    stream: &mut S,
    state: &mut ClientSeqState,
    request_body: Vec<u8>,
) -> Result<Vec<u8>, SessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request_seq = state.next_outbound;
    state.next_outbound = state.next_outbound.wrapping_add(1);

    let frame = hla_call_request_frame(
        request_seq,
        state.session_id,
        state.last_received,
        request_body,
    );
    write_frame(stream, &frame).await?;

    let response = read_frame(stream).await?;
    state.last_received = response.header.sequence_number;

    if response.header.message_type != MessageType::HlaCallResponse {
        return Err(SessionError::UnexpectedMessageType {
            got: response.header.message_type,
            expected: MessageType::HlaCallResponse,
        });
    }

    let payload = HlaCallResponsePayload::decode(&response.payload)
        .map_err(|e| SessionError::Codec(CodecError::Frame(e)))?;
    if payload.response_to_sequence_number != request_seq {
        return Err(SessionError::Codec(CodecError::Frame(
            crate::framing::FrameError::UnknownMessageType(0),
        )));
        // Note: real impl will queue out-of-order responses by seq# in a map.
    }
    Ok(payload.body)
}

/// Send a `CTRL_TERMINATE_SESSION` frame and read the server's
/// `CTRL_SESSION_TERMINATED` ack.
pub async fn terminate_session<S>(
    stream: &mut S,
    state: &mut ClientSeqState,
) -> Result<(), SessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let header = MessageHeader::with_payload_size(
        0,
        state.next_outbound,
        state.session_id,
        state.last_received,
        MessageType::CtrlTerminateSession,
    );
    state.next_outbound = state.next_outbound.wrapping_add(1);
    write_frame(stream, &Frame::new(header, Vec::new())).await?;

    let response = read_frame(stream).await?;
    if response.header.message_type != MessageType::CtrlSessionTerminated {
        return Err(SessionError::UnexpectedMessageType {
            got: response.header.message_type,
            expected: MessageType::CtrlSessionTerminated,
        });
    }
    let _ = NO_SESSION_ID; // keep import used
    Ok(())
}
