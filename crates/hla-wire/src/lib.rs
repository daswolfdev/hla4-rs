//! Session, framing, and transport for the HLA 4 Federate Protocol.

pub mod call;
pub mod codec;
pub mod framing;
pub mod session;
pub mod transport;
pub mod ws;

pub use call::{ClientSeqState, send_hla_call, terminate_session};
pub use codec::{CodecError, MAX_PACKET_SIZE, read_frame, write_frame};
pub use framing::{
    FEDERATE_PROTOCOL_VERSION, Frame, FrameError, HEADER_SIZE, HlaCallResponsePayload,
    HlaCallbackResponsePayload, INITIAL_SEQUENCE_NUMBER, MAX_SEQUENCE_NUMBER, MessageHeader,
    MessageType, NO_SEQUENCE_NUMBER, NO_SESSION_ID, NewSessionPayload, NewSessionStatusPayload,
    NewSessionStatusReason, ResumeRequestPayload, ResumeStatusPayload, claim_next_outbound_seq,
    hla_call_request_frame, new_session_frame, resume_request_frame,
};
pub use session::{
    NewSessionAck, Session, SessionConfig, SessionError, SessionState, Transport,
    client_open_session, client_open_session_frames, server_accept_session,
    server_accept_session_frames,
};
pub use transport::{AsyncReadSource, AsyncWriteSink, FrameSink, FrameSource};
pub use ws::{WsFrameSink, WsFrameSource, split_ws};
