//! FedPro wire framing — HLA 4 Federate Protocol (IEEE 1516.1-2025).
//!
//! Format derived from the FedProClient reference implementation
//! (Apache 2.0), in particular `cpp/src/fedpro/session/msg/MessageHeader.{h,cpp}`,
//! `MessageType.{h,cpp}`, and `MessageWriter.cpp`.
//!
//! ## Frame layout
//!
//! Every wire frame is `MessageHeader::SIZE` (24) bytes of header followed by a
//! payload sized `packetSize - SIZE`. All multi-byte integers are big-endian.
//!
//! ```text
//! offset  size  field
//!   0      4    packetSize           u32  (total frame size including header)
//!   4      4    sequenceNumber       i32  (i32::MIN means NO_SEQUENCE_NUMBER)
//!   8      8    sessionId            u64  (0 means NO_SESSION_ID, used by NEW_SESSION)
//!  16      4    lastReceivedSeqNum   i32
//!  20      4    messageType          u32  (enum, see `MessageType`)
//!  24    ...    payload              variable
//! ```
//!
//! ## Sentinels
//!
//! * `NO_SESSION_ID = 0`
//! * `NO_SEQUENCE_NUMBER = i32::MIN` (0x80000000)
//! * `INITIAL_SEQUENCE_NUMBER = 0`
//! * Current `FEDERATE_PROTOCOL_VERSION = 1`

use bytes::Bytes;
use thiserror::Error;

pub const HEADER_SIZE: usize = 24;
pub const NO_SESSION_ID: u64 = 0;
pub const NO_SEQUENCE_NUMBER: i32 = i32::MIN;
pub const INITIAL_SEQUENCE_NUMBER: i32 = 0;
pub const MAX_SEQUENCE_NUMBER: i32 = i32::MAX;
pub const FEDERATE_PROTOCOL_VERSION: i32 = 1;

/// Claim the next valid outbound sequence number from `atomic` and advance
/// the counter, handling wraparound.
///
/// Valid sequence numbers are `[INITIAL_SEQUENCE_NUMBER, MAX_SEQUENCE_NUMBER]`
/// i.e. `[0, i32::MAX]`. `i32::MIN` is reserved as `NO_SEQUENCE_NUMBER` and
/// must never be produced. After `MAX_SEQUENCE_NUMBER`, the counter wraps
/// back to `INITIAL_SEQUENCE_NUMBER` (per FedProClient `SequenceNumber.h`).
///
/// Returns the claimed sequence number (the *old* value of the counter, the
/// one the caller should stamp on its outbound frame).
pub fn claim_next_outbound_seq(atomic: &std::sync::atomic::AtomicI32) -> i32 {
    use std::sync::atomic::Ordering;
    let mut current = atomic.load(Ordering::Relaxed);
    loop {
        // If somehow the counter holds an invalid value (e.g., NO_SEQUENCE_NUMBER
        // because of a buggy reset), normalize the claimed value to INITIAL.
        let claimed = if current == NO_SEQUENCE_NUMBER {
            INITIAL_SEQUENCE_NUMBER
        } else {
            current
        };
        // Compute the next value to store, wrapping MAX → INITIAL and
        // skipping NO_SEQUENCE_NUMBER.
        let next = if claimed == MAX_SEQUENCE_NUMBER {
            INITIAL_SEQUENCE_NUMBER
        } else {
            claimed + 1
        };
        match atomic.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return claimed,
            Err(actual) => current = actual,
        }
    }
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("packet too small: {0} < {HEADER_SIZE}")]
    PacketTooSmall(u32),
    #[error("buffer too short: needed {needed}, had {had}")]
    Truncated { needed: usize, had: usize },
    #[error("unknown message type: {0}")]
    UnknownMessageType(u32),
    #[error("invalid NewSessionStatus reason: {0}")]
    InvalidNewSessionStatusReason(i32),
}

/// FedPro wire message types. Numeric values are stable per the standard.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum MessageType {
    CtrlNewSession = 1,
    CtrlNewSessionStatus = 2,
    CtrlHeartbeat = 3,
    CtrlHeartbeatResponse = 4,
    CtrlTerminateSession = 5,
    CtrlSessionTerminated = 6,

    CtrlResumeRequest = 10,
    CtrlResumeStatus = 11,

    HlaCallRequest = 20,
    HlaCallResponse = 21,
    HlaCallbackRequest = 22,
    HlaCallbackResponse = 23,
}

impl MessageType {
    pub fn from_u32(value: u32) -> Result<Self, FrameError> {
        Ok(match value {
            1 => Self::CtrlNewSession,
            2 => Self::CtrlNewSessionStatus,
            3 => Self::CtrlHeartbeat,
            4 => Self::CtrlHeartbeatResponse,
            5 => Self::CtrlTerminateSession,
            6 => Self::CtrlSessionTerminated,
            10 => Self::CtrlResumeRequest,
            11 => Self::CtrlResumeStatus,
            20 => Self::HlaCallRequest,
            21 => Self::HlaCallResponse,
            22 => Self::HlaCallbackRequest,
            23 => Self::HlaCallbackResponse,
            other => return Err(FrameError::UnknownMessageType(other)),
        })
    }

    pub fn is_control(self) -> bool {
        (self as u32) < (Self::HlaCallRequest as u32)
    }

    pub fn is_hla_response(self) -> bool {
        matches!(self, Self::HlaCallResponse | Self::HlaCallbackResponse)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    pub packet_size: u32,
    pub sequence_number: i32,
    pub session_id: u64,
    pub last_received_sequence_number: i32,
    pub message_type: MessageType,
}

impl MessageHeader {
    /// Build a header for a message whose payload is `payload_size` bytes.
    pub fn with_payload_size(
        payload_size: u32,
        sequence_number: i32,
        session_id: u64,
        last_received_sequence_number: i32,
        message_type: MessageType,
    ) -> Self {
        Self {
            packet_size: HEADER_SIZE as u32 + payload_size,
            sequence_number,
            session_id,
            last_received_sequence_number,
            message_type,
        }
    }

    pub fn payload_size(&self) -> u32 {
        self.packet_size - HEADER_SIZE as u32
    }

    /// Encode 24 bytes of header into `out`.
    pub fn encode(&self, out: &mut [u8; HEADER_SIZE]) {
        out[0..4].copy_from_slice(&self.packet_size.to_be_bytes());
        out[4..8].copy_from_slice(&self.sequence_number.to_be_bytes());
        out[8..16].copy_from_slice(&self.session_id.to_be_bytes());
        out[16..20].copy_from_slice(&self.last_received_sequence_number.to_be_bytes());
        out[20..24].copy_from_slice(&(self.message_type as u32).to_be_bytes());
    }

    /// Decode 24 bytes of header from `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_SIZE {
            return Err(FrameError::Truncated {
                needed: HEADER_SIZE,
                had: bytes.len(),
            });
        }
        let packet_size = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        if (packet_size as usize) < HEADER_SIZE {
            return Err(FrameError::PacketTooSmall(packet_size));
        }
        let sequence_number = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        let session_id = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let last_received_sequence_number = i32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let message_type_raw = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        let message_type = MessageType::from_u32(message_type_raw)?;
        Ok(Self {
            packet_size,
            sequence_number,
            session_id,
            last_received_sequence_number,
            message_type,
        })
    }
}

/// A complete wire frame — header plus payload bytes.
///
/// `payload` is a refcounted [`Bytes`] so fan-out can share a single
/// encoded payload across many recipients without copying. Construct
/// via `Frame::new` with anything `Into<Bytes>` (e.g. `Vec<u8>`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub header: MessageHeader,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(header: MessageHeader, payload: impl Into<Bytes>) -> Self {
        Self {
            header,
            payload: payload.into(),
        }
    }

    /// Encode the full frame (header + payload) into a single `Bytes` buffer.
    /// Prefer writing the header and payload separately when the transport
    /// allows (see `codec::write_frame`); this helper exists for transports
    /// like WebSocket that need one contiguous message per frame.
    pub fn encode(&self) -> Bytes {
        let mut buf = bytes::BytesMut::with_capacity(self.header.packet_size as usize);
        let mut header_bytes = [0u8; HEADER_SIZE];
        self.header.encode(&mut header_bytes);
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }
}

// ---------- typed message bodies ----------

/// `CTRL_NEW_SESSION` — sent by the federate on first connect.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NewSessionPayload {
    pub protocol_version: i32,
}

impl NewSessionPayload {
    pub const SIZE: usize = 4;

    pub fn current() -> Self {
        Self {
            protocol_version: FEDERATE_PROTOCOL_VERSION,
        }
    }

    pub fn encode(&self) -> [u8; 4] {
        self.protocol_version.to_be_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 4 {
            return Err(FrameError::Truncated {
                needed: 4,
                had: bytes.len(),
            });
        }
        Ok(Self {
            protocol_version: i32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        })
    }
}

/// `CTRL_RESUME_STATUS` — RTI's response to `CTRL_RESUME_REQUEST`. Uses the
/// same wire shape as `CTRL_NEW_SESSION_STATUS` for symmetry.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ResumeStatusPayload {
    pub reason: NewSessionStatusReason,
}

impl ResumeStatusPayload {
    pub const SIZE: usize = 4;
    pub fn encode(&self) -> [u8; 4] {
        self.reason.encode()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 4 {
            return Err(FrameError::Truncated {
                needed: 4,
                had: bytes.len(),
            });
        }
        let raw = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        Ok(Self {
            reason: NewSessionStatusReason::from_i32(raw)?,
        })
    }
}

/// `CTRL_NEW_SESSION_STATUS` — RTI's response to `CTRL_NEW_SESSION`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum NewSessionStatusReason {
    Success = 0,
    UnsupportedProtocolVersion = 1,
    OutOfResources = 2,
    BadMessage = 3,
    OtherError = 99,
}

impl NewSessionStatusReason {
    pub fn from_i32(v: i32) -> Result<Self, FrameError> {
        Ok(match v {
            0 => Self::Success,
            1 => Self::UnsupportedProtocolVersion,
            2 => Self::OutOfResources,
            3 => Self::BadMessage,
            99 => Self::OtherError,
            _ => return Err(FrameError::InvalidNewSessionStatusReason(v)),
        })
    }

    pub fn encode(self) -> [u8; 4] {
        (self as i32).to_be_bytes()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NewSessionStatusPayload {
    pub reason: NewSessionStatusReason,
}

impl NewSessionStatusPayload {
    pub const SIZE: usize = 4;

    pub fn encode(&self) -> [u8; 4] {
        self.reason.encode()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 4 {
            return Err(FrameError::Truncated {
                needed: 4,
                had: bytes.len(),
            });
        }
        let raw = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        Ok(Self {
            reason: NewSessionStatusReason::from_i32(raw)?,
        })
    }
}

/// `CTRL_RESUME_REQUEST` — sent by federate trying to resume a dropped session.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ResumeRequestPayload {
    pub last_received_rti_sequence_number: i32,
    pub oldest_available_federate_sequence_number: i32,
}

impl ResumeRequestPayload {
    pub const SIZE: usize = 8;

    pub fn encode(&self) -> [u8; 8] {
        let mut out = [0u8; 8];
        out[0..4].copy_from_slice(&self.last_received_rti_sequence_number.to_be_bytes());
        out[4..8].copy_from_slice(&self.oldest_available_federate_sequence_number.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 8 {
            return Err(FrameError::Truncated {
                needed: 8,
                had: bytes.len(),
            });
        }
        Ok(Self {
            last_received_rti_sequence_number: i32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            oldest_available_federate_sequence_number: i32::from_be_bytes(
                bytes[4..8].try_into().unwrap(),
            ),
        })
    }
}

/// `HLA_CALL_RESPONSE` payload — i32 responseToSequenceNumber + encoded
/// `fedpro::CallResponse` protobuf body. The `responseToSequenceNumber`
/// echoes the original `HLA_CALL_REQUEST`'s header sequence number so the
/// client can correlate responses against in-flight requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlaCallResponsePayload {
    pub response_to_sequence_number: i32,
    pub body: Vec<u8>,
}

impl HlaCallResponsePayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.body.len());
        out.extend_from_slice(&self.response_to_sequence_number.to_be_bytes());
        out.extend_from_slice(&self.body);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 4 {
            return Err(FrameError::Truncated {
                needed: 4,
                had: bytes.len(),
            });
        }
        Ok(Self {
            response_to_sequence_number: i32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            body: bytes[4..].to_vec(),
        })
    }
}

/// `HLA_CALLBACK_RESPONSE` payload — i32 responseToSequenceNumber + encoded body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlaCallbackResponsePayload {
    pub response_to_sequence_number: i32,
    pub body: Vec<u8>,
}

impl HlaCallbackResponsePayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.body.len());
        out.extend_from_slice(&self.response_to_sequence_number.to_be_bytes());
        out.extend_from_slice(&self.body);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < 4 {
            return Err(FrameError::Truncated {
                needed: 4,
                had: bytes.len(),
            });
        }
        Ok(Self {
            response_to_sequence_number: i32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            body: bytes[4..].to_vec(),
        })
    }
}

// ---------- convenience constructors ----------

/// Build a ready-to-send `CTRL_NEW_SESSION` frame.
pub fn new_session_frame() -> Frame {
    let payload = NewSessionPayload::current();
    let header = MessageHeader::with_payload_size(
        NewSessionPayload::SIZE as u32,
        INITIAL_SEQUENCE_NUMBER,
        NO_SESSION_ID,
        NO_SEQUENCE_NUMBER,
        MessageType::CtrlNewSession,
    );
    Frame::new(header, payload.encode().to_vec())
}

/// Build a `CTRL_RESUME_REQUEST` frame for an existing session.
pub fn resume_request_frame(
    session_id: u64,
    last_received_rti_sequence_number: i32,
    oldest_available_federate_sequence_number: i32,
) -> Frame {
    let payload = ResumeRequestPayload {
        last_received_rti_sequence_number,
        oldest_available_federate_sequence_number,
    };
    let header = MessageHeader::with_payload_size(
        ResumeRequestPayload::SIZE as u32,
        NO_SEQUENCE_NUMBER,
        session_id,
        last_received_rti_sequence_number,
        MessageType::CtrlResumeRequest,
    );
    Frame::new(header, payload.encode().to_vec())
}

/// Build an `HLA_CALL_REQUEST` frame carrying an already-protobuf-encoded body.
pub fn hla_call_request_frame(
    sequence_number: i32,
    session_id: u64,
    last_received_sequence_number: i32,
    protobuf_body: Vec<u8>,
) -> Frame {
    let header = MessageHeader::with_payload_size(
        protobuf_body.len() as u32,
        sequence_number,
        session_id,
        last_received_sequence_number,
        MessageType::HlaCallRequest,
    );
    Frame::new(header, protobuf_body)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `MessageType::is_control` matches the C++ helper: control = type < HLA_CALL_REQUEST.
    #[test]
    fn message_type_classification() {
        assert!(MessageType::CtrlNewSession.is_control());
        assert!(MessageType::CtrlResumeRequest.is_control());
        assert!(!MessageType::HlaCallRequest.is_control());
        assert!(MessageType::HlaCallResponse.is_hla_response());
        assert!(MessageType::HlaCallbackResponse.is_hla_response());
        assert!(!MessageType::HlaCallRequest.is_hla_response());
    }

    #[test]
    fn message_type_roundtrip() {
        for variant in [
            MessageType::CtrlNewSession,
            MessageType::CtrlNewSessionStatus,
            MessageType::CtrlHeartbeat,
            MessageType::CtrlHeartbeatResponse,
            MessageType::CtrlTerminateSession,
            MessageType::CtrlSessionTerminated,
            MessageType::CtrlResumeRequest,
            MessageType::CtrlResumeStatus,
            MessageType::HlaCallRequest,
            MessageType::HlaCallResponse,
            MessageType::HlaCallbackRequest,
            MessageType::HlaCallbackResponse,
        ] {
            assert_eq!(MessageType::from_u32(variant as u32).unwrap(), variant);
        }
        assert!(matches!(
            MessageType::from_u32(7),
            Err(FrameError::UnknownMessageType(7))
        ));
    }

    /// Header round-trip: encode → decode reproduces the same struct.
    #[test]
    fn header_roundtrip() {
        let h = MessageHeader::with_payload_size(
            42,
            17,
            0xDEAD_BEEF_CAFE_F00D,
            -1,
            MessageType::HlaCallRequest,
        );
        let mut buf = [0u8; HEADER_SIZE];
        h.encode(&mut buf);
        let decoded = MessageHeader::decode(&buf).unwrap();
        assert_eq!(decoded, h);
        assert_eq!(decoded.payload_size(), 42);
        assert_eq!(decoded.packet_size, HEADER_SIZE as u32 + 42);
    }

    /// Golden test: a `CTRL_NEW_SESSION` frame must produce a specific byte
    /// sequence on the wire. If this breaks, interop with pRTI breaks too.
    #[test]
    fn new_session_frame_golden_bytes() {
        let frame = new_session_frame();
        let bytes = frame.encode();

        // 24-byte header + 4-byte payload = 28 bytes total
        assert_eq!(bytes.len(), 28);

        #[rustfmt::skip]
        let expected: [u8; 28] = [
            // packetSize = 28
            0x00, 0x00, 0x00, 0x1C,
            // sequenceNumber = INITIAL_SEQUENCE_NUMBER = 0
            0x00, 0x00, 0x00, 0x00,
            // sessionId = NO_SESSION_ID = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // lastReceivedSequenceNumber = NO_SEQUENCE_NUMBER = i32::MIN = 0x80000000
            0x80, 0x00, 0x00, 0x00,
            // messageType = CTRL_NEW_SESSION = 1
            0x00, 0x00, 0x00, 0x01,
            // payload: protocolVersion = 1
            0x00, 0x00, 0x00, 0x01,
        ];
        assert_eq!(&bytes[..], &expected[..]);
    }

    /// `CTRL_RESUME_REQUEST` golden bytes for a session with id 0x42, last
    /// received RTI sequence 100, oldest federate sequence 50.
    #[test]
    fn resume_request_frame_golden_bytes() {
        let frame = resume_request_frame(0x42, 100, 50);
        let bytes = frame.encode();

        // 24-byte header + 8-byte payload = 32 bytes total
        assert_eq!(bytes.len(), 32);

        #[rustfmt::skip]
        let expected: [u8; 32] = [
            // packetSize = 32
            0x00, 0x00, 0x00, 0x20,
            // sequenceNumber = NO_SEQUENCE_NUMBER
            0x80, 0x00, 0x00, 0x00,
            // sessionId = 0x42
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42,
            // lastReceivedSequenceNumber = 100
            0x00, 0x00, 0x00, 0x64,
            // messageType = CTRL_RESUME_REQUEST = 10
            0x00, 0x00, 0x00, 0x0A,
            // payload: lastReceivedRtiSequenceNumber = 100, oldestAvailableFederateSequenceNumber = 50
            0x00, 0x00, 0x00, 0x64,
            0x00, 0x00, 0x00, 0x32,
        ];
        assert_eq!(&bytes[..], &expected[..]);
    }

    #[test]
    fn new_session_status_payload_roundtrip() {
        for reason in [
            NewSessionStatusReason::Success,
            NewSessionStatusReason::UnsupportedProtocolVersion,
            NewSessionStatusReason::OutOfResources,
            NewSessionStatusReason::BadMessage,
            NewSessionStatusReason::OtherError,
        ] {
            let p = NewSessionStatusPayload { reason };
            let bytes = p.encode();
            let decoded = NewSessionStatusPayload::decode(&bytes).unwrap();
            assert_eq!(decoded, p);
        }
        assert!(matches!(
            NewSessionStatusPayload::decode(&i32::to_be_bytes(7)),
            Err(FrameError::InvalidNewSessionStatusReason(7))
        ));
    }

    #[test]
    fn hla_callback_response_payload_roundtrip() {
        let p = HlaCallbackResponsePayload {
            response_to_sequence_number: 1234,
            body: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        };
        let bytes = p.encode();
        let decoded = HlaCallbackResponsePayload::decode(&bytes).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn claim_next_outbound_seq_advances_monotonically() {
        use std::sync::atomic::AtomicI32;
        let a = AtomicI32::new(0);
        assert_eq!(claim_next_outbound_seq(&a), 0);
        assert_eq!(claim_next_outbound_seq(&a), 1);
        assert_eq!(claim_next_outbound_seq(&a), 2);
    }

    #[test]
    fn claim_next_outbound_seq_wraps_at_max_to_initial() {
        use std::sync::atomic::AtomicI32;
        let a = AtomicI32::new(MAX_SEQUENCE_NUMBER);
        // Claims MAX, next stored is INITIAL (0) — never NO_SEQUENCE_NUMBER (MIN).
        assert_eq!(claim_next_outbound_seq(&a), MAX_SEQUENCE_NUMBER);
        let next = a.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(next, INITIAL_SEQUENCE_NUMBER);
        assert_ne!(next, NO_SEQUENCE_NUMBER);
        // Continues from INITIAL.
        assert_eq!(claim_next_outbound_seq(&a), INITIAL_SEQUENCE_NUMBER);
        assert_eq!(claim_next_outbound_seq(&a), 1);
    }

    #[test]
    fn claim_next_outbound_seq_normalizes_no_sequence_number() {
        use std::sync::atomic::AtomicI32;
        // Even if some buggy initializer puts NO_SEQUENCE_NUMBER in there,
        // the claim helper must normalize rather than emit it on the wire.
        let a = AtomicI32::new(NO_SEQUENCE_NUMBER);
        let claimed = claim_next_outbound_seq(&a);
        assert_ne!(claimed, NO_SEQUENCE_NUMBER);
        assert_eq!(claimed, INITIAL_SEQUENCE_NUMBER);
    }

    #[test]
    fn frame_decode_rejects_too_small_packet_size() {
        let mut bad = [0u8; HEADER_SIZE];
        // packetSize = 10, smaller than HEADER_SIZE
        bad[0..4].copy_from_slice(&10u32.to_be_bytes());
        // Set messageType to something valid so the size check is what fails.
        bad[20..24].copy_from_slice(&1u32.to_be_bytes());
        assert!(matches!(
            MessageHeader::decode(&bad),
            Err(FrameError::PacketTooSmall(10))
        ));
    }
}
