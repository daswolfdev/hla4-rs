//! Property-based tests for the FedPro wire layer.
//!
//! These exercise the two correctness contracts that matter for production
//! deployment:
//!   1. `decode(encode(x)) == x` for every valid value of x (round-trip)
//!   2. `decode(arbitrary_bytes)` never panics — only ever returns an error
//!      (DoS / malicious-input hardening; the RTI must survive a peer that
//!      sends garbage)
//!
//! Property tests cover orders of magnitude more inputs than hand-written
//! examples. For mission-critical use these should be supplemented with
//! `cargo-fuzz` for byte-level fuzzing.

use hla_wire::{
    Frame, FrameError, HEADER_SIZE, HlaCallResponsePayload, HlaCallbackResponsePayload,
    MessageHeader, MessageType, NewSessionPayload, NewSessionStatusPayload, NewSessionStatusReason,
    ResumeRequestPayload,
};
use proptest::prelude::*;

// ----- generators -----

fn message_type_strategy() -> impl Strategy<Value = MessageType> {
    prop_oneof![
        Just(MessageType::CtrlNewSession),
        Just(MessageType::CtrlNewSessionStatus),
        Just(MessageType::CtrlHeartbeat),
        Just(MessageType::CtrlHeartbeatResponse),
        Just(MessageType::CtrlTerminateSession),
        Just(MessageType::CtrlSessionTerminated),
        Just(MessageType::CtrlResumeRequest),
        Just(MessageType::CtrlResumeStatus),
        Just(MessageType::HlaCallRequest),
        Just(MessageType::HlaCallResponse),
        Just(MessageType::HlaCallbackRequest),
        Just(MessageType::HlaCallbackResponse),
    ]
}

fn header_strategy() -> impl Strategy<Value = MessageHeader> {
    (
        // payload_size — bound so total packet doesn't overflow u32 / exceed
        // our DoS cap. Keep small enough that test runs stay fast.
        0u32..1024,
        any::<i32>(),
        any::<u64>(),
        any::<i32>(),
        message_type_strategy(),
    )
        .prop_map(|(payload_size, seq, session, last_received, kind)| {
            MessageHeader::with_payload_size(payload_size, seq, session, last_received, kind)
        })
}

fn status_reason_strategy() -> impl Strategy<Value = NewSessionStatusReason> {
    prop_oneof![
        Just(NewSessionStatusReason::Success),
        Just(NewSessionStatusReason::UnsupportedProtocolVersion),
        Just(NewSessionStatusReason::OutOfResources),
        Just(NewSessionStatusReason::BadMessage),
        Just(NewSessionStatusReason::OtherError),
    ]
}

// ----- round-trip property tests -----

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn header_roundtrip(h in header_strategy()) {
        let mut buf = [0u8; HEADER_SIZE];
        h.encode(&mut buf);
        let decoded = MessageHeader::decode(&buf).expect("decode");
        prop_assert_eq!(decoded, h);
    }

    #[test]
    fn frame_roundtrip(h in header_strategy(), payload in prop::collection::vec(any::<u8>(), 0..1024)) {
        let mut h = h;
        h.packet_size = HEADER_SIZE as u32 + payload.len() as u32;
        let frame = Frame::new(h, payload.clone());
        let bytes = frame.encode();
        prop_assert_eq!(bytes.len() as u32, h.packet_size);
        // Re-parse the header from the bytes.
        let decoded_header = MessageHeader::decode(&bytes[..HEADER_SIZE]).expect("header");
        prop_assert_eq!(decoded_header, h);
        prop_assert_eq!(&bytes[HEADER_SIZE..], &payload[..]);
    }

    #[test]
    fn new_session_payload_roundtrip(v in any::<i32>()) {
        let p = NewSessionPayload { protocol_version: v };
        let bytes = p.encode();
        let decoded = NewSessionPayload::decode(&bytes).unwrap();
        prop_assert_eq!(decoded, p);
    }

    #[test]
    fn new_session_status_payload_roundtrip(r in status_reason_strategy()) {
        let p = NewSessionStatusPayload { reason: r };
        let bytes = p.encode();
        let decoded = NewSessionStatusPayload::decode(&bytes).unwrap();
        prop_assert_eq!(decoded, p);
    }

    #[test]
    fn resume_request_roundtrip(a in any::<i32>(), b in any::<i32>()) {
        let p = ResumeRequestPayload {
            last_received_rti_sequence_number: a,
            oldest_available_federate_sequence_number: b,
        };
        let bytes = p.encode();
        let decoded = ResumeRequestPayload::decode(&bytes).unwrap();
        prop_assert_eq!(decoded, p);
    }

    #[test]
    fn hla_call_response_payload_roundtrip(
        seq in any::<i32>(),
        body in prop::collection::vec(any::<u8>(), 0..2048)
    ) {
        let p = HlaCallResponsePayload {
            response_to_sequence_number: seq,
            body,
        };
        let bytes = p.encode();
        let decoded = HlaCallResponsePayload::decode(&bytes).unwrap();
        prop_assert_eq!(decoded, p);
    }

    #[test]
    fn hla_callback_response_payload_roundtrip(
        seq in any::<i32>(),
        body in prop::collection::vec(any::<u8>(), 0..2048)
    ) {
        let p = HlaCallbackResponsePayload {
            response_to_sequence_number: seq,
            body,
        };
        let bytes = p.encode();
        let decoded = HlaCallbackResponsePayload::decode(&bytes).unwrap();
        prop_assert_eq!(decoded, p);
    }
}

// ----- adversarial-input fuzz property tests -----

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    /// Decoding random bytes as a `MessageHeader` must never panic. Either
    /// returns `Ok` (if the bytes happen to be well-formed) or `Err`.
    #[test]
    fn header_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..128)) {
        let result = MessageHeader::decode(&bytes);
        // Either succeeds or returns a structured error; never panics.
        match result {
            Ok(_) | Err(FrameError::Truncated { .. })
            | Err(FrameError::PacketTooSmall(_))
            | Err(FrameError::UnknownMessageType(_))
            | Err(FrameError::InvalidNewSessionStatusReason(_)) => {}
        }
    }

    /// Same for all body decoders.
    #[test]
    fn new_session_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..16)) {
        let _ = NewSessionPayload::decode(&bytes);
    }

    #[test]
    fn new_session_status_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..16)) {
        let _ = NewSessionStatusPayload::decode(&bytes);
    }

    #[test]
    fn resume_request_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..32)) {
        let _ = ResumeRequestPayload::decode(&bytes);
    }

    #[test]
    fn hla_call_response_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = HlaCallResponsePayload::decode(&bytes);
    }

    #[test]
    fn hla_callback_response_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = HlaCallbackResponsePayload::decode(&bytes);
    }

    /// MessageType::from_u32 must classify any u32 as either a known variant
    /// or `UnknownMessageType`. Never panics.
    #[test]
    fn message_type_classification_never_panics(v in any::<u32>()) {
        match MessageType::from_u32(v) {
            Ok(_) | Err(FrameError::UnknownMessageType(_)) => {}
            Err(other) => prop_assert!(false, "unexpected error variant: {other:?}"),
        }
    }
}
