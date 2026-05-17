//! Conversion between the proto-wire `bytes`-shaped handles and the typed
//! `hla_core` newtypes.
//!
//! FedPro defines every handle as `bytes` (opaque). We choose: u32 raw → 4
//! big-endian bytes (matching how `JoinResult` already encodes
//! `FederateHandle`); `ObjectInstanceHandle` is u64 → 8 BE bytes.

use hla_core::{
    AttributeHandle, FederateHandle, InteractionClassHandle, ObjectClassHandle,
    ObjectInstanceHandle, ParameterHandle,
};
use hla_fedpro_proto::fedpro::{
    AttributeHandle as ProtoAttributeHandle, FederateHandle as ProtoFederateHandle,
    InteractionClassHandle as ProtoInteractionClassHandle,
    ObjectClassHandle as ProtoObjectClassHandle, ObjectInstanceHandle as ProtoObjectInstanceHandle,
    ParameterHandle as ProtoParameterHandle,
};

use crate::exception::HlaException;

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum HandleError {
    /// IEEE 1516.1 exception kind to surface back to the federate.
    Invalid(HlaException),
}

pub(crate) fn encode_u32(raw: u32) -> Vec<u8> {
    raw.to_be_bytes().to_vec()
}

pub(crate) fn encode_u64(raw: u64) -> Vec<u8> {
    raw.to_be_bytes().to_vec()
}

fn decode_u32(bytes: &[u8], exception: HlaException) -> Result<u32, HandleError> {
    if bytes.len() != 4 {
        return Err(HandleError::Invalid(exception));
    }
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

fn decode_u64(bytes: &[u8], exception: HlaException) -> Result<u64, HandleError> {
    if bytes.len() != 8 {
        return Err(HandleError::Invalid(exception));
    }
    Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
}

// ----- encode (typed → proto) -----

pub(crate) fn encode_object_class(h: ObjectClassHandle) -> ProtoObjectClassHandle {
    ProtoObjectClassHandle {
        data: encode_u32(h.raw()),
    }
}

pub(crate) fn encode_attribute(h: AttributeHandle) -> ProtoAttributeHandle {
    ProtoAttributeHandle {
        data: encode_u32(h.raw()),
    }
}

pub(crate) fn encode_interaction_class(h: InteractionClassHandle) -> ProtoInteractionClassHandle {
    ProtoInteractionClassHandle {
        data: encode_u32(h.raw()),
    }
}

pub(crate) fn encode_parameter(h: ParameterHandle) -> ProtoParameterHandle {
    ProtoParameterHandle {
        data: encode_u32(h.raw()),
    }
}

pub(crate) fn encode_object_instance(h: ObjectInstanceHandle) -> ProtoObjectInstanceHandle {
    ProtoObjectInstanceHandle {
        data: encode_u64(h.raw()),
    }
}

pub(crate) fn encode_federate(h: FederateHandle) -> ProtoFederateHandle {
    ProtoFederateHandle {
        data: encode_u32(h.raw()),
    }
}

// ----- decode (proto → typed) -----

pub(crate) fn decode_object_class(
    p: &ProtoObjectClassHandle,
) -> Result<ObjectClassHandle, HandleError> {
    decode_u32(&p.data, HlaException::InvalidObjectClassHandle).map(ObjectClassHandle::new)
}

pub(crate) fn decode_attribute(p: &ProtoAttributeHandle) -> Result<AttributeHandle, HandleError> {
    decode_u32(&p.data, HlaException::InvalidAttributeHandle).map(AttributeHandle::new)
}

pub(crate) fn decode_interaction_class(
    p: &ProtoInteractionClassHandle,
) -> Result<InteractionClassHandle, HandleError> {
    decode_u32(&p.data, HlaException::InvalidInteractionClassHandle)
        .map(InteractionClassHandle::new)
}

pub(crate) fn decode_parameter(p: &ProtoParameterHandle) -> Result<ParameterHandle, HandleError> {
    decode_u32(&p.data, HlaException::InvalidParameterHandle).map(ParameterHandle::new)
}

pub(crate) fn decode_object_instance(
    p: &ProtoObjectInstanceHandle,
) -> Result<ObjectInstanceHandle, HandleError> {
    decode_u64(&p.data, HlaException::InvalidObjectInstanceHandle).map(ObjectInstanceHandle::new)
}
