//! Conversion helpers between `hla_core` typed handles and the protobuf
//! `bytes`-shaped handles. Mirrors `hla_rti::handles` so federate code can
//! work with strongly-typed handles.

use hla_core::{
    AttributeHandle, FederateHandle, InteractionClassHandle, ObjectClassHandle,
    ObjectInstanceHandle, ParameterHandle,
};
use hla_fedpro_proto::fedpro;

pub fn encode_object_class(h: ObjectClassHandle) -> fedpro::ObjectClassHandle {
    fedpro::ObjectClassHandle {
        data: h.raw().to_be_bytes().to_vec(),
    }
}

pub fn decode_object_class(p: &fedpro::ObjectClassHandle) -> Option<ObjectClassHandle> {
    if p.data.len() != 4 {
        return None;
    }
    Some(ObjectClassHandle::new(u32::from_be_bytes(
        p.data[..].try_into().unwrap(),
    )))
}

pub fn encode_attribute(h: AttributeHandle) -> fedpro::AttributeHandle {
    fedpro::AttributeHandle {
        data: h.raw().to_be_bytes().to_vec(),
    }
}

pub fn decode_attribute(p: &fedpro::AttributeHandle) -> Option<AttributeHandle> {
    if p.data.len() != 4 {
        return None;
    }
    Some(AttributeHandle::new(u32::from_be_bytes(
        p.data[..].try_into().unwrap(),
    )))
}

pub fn encode_interaction_class(h: InteractionClassHandle) -> fedpro::InteractionClassHandle {
    fedpro::InteractionClassHandle {
        data: h.raw().to_be_bytes().to_vec(),
    }
}

pub fn decode_interaction_class(
    p: &fedpro::InteractionClassHandle,
) -> Option<InteractionClassHandle> {
    if p.data.len() != 4 {
        return None;
    }
    Some(InteractionClassHandle::new(u32::from_be_bytes(
        p.data[..].try_into().unwrap(),
    )))
}

pub fn encode_parameter(h: ParameterHandle) -> fedpro::ParameterHandle {
    fedpro::ParameterHandle {
        data: h.raw().to_be_bytes().to_vec(),
    }
}

pub fn decode_parameter(p: &fedpro::ParameterHandle) -> Option<ParameterHandle> {
    if p.data.len() != 4 {
        return None;
    }
    Some(ParameterHandle::new(u32::from_be_bytes(
        p.data[..].try_into().unwrap(),
    )))
}

pub fn encode_object_instance(h: ObjectInstanceHandle) -> fedpro::ObjectInstanceHandle {
    fedpro::ObjectInstanceHandle {
        data: h.raw().to_be_bytes().to_vec(),
    }
}

pub fn decode_object_instance(p: &fedpro::ObjectInstanceHandle) -> Option<ObjectInstanceHandle> {
    if p.data.len() != 8 {
        return None;
    }
    Some(ObjectInstanceHandle::new(u64::from_be_bytes(
        p.data[..].try_into().unwrap(),
    )))
}

pub fn decode_federate(p: &fedpro::FederateHandle) -> Option<FederateHandle> {
    if p.data.len() != 4 {
        return None;
    }
    Some(FederateHandle::new(u32::from_be_bytes(
        p.data[..].try_into().unwrap(),
    )))
}

pub fn decode_attribute_value_map(
    p: &fedpro::AttributeHandleValueMap,
) -> hla_core::AttributeHandleValueMap {
    p.attribute_handle_value
        .iter()
        .filter_map(|e| {
            let h = e.attribute_handle.as_ref().and_then(decode_attribute)?;
            Some((h, e.value.clone()))
        })
        .collect()
}

pub fn decode_parameter_value_map(
    p: &fedpro::ParameterHandleValueMap,
) -> hla_core::ParameterHandleValueMap {
    p.parameter_handle_value
        .iter()
        .filter_map(|e| {
            let h = e.parameter_handle.as_ref().and_then(decode_parameter)?;
            Some((h, e.value.clone()))
        })
        .collect()
}
