//! Dispatch handlers for the Federate self-introspection service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn get_federate_handle(ctx: &SessionContext, name: &str) -> Resp {
    use crate::handles::encode_federate;
    use hla_fedpro_proto::fedpro::GetFederateHandleResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    match federates.values().find(|f| f.name == name) {
        Some(fs) => Resp::GetFederateHandleResponse(GetFederateHandleResponse {
            result: Some(encode_federate(fs.handle)),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

pub(super) fn get_federate_name(ctx: &SessionContext, handle: fedpro::FederateHandle) -> Resp {
    use hla_fedpro_proto::fedpro::GetFederateNameResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let fh = match handle.data.len() {
        4 => FederateHandle::new(u32::from_be_bytes(handle.data[..].try_into().unwrap())),
        _ => return exception_variant(HlaException::InvalidFederateHandle, ""),
    };
    let federates = m.federation.federates.read();
    match federates.get(&fh) {
        Some(fs) => Resp::GetFederateNameResponse(GetFederateNameResponse {
            result: fs.name.clone(),
        }),
        None => exception_variant(HlaException::InvalidFederateHandle, ""),
    }
}

/// Well-known order types per IEEE 1516.1: Receive (0), TimeStamp (1).
pub(super) fn get_order_type(name: &str) -> Resp {
    use hla_fedpro_proto::fedpro::GetOrderTypeResponse;
    let raw = match name {
        "Receive" => 0i32,
        "TimeStamp" | "TimestampOrder" => 1i32,
        _ => return exception_variant(HlaException::InvalidOrderName, name),
    };
    Resp::GetOrderTypeResponse(GetOrderTypeResponse { result: raw })
}

pub(super) fn get_order_name(order_type: i32) -> Resp {
    use hla_fedpro_proto::fedpro::GetOrderNameResponse;
    let name = match order_type {
        0 => "Receive",
        1 => "TimeStamp",
        _ => return exception_variant(HlaException::InvalidOrderType, &order_type.to_string()),
    };
    Resp::GetOrderNameResponse(GetOrderNameResponse {
        result: name.to_string(),
    })
}

/// Well-known transportation types per IEEE 1516.1: HLAreliable (1), HLAbestEffort (2).
pub(super) fn get_transportation_type_handle(name: &str) -> Resp {
    use hla_fedpro_proto::fedpro::{GetTransportationTypeHandleResponse, TransportationTypeHandle};
    let raw: u32 = match name {
        "HLAreliable" => 1,
        "HLAbestEffort" => 2,
        _ => return exception_variant(HlaException::InvalidTransportationName, name),
    };
    Resp::GetTransportationTypeHandleResponse(GetTransportationTypeHandleResponse {
        result: Some(TransportationTypeHandle {
            data: raw.to_be_bytes().to_vec(),
        }),
    })
}

pub(super) fn get_transportation_type_name(handle: fedpro::TransportationTypeHandle) -> Resp {
    use hla_fedpro_proto::fedpro::GetTransportationTypeNameResponse;
    if handle.data.len() != 4 {
        return exception_variant(HlaException::InvalidTransportationTypeHandle, "");
    }
    let raw = u32::from_be_bytes(handle.data[..].try_into().unwrap());
    let name = match raw {
        1 => "HLAreliable",
        2 => "HLAbestEffort",
        _ => {
            return exception_variant(
                HlaException::InvalidTransportationTypeHandle,
                &raw.to_string(),
            );
        }
    };
    Resp::GetTransportationTypeNameResponse(GetTransportationTypeNameResponse {
        result: name.to_string(),
    })
}
