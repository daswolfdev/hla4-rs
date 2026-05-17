//! Dispatch handlers for the Handle lookup service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn need_membership(ctx: &SessionContext) -> Result<&Membership, Resp> {
    ctx.membership
        .as_ref()
        .ok_or_else(|| exception_variant(HlaException::FederateNotExecutionMember, ""))
}

pub(super) fn get_object_class_handle(ctx: &SessionContext, name: &str) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    match m.federation.fom.object_class_handle(name) {
        Some(h) => Resp::GetObjectClassHandleResponse(GetObjectClassHandleResponse {
            result: Some(encode_object_class(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

pub(super) fn get_object_class_name(ctx: &SessionContext, h: fedpro::ObjectClassHandle) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let handle = match decode_object_class(&h) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    match m.federation.fom.object_class_def(handle) {
        Some(d) => Resp::GetObjectClassNameResponse(GetObjectClassNameResponse {
            result: d.name.clone(),
        }),
        None => exception_variant(
            HlaException::InvalidObjectClassHandle,
            &format!("{handle:?}"),
        ),
    }
}

pub(super) fn get_attribute_handle(
    ctx: &SessionContext,
    class: fedpro::ObjectClassHandle,
    attr_name: &str,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_object_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    if m.federation.fom.object_class_def(class).is_none() {
        return exception_variant(
            HlaException::InvalidObjectClassHandle,
            &format!("{class:?}"),
        );
    }
    match m.federation.fom.attribute_handle(class, attr_name) {
        Some(h) => Resp::GetAttributeHandleResponse(GetAttributeHandleResponse {
            result: Some(encode_attribute(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, attr_name),
    }
}

pub(super) fn get_attribute_name(
    ctx: &SessionContext,
    class: fedpro::ObjectClassHandle,
    attr: fedpro::AttributeHandle,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_object_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let attr = match decode_attribute(&attr) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let def = match m.federation.fom.object_class_def(class) {
        Some(d) => d,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    // Linear scan; small N. We don't currently store attribute names indexed
    // by handle directly — `attribute_table` is name→handle.
    for a in &def.attributes {
        if let Some(h) = m.federation.fom.attribute_handle(class, &a.name)
            && h == attr
        {
            return Resp::GetAttributeNameResponse(GetAttributeNameResponse {
                result: a.name.clone(),
            });
        }
    }
    exception_variant(HlaException::AttributeNotDefined, &format!("{attr:?}"))
}

pub(super) fn get_interaction_class_handle(ctx: &SessionContext, name: &str) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    match m.federation.fom.interaction_class_handle(name) {
        Some(h) => Resp::GetInteractionClassHandleResponse(GetInteractionClassHandleResponse {
            result: Some(encode_interaction_class(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

pub(super) fn get_interaction_class_name(
    ctx: &SessionContext,
    h: fedpro::InteractionClassHandle,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let handle = match decode_interaction_class(&h) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    match m.federation.fom.interaction_class_def(handle) {
        Some(d) => Resp::GetInteractionClassNameResponse(GetInteractionClassNameResponse {
            result: d.name.clone(),
        }),
        None => exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    }
}

pub(super) fn get_parameter_handle(
    ctx: &SessionContext,
    class: fedpro::InteractionClassHandle,
    param_name: &str,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_interaction_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    if m.federation.fom.interaction_class_def(class).is_none() {
        return exception_variant(HlaException::InvalidInteractionClassHandle, "");
    }
    match m.federation.fom.parameter_handle(class, param_name) {
        Some(h) => Resp::GetParameterHandleResponse(GetParameterHandleResponse {
            result: Some(encode_parameter(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, param_name),
    }
}

pub(super) fn get_parameter_name(
    ctx: &SessionContext,
    class: fedpro::InteractionClassHandle,
    param: fedpro::ParameterHandle,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_interaction_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let param = match crate::handles::decode_parameter(&param) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let def = match m.federation.fom.interaction_class_def(class) {
        Some(d) => d,
        None => return exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    };
    for p in &def.parameters {
        if let Some(h) = m.federation.fom.parameter_handle(class, &p.name)
            && h == param
        {
            return Resp::GetParameterNameResponse(GetParameterNameResponse {
                result: p.name.clone(),
            });
        }
    }
    exception_variant(HlaException::InteractionParameterNotDefined, "")
}
