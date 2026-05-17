//! Dispatch handlers for the Advisory / Reporting Switches service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn get_switch_bool<G, W>(ctx: &SessionContext, getter: G, wrap: W) -> Resp
where
    G: Fn(&crate::Switches) -> bool,
    W: Fn(bool) -> Resp,
{
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    match federates.get(&m.federate_handle) {
        Some(fs) => wrap(getter(&fs.switches)),
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

pub(super) fn set_switch_bool<S, W>(
    ctx: &mut SessionContext,
    value: bool,
    setter: S,
    wrap: W,
) -> Resp
where
    S: Fn(&mut crate::Switches, bool),
    W: Fn() -> Resp,
{
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut federates = m.federation.federates.write();
    match federates.get_mut(&m.federate_handle) {
        Some(fs) => {
            setter(&mut fs.switches, value);
            wrap()
        }
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

pub(super) fn get_automatic_resign_directive(ctx: &SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::GetAutomaticResignDirectiveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    let fs = match federates.get(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    Resp::GetAutomaticResignDirectiveResponse(GetAutomaticResignDirectiveResponse {
        result: encode_resign_action(fs.switches.automatic_resign_directive),
    })
}

pub(super) fn set_automatic_resign_directive(ctx: &mut SessionContext, value: i32) -> Resp {
    use hla_fedpro_proto::fedpro::SetAutomaticResignDirectiveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let action = match value {
        0 => hla_core::ResignAction::UnconditionallyDivestAttributes,
        1 => hla_core::ResignAction::DeleteObjects,
        2 => hla_core::ResignAction::CancelPendingOwnershipAcquisitions,
        3 => hla_core::ResignAction::DeleteObjectsThenDivest,
        4 => hla_core::ResignAction::CancelThenDeleteThenDivest,
        5 => hla_core::ResignAction::NoAction,
        other => return exception_variant(HlaException::InvalidResignAction, &other.to_string()),
    };
    let mut federates = m.federation.federates.write();
    match federates.get_mut(&m.federate_handle) {
        Some(fs) => {
            fs.switches.automatic_resign_directive = action;
            Resp::SetAutomaticResignDirectiveResponse(SetAutomaticResignDirectiveResponse {})
        }
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

pub(super) fn encode_resign_action(a: hla_core::ResignAction) -> i32 {
    match a {
        hla_core::ResignAction::UnconditionallyDivestAttributes => 0,
        hla_core::ResignAction::DeleteObjects => 1,
        hla_core::ResignAction::CancelPendingOwnershipAcquisitions => 2,
        hla_core::ResignAction::DeleteObjectsThenDivest => 3,
        hla_core::ResignAction::CancelThenDeleteThenDivest => 4,
        hla_core::ResignAction::NoAction => 5,
    }
}
