//! Cross-cutting dispatch helpers.
//!
//! Holds `exception` / `exception_variant` (used by every service
//! group to build error responses) and the dead-code-suppressed
//! `_force_use_objectclass_handle_type` marker.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn exception(kind: HlaException, details: &str) -> fedpro::CallResponse {
    fedpro::CallResponse {
        call_response: Some(exception_variant(kind, details)),
    }
}

pub(super) fn exception_variant(kind: HlaException, details: &str) -> Resp {
    Resp::ExceptionData(ExceptionData {
        exception_name: kind.name().to_string(),
        details: details.to_string(),
    })
}

// Silence "imported but not used" if at some point we trim Re-exports.
#[allow(dead_code)]
pub(super) fn _force_use_objectclass_handle_type(_: ObjectClassHandle) {}
