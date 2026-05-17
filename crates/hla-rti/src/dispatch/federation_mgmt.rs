//! Dispatch handlers for the Federation Management service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn create_federation_inner(
    node: &Arc<RtiNode>,
    federation_name: String,
    proto_modules: Vec<fedpro::FomModule>,
) -> Result<(), Resp> {
    if federation_name.is_empty() {
        return Err(exception_variant(
            HlaException::ErrorReadingFdd,
            "federationName must not be empty",
        ));
    }
    let mut federations = node.federations.write();
    if federations.contains_key(&federation_name) {
        return Err(exception_variant(
            HlaException::FederationExecutionAlreadyExists,
            &federation_name,
        ));
    }
    let mut modules = Vec::new();
    for pm in &proto_modules {
        match decode_fom_module(pm) {
            Ok(m) => modules.push(m),
            Err(e) => return Err(exception_variant(HlaException::CouldNotOpenFdd, &e)),
        }
    }
    let fom = if modules.is_empty() {
        Arc::clone(&*node.default_fom.read())
    } else {
        match hla_omt::MergedFom::merge(modules) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                return Err(exception_variant(
                    HlaException::ErrorReadingFdd,
                    &e.to_string(),
                ));
            }
        }
    };
    let federation = Arc::new(Federation::new(federation_name.clone(), fom));
    federations.insert(federation_name, federation);
    Ok(())
}

/// Extract the XML text from a `FomModule` oneof and parse it.
/// MVP supports `FileFomModule` (inline name + content) only. `compressedModule`
/// and `url` return `Err("unsupported FOM module form")`.
pub(super) fn decode_fom_module(pm: &fedpro::FomModule) -> Result<hla_omt::FomModule, String> {
    use fedpro::fom_module::FomModule as Variant;
    match pm.fom_module.as_ref() {
        Some(Variant::File(f)) => {
            let text = std::str::from_utf8(&f.content)
                .map_err(|e| format!("FOM module {:?} content is not UTF-8: {}", f.name, e))?;
            hla_omt::FomModule::parse(text).map_err(|e| format!("{}: {}", f.name, e))
        }
        Some(Variant::CompressedModule(_)) => {
            Err("compressed FOM modules not yet supported".into())
        }
        Some(Variant::Url(_)) => Err("URL FOM modules not yet supported".into()),
        None => Err("FomModule has no oneof variant set".into()),
    }
}

pub(super) fn destroy_federation_execution(node: &Arc<RtiNode>, federation_name: String) -> Resp {
    let mut federations = node.federations.write();
    let entry = match federations.get(&federation_name) {
        Some(e) => e,
        None => {
            return exception_variant(
                HlaException::FederationExecutionDoesNotExist,
                &federation_name,
            );
        }
    };
    if !entry.federates.read().is_empty() {
        return exception_variant(HlaException::FederatesCurrentlyJoined, &federation_name);
    }
    federations.remove(&federation_name);
    Resp::DestroyFederationExecutionResponse(DestroyFederationExecutionResponse {})
}

pub(super) fn join(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    requested_name: Option<String>,
    federation_name: String,
) -> Result<JoinResult, Resp> {
    if ctx.is_joined() {
        return Err(exception_variant(
            HlaException::FederateAlreadyExecutionMember,
            &format!("session {}", ctx.session_id),
        ));
    }
    let federation = {
        let federations = node.federations.read();
        match federations.get(&federation_name) {
            Some(f) => Arc::clone(f),
            None => {
                return Err(exception_variant(
                    HlaException::FederationExecutionDoesNotExist,
                    &federation_name,
                ));
            }
        }
    };

    let raw_handle = federation.next_federate_id.fetch_add(1, Ordering::Relaxed);
    let federate_handle = FederateHandle::new(raw_handle);
    let federate_name = match requested_name {
        Some(n) => {
            let federates = federation.federates.read();
            if federates.values().any(|f| f.name == n) {
                return Err(exception_variant(
                    HlaException::FederateNameAlreadyInUse,
                    &n,
                ));
            }
            n
        }
        None => format!("federate-{raw_handle:08X}"),
    };

    federation.federates.write().insert(
        federate_handle,
        FederateSession {
            handle: federate_handle,
            name: federate_name.clone(),
            federate_type: String::new(),
            session_id: ctx.session_id,
            pub_sub: PubSubState::default(),
            time: crate::TimeState::default(),
            switches: crate::Switches::default(),
            tso_queue: Vec::new(),
        },
    );

    ctx.membership = Some(Membership {
        federation,
        federate_handle,
        federate_name,
    });

    Ok(JoinResult {
        federate_handle: Some(crate::handles::encode_federate(federate_handle)),
        logical_time_implementation_name: "HLAfloat64Time".to_string(),
    })
}

pub(super) fn resign(ctx: &mut SessionContext) -> Resp {
    let membership = match ctx.membership.take() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    membership
        .federation
        .federates
        .write()
        .remove(&membership.federate_handle);
    Resp::ResignFederationExecutionResponse(ResignFederationExecutionResponse {})
}
