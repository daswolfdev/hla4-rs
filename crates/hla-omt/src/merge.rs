//! Modular FOM merge per IEEE 1516.2.
//!
//! MVP semantics: union-by-FQN. When two modules define the same object or
//! interaction class:
//!   * attribute / parameter lists are unioned by name
//!   * conflicting field values on the same attribute name → `MergeConflict`
//! Handles are assigned in deterministic order:
//!   * Object classes: depth-first walk of the merged inheritance tree
//!     rooted at `HLAobjectRoot`, in insertion order at each level
//!   * Attributes: per-class, in attribute insertion order
//!   * Interaction classes / parameters: symmetric

use std::collections::{BTreeMap, HashMap};

use hla_core::{
    AttributeHandle, InteractionClassHandle, ObjectClassHandle, ParameterHandle,
};

use crate::{
    AttributeDef, FomError, FomModule, InteractionClassDef, MergedFom, ObjectClassDef,
    ParameterDef,
};

pub(crate) fn merge_modules(modules: Vec<FomModule>) -> Result<MergedFom, FomError> {
    let mut merged_objects: BTreeMap<String, ObjectClassDef> = BTreeMap::new();
    let mut merged_interactions: BTreeMap<String, InteractionClassDef> = BTreeMap::new();
    // Preserve insertion order separately for handle-assignment determinism.
    let mut object_order: Vec<String> = Vec::new();
    let mut interaction_order: Vec<String> = Vec::new();

    for module in &modules {
        for cls in &module.object_classes {
            merge_object_class(cls, &mut merged_objects, &mut object_order)?;
        }
        for ic in &module.interaction_classes {
            merge_interaction_class(ic, &mut merged_interactions, &mut interaction_order)?;
        }
    }

    // ---- assign handles for object classes (depth-first from roots) ----
    let mut object_classes: HashMap<ObjectClassHandle, ObjectClassDef> = HashMap::new();
    let mut object_class_table: HashMap<String, ObjectClassHandle> = HashMap::new();
    let mut attribute_table: HashMap<(ObjectClassHandle, String), AttributeHandle> =
        HashMap::new();

    let mut next_class: u32 = 1;
    let mut next_attribute: u32 = 1;

    // Build parent → children adjacency (insertion-order preserved).
    let mut children_of: HashMap<Option<String>, Vec<String>> = HashMap::new();
    for fqn in &object_order {
        let parent = merged_objects[fqn].parent.clone();
        children_of.entry(parent).or_default().push(fqn.clone());
    }

    let roots = children_of.remove(&None).unwrap_or_default();
    for root in roots {
        assign_object_class_handles(
            &root,
            &merged_objects,
            &children_of,
            &mut next_class,
            &mut next_attribute,
            &mut object_classes,
            &mut object_class_table,
            &mut attribute_table,
        );
    }

    // ---- assign handles for interaction classes (symmetric) ----
    let mut interaction_classes: HashMap<InteractionClassHandle, InteractionClassDef> =
        HashMap::new();
    let mut interaction_class_table: HashMap<String, InteractionClassHandle> = HashMap::new();
    let mut parameter_table: HashMap<(InteractionClassHandle, String), ParameterHandle> =
        HashMap::new();

    let mut next_interaction: u32 = 1;
    let mut next_parameter: u32 = 1;

    let mut ix_children_of: HashMap<Option<String>, Vec<String>> = HashMap::new();
    for fqn in &interaction_order {
        let parent = merged_interactions[fqn].parent.clone();
        ix_children_of.entry(parent).or_default().push(fqn.clone());
    }
    let ix_roots = ix_children_of.remove(&None).unwrap_or_default();
    for root in ix_roots {
        assign_interaction_class_handles(
            &root,
            &merged_interactions,
            &ix_children_of,
            &mut next_interaction,
            &mut next_parameter,
            &mut interaction_classes,
            &mut interaction_class_table,
            &mut parameter_table,
        );
    }

    Ok(MergedFom {
        modules,
        object_classes,
        interaction_classes,
        object_class_table,
        attribute_table,
        interaction_class_table,
        parameter_table,
        dimension_table: HashMap::new(),
    })
}

fn merge_object_class(
    cls: &ObjectClassDef,
    merged: &mut BTreeMap<String, ObjectClassDef>,
    order: &mut Vec<String>,
) -> Result<(), FomError> {
    if let Some(existing) = merged.get_mut(&cls.name) {
        if existing.parent != cls.parent {
            return Err(FomError::MergeConflict(format!(
                "{}: parent disagreement ({:?} vs {:?})",
                cls.name, existing.parent, cls.parent
            )));
        }
        for attr in &cls.attributes {
            merge_attribute(&cls.name, attr, &mut existing.attributes)?;
        }
        if existing.semantics.is_none() {
            existing.semantics = cls.semantics.clone();
        }
    } else {
        order.push(cls.name.clone());
        merged.insert(cls.name.clone(), cls.clone());
    }
    Ok(())
}

fn merge_attribute(
    class_name: &str,
    attr: &AttributeDef,
    target: &mut Vec<AttributeDef>,
) -> Result<(), FomError> {
    if let Some(existing) = target.iter().find(|a| a.name == attr.name) {
        if existing != attr {
            return Err(FomError::MergeConflict(format!(
                "{class_name}.{}: attribute field disagreement",
                attr.name
            )));
        }
        Ok(())
    } else {
        target.push(attr.clone());
        Ok(())
    }
}

fn merge_interaction_class(
    ic: &InteractionClassDef,
    merged: &mut BTreeMap<String, InteractionClassDef>,
    order: &mut Vec<String>,
) -> Result<(), FomError> {
    if let Some(existing) = merged.get_mut(&ic.name) {
        if existing.parent != ic.parent {
            return Err(FomError::MergeConflict(format!(
                "{}: parent disagreement",
                ic.name
            )));
        }
        for p in &ic.parameters {
            merge_parameter(&ic.name, p, &mut existing.parameters)?;
        }
        if existing.semantics.is_none() {
            existing.semantics = ic.semantics.clone();
        }
    } else {
        order.push(ic.name.clone());
        merged.insert(ic.name.clone(), ic.clone());
    }
    Ok(())
}

fn merge_parameter(
    class_name: &str,
    p: &ParameterDef,
    target: &mut Vec<ParameterDef>,
) -> Result<(), FomError> {
    if let Some(existing) = target.iter().find(|x| x.name == p.name) {
        if existing != p {
            return Err(FomError::MergeConflict(format!(
                "{class_name}.{}: parameter field disagreement",
                p.name
            )));
        }
        Ok(())
    } else {
        target.push(p.clone());
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn assign_object_class_handles(
    fqn: &str,
    merged: &BTreeMap<String, ObjectClassDef>,
    children_of: &HashMap<Option<String>, Vec<String>>,
    next_class: &mut u32,
    next_attribute: &mut u32,
    object_classes: &mut HashMap<ObjectClassHandle, ObjectClassDef>,
    object_class_table: &mut HashMap<String, ObjectClassHandle>,
    attribute_table: &mut HashMap<(ObjectClassHandle, String), AttributeHandle>,
) {
    let handle = ObjectClassHandle::new(*next_class);
    *next_class += 1;

    let def = merged[fqn].clone();
    for attr in &def.attributes {
        let ah = AttributeHandle::new(*next_attribute);
        *next_attribute += 1;
        attribute_table.insert((handle, attr.name.clone()), ah);
    }
    object_class_table.insert(fqn.to_string(), handle);
    object_classes.insert(handle, def);

    if let Some(children) = children_of.get(&Some(fqn.to_string())) {
        for child in children {
            assign_object_class_handles(
                child,
                merged,
                children_of,
                next_class,
                next_attribute,
                object_classes,
                object_class_table,
                attribute_table,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn assign_interaction_class_handles(
    fqn: &str,
    merged: &BTreeMap<String, InteractionClassDef>,
    children_of: &HashMap<Option<String>, Vec<String>>,
    next_interaction: &mut u32,
    next_parameter: &mut u32,
    interaction_classes: &mut HashMap<InteractionClassHandle, InteractionClassDef>,
    interaction_class_table: &mut HashMap<String, InteractionClassHandle>,
    parameter_table: &mut HashMap<(InteractionClassHandle, String), ParameterHandle>,
) {
    let handle = InteractionClassHandle::new(*next_interaction);
    *next_interaction += 1;

    let def = merged[fqn].clone();
    for p in &def.parameters {
        let ph = ParameterHandle::new(*next_parameter);
        *next_parameter += 1;
        parameter_table.insert((handle, p.name.clone()), ph);
    }
    interaction_class_table.insert(fqn.to_string(), handle);
    interaction_classes.insert(handle, def);

    if let Some(children) = children_of.get(&Some(fqn.to_string())) {
        for child in children {
            assign_interaction_class_handles(
                child,
                merged,
                children_of,
                next_interaction,
                next_parameter,
                interaction_classes,
                interaction_class_table,
                parameter_table,
            );
        }
    }
}
