//! FOM / OMT representation, parsing, and modular merge.
//!
//! Domain types live here; XML deserialization in [`xml`]; modular FOM merge
//! per IEEE 1516.2 in [`merge`]. The MVP target FOM corpus is Sushi and
//! NETN-BASE — both exercise hierarchical object/interaction classes, named
//! attributes/parameters, and (for NETN-BASE) modular composition.

use std::collections::HashMap;

use hla_core::{
    AttributeHandle, DimensionHandle, InteractionClassHandle, ObjectClassHandle, ParameterHandle,
};
use thiserror::Error;

pub mod merge;
pub mod xml;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FomError {
    #[error("XML parse error: {0}")]
    Xml(String),
    #[error("modular FOM merge conflict on {0}")]
    MergeConflict(String),
    #[error("unknown reference: {0}")]
    UnknownReference(String),
    #[error("invalid enum value: {0}")]
    InvalidEnum(String),
}

#[derive(Clone, Debug, Default)]
pub struct ModuleIdentification {
    pub name: String,
    pub version: Option<String>,
    pub description: Option<String>,
}

/// A fully-qualified object class definition. `name` is the dotted FQN
/// (e.g. `"HLAobjectRoot.Food.Drink"`); `parent` is the FQN of the
/// immediately-enclosing class, or `None` for `HLAobjectRoot`.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectClassDef {
    pub name: String,
    pub parent: Option<String>,
    pub attributes: Vec<AttributeDef>,
    pub semantics: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttributeDef {
    pub name: String,
    pub datatype: String,
    pub order: OrderTypeRef,
    pub transportation: String,
    pub ownership: Ownership,
    pub sharing: Sharing,
    pub update_type: UpdateType,
    pub dimensions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteractionClassDef {
    pub name: String,
    pub parent: Option<String>,
    pub order: OrderTypeRef,
    pub transportation: String,
    pub parameters: Vec<ParameterDef>,
    pub semantics: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParameterDef {
    pub name: String,
    pub datatype: String,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OrderTypeRef {
    Receive,
    TimestampOrder,
}

impl OrderTypeRef {
    pub fn from_xml(s: &str) -> Result<Self, FomError> {
        match s {
            "Receive" => Ok(Self::Receive),
            "TimeStamp" | "TimestampOrder" => Ok(Self::TimestampOrder),
            other => Err(FomError::InvalidEnum(format!("order: {other:?}"))),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Ownership {
    DivestAcquire,
    NoTransfer,
}

impl Ownership {
    pub fn from_xml(s: &str) -> Result<Self, FomError> {
        match s {
            "DivestAcquire" => Ok(Self::DivestAcquire),
            "NoTransfer" => Ok(Self::NoTransfer),
            other => Err(FomError::InvalidEnum(format!("ownership: {other:?}"))),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Sharing {
    Publish,
    Subscribe,
    PublishSubscribe,
    Neither,
}

impl Sharing {
    pub fn from_xml(s: &str) -> Result<Self, FomError> {
        match s {
            "Publish" => Ok(Self::Publish),
            "Subscribe" => Ok(Self::Subscribe),
            "PublishSubscribe" => Ok(Self::PublishSubscribe),
            "Neither" => Ok(Self::Neither),
            other => Err(FomError::InvalidEnum(format!("sharing: {other:?}"))),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UpdateType {
    Static,
    Periodic,
    Conditional,
}

impl UpdateType {
    pub fn from_xml(s: &str) -> Result<Self, FomError> {
        match s {
            "Static" => Ok(Self::Static),
            "Periodic" => Ok(Self::Periodic),
            "Conditional" => Ok(Self::Conditional),
            other => Err(FomError::InvalidEnum(format!("update_type: {other:?}"))),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DatatypeLibrary {
    pub basic: Vec<String>,
    pub simple: Vec<String>,
    pub enumerated: Vec<String>,
    pub array: Vec<String>,
    pub fixed_record: Vec<String>,
    pub variant_record: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct FomModule {
    pub identification: ModuleIdentification,
    pub object_classes: Vec<ObjectClassDef>,
    pub interaction_classes: Vec<InteractionClassDef>,
    pub datatypes: DatatypeLibrary,
    pub dimensions: Vec<String>,
}

impl FomModule {
    /// Parse a 1516e/2025-style FOM XML document into a `FomModule`.
    ///
    /// The recursive `<objectClass>` and `<interactionClass>` trees are
    /// flattened into the `object_classes`/`interaction_classes` vectors,
    /// each entry carrying its fully-qualified name and parent FQN.
    pub fn parse(xml_input: &str) -> Result<Self, FomError> {
        xml::parse_fom_module(xml_input)
    }
}

#[derive(Debug, Default)]
pub struct MergedFom {
    pub modules: Vec<FomModule>,
    pub(crate) object_classes: HashMap<ObjectClassHandle, ObjectClassDef>,
    pub(crate) interaction_classes: HashMap<InteractionClassHandle, InteractionClassDef>,
    pub(crate) object_class_table: HashMap<String, ObjectClassHandle>,
    pub(crate) attribute_table: HashMap<(ObjectClassHandle, String), AttributeHandle>,
    pub(crate) interaction_class_table: HashMap<String, InteractionClassHandle>,
    pub(crate) parameter_table: HashMap<(InteractionClassHandle, String), ParameterHandle>,
    pub(crate) dimension_table: HashMap<String, DimensionHandle>,
}

impl MergedFom {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge a list of FOM modules into a single addressable FOM with
    /// stable handle assignments. Handles are assigned in document order:
    /// object classes by depth-first traversal of the merged tree, then
    /// attributes per-class, then interaction classes, then parameters.
    pub fn merge(modules: Vec<FomModule>) -> Result<Self, FomError> {
        merge::merge_modules(modules)
    }

    pub fn object_class_handle(&self, name: &str) -> Option<ObjectClassHandle> {
        self.object_class_table.get(name).copied()
    }

    pub fn object_class_def(&self, handle: ObjectClassHandle) -> Option<&ObjectClassDef> {
        self.object_classes.get(&handle)
    }

    pub fn attribute_handle(
        &self,
        class: ObjectClassHandle,
        name: &str,
    ) -> Option<AttributeHandle> {
        self.attribute_table
            .get(&(class, name.to_string()))
            .copied()
    }

    pub fn interaction_class_handle(&self, name: &str) -> Option<InteractionClassHandle> {
        self.interaction_class_table.get(name).copied()
    }

    pub fn interaction_class_def(
        &self,
        handle: InteractionClassHandle,
    ) -> Option<&InteractionClassDef> {
        self.interaction_classes.get(&handle)
    }

    pub fn parameter_handle(
        &self,
        class: InteractionClassHandle,
        name: &str,
    ) -> Option<ParameterHandle> {
        self.parameter_table
            .get(&(class, name.to_string()))
            .copied()
    }

    pub fn dimension_handle(&self, name: &str) -> Option<DimensionHandle> {
        self.dimension_table.get(name).copied()
    }

    /// Walk from `handle` toward the root, yielding each ancestor's handle
    /// (including `handle` itself). Used for subscription matching: a
    /// publisher of `Soda` should reach a subscriber to `Drink` because
    /// `Drink` is in `Soda`'s inheritance chain.
    pub fn inheritance_chain(
        &self,
        handle: ObjectClassHandle,
    ) -> impl Iterator<Item = ObjectClassHandle> + '_ {
        InheritanceIter {
            fom: self,
            next: Some(handle),
        }
    }
}

struct InheritanceIter<'a> {
    fom: &'a MergedFom,
    next: Option<ObjectClassHandle>,
}

impl<'a> Iterator for InheritanceIter<'a> {
    type Item = ObjectClassHandle;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next?;
        let def = self.fom.object_class_def(current)?;
        self.next = def
            .parent
            .as_deref()
            .and_then(|p| self.fom.object_class_handle(p));
        Some(current)
    }
}
