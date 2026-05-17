//! Opaque HLA handles.
//!
//! IEEE 1516.1 treats handles as opaque to the federate. We use newtype wrappers
//! so the type system prevents e.g. passing an `AttributeHandle` where an
//! `InteractionClassHandle` is expected — a class of bug the C++/Java APIs can't
//! catch at compile time.

use std::collections::{HashMap, HashSet};

macro_rules! opaque_handle {
    ($name:ident, $repr:ty) => {
        #[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
        pub struct $name(pub(crate) $repr);

        impl $name {
            pub const fn new(raw: $repr) -> Self {
                Self(raw)
            }

            pub const fn raw(self) -> $repr {
                self.0
            }
        }
    };
}

opaque_handle!(ObjectClassHandle, u32);
opaque_handle!(AttributeHandle, u32);
opaque_handle!(InteractionClassHandle, u32);
opaque_handle!(ParameterHandle, u32);
opaque_handle!(ObjectInstanceHandle, u64);
opaque_handle!(FederateHandle, u32);
opaque_handle!(DimensionHandle, u32);
opaque_handle!(RegionHandle, u64);

pub type AttributeHandleSet = HashSet<AttributeHandle>;
pub type AttributeHandleValueMap = HashMap<AttributeHandle, Vec<u8>>;
pub type ParameterHandleValueMap = HashMap<ParameterHandle, Vec<u8>>;
pub type FederateHandleSet = HashSet<FederateHandle>;
