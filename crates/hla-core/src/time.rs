//! Logical time abstraction.
//!
//! IEEE 1516.1 §8 defines logical time as a federation-wide choice — typical
//! deployments use either `HLAfloat64Time` (continuous-domain simulations) or
//! `HLAinteger64Time` (discrete-event simulations). Federations must agree.

use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Add;

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TimeCodecError {
    #[error("logical time encoding has wrong length: expected {expected}, got {got}")]
    WrongLength { expected: usize, got: usize },
}

pub trait LogicalTime:
    Copy + Debug + Send + Sync + 'static + PartialOrd + Ord + Eq + Add<Self::Interval, Output = Self>
{
    type Interval: LogicalTimeInterval;

    fn initial() -> Self;
    fn final_() -> Self;
    fn encoded_length() -> usize;
    fn encode(&self) -> Vec<u8>;
    fn decode(bytes: &[u8]) -> Result<Self, TimeCodecError>;
}

pub trait LogicalTimeInterval: Copy + Debug + Send + Sync + 'static + Ord + Eq {
    fn zero() -> Self;
    fn epsilon() -> Self;
}

#[derive(Copy, Clone, Debug)]
pub struct HlaFloat64Time(pub f64);

#[derive(Copy, Clone, Debug)]
pub struct HlaFloat64Interval(pub f64);

impl PartialEq for HlaFloat64Time {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for HlaFloat64Time {}

impl PartialOrd for HlaFloat64Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HlaFloat64Time {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Add<HlaFloat64Interval> for HlaFloat64Time {
    type Output = HlaFloat64Time;
    fn add(self, rhs: HlaFloat64Interval) -> Self {
        HlaFloat64Time(self.0 + rhs.0)
    }
}

impl PartialEq for HlaFloat64Interval {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for HlaFloat64Interval {}
impl PartialOrd for HlaFloat64Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HlaFloat64Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl LogicalTime for HlaFloat64Time {
    type Interval = HlaFloat64Interval;

    fn initial() -> Self {
        HlaFloat64Time(0.0)
    }
    fn final_() -> Self {
        HlaFloat64Time(f64::INFINITY)
    }
    fn encoded_length() -> usize {
        8
    }
    fn encode(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self, TimeCodecError> {
        if bytes.len() != 8 {
            return Err(TimeCodecError::WrongLength {
                expected: 8,
                got: bytes.len(),
            });
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(HlaFloat64Time(f64::from_be_bytes(buf)))
    }
}

impl LogicalTimeInterval for HlaFloat64Interval {
    fn zero() -> Self {
        HlaFloat64Interval(0.0)
    }
    fn epsilon() -> Self {
        HlaFloat64Interval(f64::EPSILON)
    }
}
