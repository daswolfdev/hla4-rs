//! HLA standard data types per the standard MIM in IEEE 1516.2.
//!
//! MVP scope: the primitives federates actually need to round-trip the Sushi
//! and NETN-BASE FOMs. Composite types (`HLAfixedRecord`, `HLAvariantRecord`)
//! are sketched but not yet exhaustive — implement as needed against FOM
//! coverage.
//!
//! Watch-out: HLA encoding rules specify *octet boundaries* for alignment
//! within composite types. Most C++ RTIs get this subtly wrong for nested
//! variants. Conformance-test against the worked examples in 1516.2 Annex B
//! before declaring this crate done.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("not enough bytes: needed {needed}, had {had}")]
    Truncated { needed: usize, had: usize },
    #[error("invalid encoding: {0}")]
    Invalid(&'static str),
    #[error("UTF-16 decode error")]
    Utf16,
}

/// Standard contract for every HLA data type per 1516.2.
pub trait DataElement: Sized {
    /// Alignment requirement of this element, in octets, for placement inside
    /// composite types.
    fn octet_boundary(&self) -> usize;

    /// Encoded length in octets, not counting leading padding.
    fn encoded_length(&self) -> usize;

    /// Append the encoded representation to `buf`. Callers handle alignment
    /// padding when assembling composites.
    fn encode_into(&self, buf: &mut Vec<u8>);

    /// Decode, returning the value and the number of bytes consumed.
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError>;

    fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.encoded_length());
        self.encode_into(&mut v);
        v
    }
}

// ----- primitives -----

macro_rules! impl_be_primitive {
    ($name:ident, $inner:ty, $bytes:literal) => {
        #[derive(Copy, Clone, Debug, PartialEq)]
        pub struct $name(pub $inner);

        impl DataElement for $name {
            fn octet_boundary(&self) -> usize {
                $bytes
            }
            fn encoded_length(&self) -> usize {
                $bytes
            }
            fn encode_into(&self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&self.0.to_be_bytes());
            }
            fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
                if buf.len() < $bytes {
                    return Err(EncodingError::Truncated {
                        needed: $bytes,
                        had: buf.len(),
                    });
                }
                let mut arr = [0u8; $bytes];
                arr.copy_from_slice(&buf[..$bytes]);
                Ok(($name(<$inner>::from_be_bytes(arr)), $bytes))
            }
        }
    };
}

impl_be_primitive!(HLAinteger16BE, i16, 2);
impl_be_primitive!(HLAinteger32BE, i32, 4);
impl_be_primitive!(HLAinteger64BE, i64, 8);
impl_be_primitive!(HLAfloat32BE, f32, 4);
impl_be_primitive!(HLAfloat64BE, f64, 8);

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct HLAoctet(pub u8);

impl DataElement for HLAoctet {
    fn octet_boundary(&self) -> usize {
        1
    }
    fn encoded_length(&self) -> usize {
        1
    }
    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.push(self.0);
    }
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        buf.first()
            .copied()
            .map(|b| (HLAoctet(b), 1))
            .ok_or(EncodingError::Truncated { needed: 1, had: 0 })
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct HLAboolean(pub bool);

impl DataElement for HLAboolean {
    fn octet_boundary(&self) -> usize {
        4
    }
    fn encoded_length(&self) -> usize {
        4
    }
    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.0 as i32).to_be_bytes());
    }
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (HLAinteger32BE(v), n) = HLAinteger32BE::decode_from(buf)?;
        Ok((HLAboolean(v != 0), n))
    }
}

// ----- strings -----

#[derive(Clone, Debug, PartialEq)]
pub struct HLAunicodeString(pub String);

impl DataElement for HLAunicodeString {
    fn octet_boundary(&self) -> usize {
        4
    }
    fn encoded_length(&self) -> usize {
        4 + self.0.encode_utf16().count() * 2
    }
    fn encode_into(&self, buf: &mut Vec<u8>) {
        let utf16: Vec<u16> = self.0.encode_utf16().collect();
        buf.extend_from_slice(&(utf16.len() as u32).to_be_bytes());
        for unit in utf16 {
            buf.extend_from_slice(&unit.to_be_bytes());
        }
    }
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError::Truncated {
                needed: 4,
                had: buf.len(),
            });
        }
        let n = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
        let need = 4 + n * 2;
        if buf.len() < need {
            return Err(EncodingError::Truncated {
                needed: need,
                had: buf.len(),
            });
        }
        // Decode directly via the iterator API to skip the intermediate
        // `Vec<u16>` allocation that `String::from_utf16` would have needed.
        let s: Result<String, _> = char::decode_utf16(
            (0..n).map(|i| u16::from_be_bytes([buf[4 + 2 * i], buf[5 + 2 * i]])),
        )
        .collect();
        let s = s.map_err(|_| EncodingError::Utf16)?;
        Ok((HLAunicodeString(s), need))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HLAopaqueData(pub Vec<u8>);

impl DataElement for HLAopaqueData {
    fn octet_boundary(&self) -> usize {
        4
    }
    fn encoded_length(&self) -> usize {
        4 + self.0.len()
    }
    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.0.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.0);
    }
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError::Truncated {
                needed: 4,
                had: buf.len(),
            });
        }
        let n = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
        let need = 4 + n;
        if buf.len() < need {
            return Err(EncodingError::Truncated {
                needed: need,
                had: buf.len(),
            });
        }
        Ok((HLAopaqueData(buf[4..need].to_vec()), need))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer32be_roundtrip() {
        let v = HLAinteger32BE(-12345);
        let bytes = v.to_bytes();
        let (got, n) = HLAinteger32BE::decode_from(&bytes).unwrap();
        assert_eq!(got, v);
        assert_eq!(n, 4);
    }

    #[test]
    fn unicode_string_roundtrip() {
        let v = HLAunicodeString("Hello, 世界".to_string());
        let bytes = v.to_bytes();
        let (got, _) = HLAunicodeString::decode_from(&bytes).unwrap();
        assert_eq!(got, v);
    }
}
