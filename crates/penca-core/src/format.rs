//! `Format` — the closed-set discriminator for cold-segment columnar
//! file formats (CHA-299).
//!
//! Replaces the prior `naming::format_to_extension(i32) -> &'static str`
//! / `naming::format_from_extension(&str) -> i32` helpers, which silently
//! defaulted to Parquet on unknown wire codes and to `0`
//! (`STORAGE_FORMAT_UNSPECIFIED`) on unknown extensions. The closed-set
//! enum makes silent fall-through unrepresentable: callers must hold a
//! `Format` to ask for an extension, and obtaining one goes through a
//! fallible `TryFrom` / `FromStr` that surfaces `ParseFormatError`.
//!
//! # Wire-code contract
//!
//! The discriminant values are stable storage wire codes, used as the
//! `i32` key for cold-segment reader dispatch ([`Format::as_wire_code`]):
//!
//! - `Format::Lance = 1`
//! - `Format::Parquet = 2`
//!
//! Code `0` has no `Format` counterpart — it is a parse error. These
//! codes were formerly mirrored by the `StorageFormat` proto enum
//! (removed with the storage-metadata service in CHA-445); `Format` is
//! now the sole source of truth. `penca-core` itself stays free of any
//! `penca-proto` dependency (matching `LogKind`'s policy); the
//! wire-code contract is locked down by the `wire_code_goldens` unit
//! test below.

use std::fmt;
use std::str::FromStr;

/// Returned by [`Format::from_str`] / [`TryFrom<&str>`] / [`TryFrom<i32>`]
/// when the input does not name one of the two known formats. Carries
/// the offending value for diagnostics — extension string for the text
/// path, decimal-stringified wire code for the integer path.
#[derive(Debug, thiserror::Error)]
#[error("unknown storage format value: {0:?}")]
pub struct ParseFormatError(pub String);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i32)]
pub enum Format {
    Lance = 1,
    Parquet = 2,
}

impl Format {
    /// Canonical file-extension string for the format, used in the cold
    /// segment URI (`…/data.{ext}`) and persisted to the `format` text
    /// column on `table_persist_segment_metadata` /
    /// `table_snapshot_segment_metadata` rows.
    pub fn extension(self) -> &'static str {
        match self {
            Format::Lance => "lance",
            Format::Parquet => "parquet",
        }
    }

    /// The stable storage wire code (1 for Lance, 2 for Parquet). Used as
    /// the `i32` key for cold-segment reader dispatch — the
    /// `HashMap<i32, FormatReader>` lookup in `penca-storage-cold` /
    /// `penca-dl`.
    pub fn as_wire_code(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.extension())
    }
}

impl FromStr for Format {
    type Err = ParseFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lance" => Ok(Format::Lance),
            "parquet" => Ok(Format::Parquet),
            other => Err(ParseFormatError(other.to_string())),
        }
    }
}

impl TryFrom<&str> for Format {
    type Error = ParseFormatError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<i32> for Format {
    type Error = ParseFormatError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Format::Lance),
            2 => Ok(Format::Parquet),
            other => Err(ParseFormatError(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_extension_round_trip() {
        for fmt in [Format::Lance, Format::Parquet] {
            assert_eq!(fmt.extension().parse::<Format>().unwrap(), fmt);
        }
    }

    #[test]
    fn format_wire_code_round_trip() {
        for fmt in [Format::Lance, Format::Parquet] {
            assert_eq!(Format::try_from(fmt.as_wire_code()).unwrap(), fmt);
        }
    }

    #[test]
    fn parse_extension_rejects_unknown() {
        let err = "snappy".parse::<Format>().unwrap_err();
        assert_eq!(err.0, "snappy");
    }

    #[test]
    fn try_from_wire_rejects_unspecified() {
        // STORAGE_FORMAT_UNSPECIFIED = 0 has no Format counterpart —
        // the wire-code path must reject it explicitly, not silently
        // map it to a default variant.
        let err = Format::try_from(0_i32).unwrap_err();
        assert_eq!(err.0, "0");
    }

    #[test]
    fn try_from_wire_rejects_unknown() {
        let err = Format::try_from(99_i32).unwrap_err();
        assert_eq!(err.0, "99");
    }

    #[test]
    fn wire_code_goldens() {
        // Cross-language wire contract: these discriminants are the
        // stable on-disk/dispatch codes (mirrored by the Python client's
        // FORMAT_EXTENSIONS map). If anyone renumbers the variants, this
        // test fails loudly so the divergence is caught at compile/test
        // time rather than as silent on-disk corruption.
        assert_eq!(Format::Lance as i32, 1);
        assert_eq!(Format::Parquet as i32, 2);
    }
}
