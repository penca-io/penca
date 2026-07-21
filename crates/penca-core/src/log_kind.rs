//! `LogKind` — the closed-set discriminator on
//! `table_persist_metadata.log_kind` (CHA-203).
//!
//! The two variants enumerate the per-(table, branch) data logs:
//!
//! - [`LogKind::UpsertLog`] — the unified upsert/insert/update log
//!   (see ADR 0001). Each segment row carries `row_uuid + user_cols`
//!   plus the four denormalized tx metadata columns
//!   (`commit_micros, began_at_micros, comment, author`).
//! - [`LogKind::DeleteLog`] — the per-row delete tombstone log. Each
//!   segment row carries `row_uuid` plus the same four denormalized
//!   tx metadata columns.
//!
//! CHA-218 collapsed `LogKind::TxLog` out of cold storage: per-tx
//! framing is hot-only, and the four tx metadata columns are pre-joined
//! onto each cold data segment row at persist time (see ADR 0017).
//!
//! `log_kind` participates in the deterministic `table_persist_uuid`
//! derivation (`row_uuid_for_pk(branch_persist, [table_uuid, log_kind])`).
//! Rust-internal only — not exposed in any proto field; the column is
//! `TEXT CHECK (log_kind IN ('upsert_log','delete_log'))` in Postgres
//! and [`LogKind::as_str`] is the canonical string form for both SQL
//! binding and `IN (...)` literals. The Python parity tests hardcode
//! the same two strings against this enum's `as_str`.

use std::fmt;
use std::str::FromStr;

/// Returned by [`LogKind::from_str`] / [`TryFrom<&str>`] when the input
/// is not one of the two accepted strings. Carries the offending value
/// for diagnostics.
#[derive(Debug, thiserror::Error)]
#[error("unknown log_kind value: {0:?}")]
pub struct ParseLogKindError(pub String);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogKind {
    UpsertLog,
    DeleteLog,
}

impl LogKind {
    /// Canonical string form for SQL binding and goldens. Stable —
    /// matches the `CHECK (log_kind IN (...))` literals in the
    /// `table_persist_metadata` DDL.
    pub fn as_str(self) -> &'static str {
        match self {
            LogKind::UpsertLog => "upsert_log",
            LogKind::DeleteLog => "delete_log",
        }
    }
}

impl fmt::Display for LogKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LogKind {
    type Err = ParseLogKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "upsert_log" => Ok(LogKind::UpsertLog),
            "delete_log" => Ok(LogKind::DeleteLog),
            other => Err(ParseLogKindError(other.to_string())),
        }
    }
}

impl TryFrom<&str> for LogKind {
    type Error = ParseLogKindError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_matches_db_check_literals() {
        assert_eq!(LogKind::UpsertLog.as_str(), "upsert_log");
        assert_eq!(LogKind::DeleteLog.as_str(), "delete_log");
    }

    #[test]
    fn round_trip_via_from_str() {
        for kind in [LogKind::UpsertLog, LogKind::DeleteLog] {
            assert_eq!(kind.as_str().parse::<LogKind>().unwrap(), kind);
        }
    }

    #[test]
    fn from_str_rejects_unknown() {
        let err = "nope".parse::<LogKind>().unwrap_err();
        assert_eq!(err.0, "nope");
    }

    #[test]
    fn from_str_rejects_commit_tx_log() {
        // CHA-218: commit_tx_log is no longer a cold log_kind.
        let err = "commit_tx_log".parse::<LogKind>().unwrap_err();
        assert_eq!(err.0, "commit_tx_log");
    }
}
