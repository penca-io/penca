//! SQL fragment builders shared across hot-tier modules.

/// Build the `" AND t.commit_micros >= … AND t.commit_micros < …"`
/// fragment for time-range filtering. Designed to compose after a
/// `WHERE 1=1` or `WHERE TRUE` so an empty range produces an empty
/// string and a one-sided range produces a single conjunct.
///
/// Values are system-generated integers (from proto fields), safe to
/// interpolate directly.
pub(crate) fn build_committed_at_filter(
    from_micros: Option<i64>,
    to_micros: Option<i64>,
) -> String {
    let mut filter = String::new();
    if let Some(from) = from_micros {
        filter.push_str(&format!(" AND t.commit_micros >= {from}"));
    }
    if let Some(to) = to_micros {
        filter.push_str(&format!(" AND t.commit_micros < {to}"));
    }
    filter
}

/// CHA-429: half-open `commit_seq_num` window for the audit `committed`
/// seq-axis cursor — sibling of [`build_committed_at_filter`] on the
/// commit-order serial (sourced from the `commit_tx_log` JOIN, alias `t`).
/// Composes after `WHERE TRUE` alongside the committed_at fragment; an
/// empty range produces an empty string. The "changes since N" cursor is
/// `from_seq = N + 1`.
pub(crate) fn build_commit_seq_num_filter(from_seq: Option<i64>, to_seq: Option<i64>) -> String {
    let mut filter = String::new();
    if let Some(from) = from_seq {
        filter.push_str(&format!(" AND t.commit_seq_num >= {from}"));
    }
    if let Some(to) = to_seq {
        filter.push_str(&format!(" AND t.commit_seq_num < {to}"));
    }
    filter
}
