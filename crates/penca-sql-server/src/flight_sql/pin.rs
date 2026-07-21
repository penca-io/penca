//! CHA-374: the pure decision for the auto-commit read-snapshot pin.
//!
//! `GetFlightInfo` calls [`pin_as_of_seq`] to decide whether to pin a statement's
//! read snapshot, then stamps the result on the `CommandTicket`. Keeping the
//! decision a pure function makes the policy exhaustively unit-testable and
//! shared by both the statement and prepared-statement entry-points (one
//! implementation → driver parity by construction).
//!
//! The decision is binary on a single input — *is the connection in a
//! transaction?* An open tx carries its own snapshot via `open_tx_uuid`, so
//! the pin is auto-commit-only. There is deliberately no `as_of` input to
//! arbitrate here: `GetFlightInfo` mints a fresh ticket (no client-supplied
//! snapshot reaches it), and the `as_of` ⊕ `open_tx_uuid` mutual exclusion is
//! enforced fail-fast at the read boundary (`penca-api`
//! `resolve_query_snapshot`), never silently collapsed in this helper.

/// Decide the auto-commit read-snapshot pin (CHA-374 / CHA-460). `None` when a
/// tx is open — the open tx carries the snapshot via `open_tx_uuid` and the pin
/// is auto-commit-only; otherwise pin to the freshly captured `seq_frontier`
/// (the branch's max committed `commit_seq_num`).
pub(crate) fn pin_as_of_seq(open_tx: Option<&str>, seq_frontier: i64) -> Option<i64> {
    match open_tx {
        Some(_) => None,
        None => Some(seq_frontier),
    }
}

#[cfg(test)]
mod tests {
    use super::pin_as_of_seq;

    #[test]
    fn in_tx_does_not_pin() {
        // Open tx → the open tx carries the snapshot; the pin stays None.
        assert_eq!(pin_as_of_seq(Some("tx-1"), 1000), None);
    }

    #[test]
    fn auto_commit_pins_to_seq_frontier() {
        // No open tx → pin to the freshly captured seq frontier.
        assert_eq!(pin_as_of_seq(None, 1000), Some(1000));
    }
}
