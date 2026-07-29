// -- Segment URIs ---------------------------------------------------------
//
// CHA-203: cold URIs live under
// `{base_uri}/{catalog_uuid}/{branch_uuid}/{persist|snapshot}/{parent_uuid}/{segment_uuid}/data.{ext}`
// — catalog/branch isolation is visible at the filesystem layout and
// the parent_uuid groups segments by their persist/snapshot event.
//
// The layout is for humans reading a bucket listing. Nothing parses a
// stored URI: every consumer (retirement, the `segment_delete_set` sweep,
// compaction reconciliation) drives off metadata columns and treats the
// URI as an opaque string.
//
// The `branch_uuid` path segment therefore names the branch that WROTE the
// file, which is not necessarily a branch whose metadata references it —
// carry-forward gives a child's segment row the parent's URI verbatim.
// Ownership is the `branch_uuid` COLUMN, never the path.

use uuid::Uuid;

/// `kind` is a closed set: `"persist"` (from [`persist_segment_uri`]) or
/// `"snapshot"` (from [`snapshot_segment_uri`]), which keeps the writers'
/// prefixes disjoint so an orphan sweep over one kind's prefix cannot
/// reach the other's. If the set grows past two, lift `kind` into a
/// two-variant enum.
fn segment_uri(
    base_uri: &str,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    kind: &str,
    parent_uuid: &Uuid,
    segment_uuid: &Uuid,
    extension: &str,
) -> String {
    format!(
        "{base_uri}/{catalog_uuid}/{branch_uuid}/{kind}/{parent_uuid}/{segment_uuid}/data.{extension}"
    )
}

/// URI for a cold persist segment file.
pub fn persist_segment_uri(
    base_uri: &str,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_persist_uuid: &Uuid,
    segment_uuid: &Uuid,
    extension: &str,
) -> String {
    segment_uri(
        base_uri,
        catalog_uuid,
        branch_uuid,
        "persist",
        table_persist_uuid,
        segment_uuid,
        extension,
    )
}

/// URI for a cold `tx_log` persist segment file (CHA-507).
///
/// Its own `tx_log` path kind (no parent grouping) keeps it out of the
/// persist/snapshot orphan-retirement sweeps, which key off their own kinds.
pub fn tx_log_persist_segment_uri(
    base_uri: &str,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    segment_uuid: &Uuid,
    extension: &str,
) -> String {
    format!("{base_uri}/{catalog_uuid}/{branch_uuid}/tx_log/{segment_uuid}/data.{extension}")
}

/// URI for a cold snapshot segment file.
///
/// `branch_uuid` is the branch WRITING the file; a segment row on another
/// branch may reference it (carry-forward).
pub fn snapshot_segment_uri(
    base_uri: &str,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_snapshot_uuid: &Uuid,
    segment_uuid: &Uuid,
    extension: &str,
) -> String {
    segment_uri(
        base_uri,
        catalog_uuid,
        branch_uuid,
        "snapshot",
        table_snapshot_uuid,
        segment_uuid,
        extension,
    )
}

/// URI for a per-segment cold-index sidecar (CHA-412), co-located in its base
/// segment's directory alongside the `data.{ext}` file:
/// `.../snapshot/{snap}/{segment}/idx_{index_slug}.{ext}`. `index_slug`
/// distinguishes sidecars on the same segment — `"row_uuid"` for the internal
/// identity index; CHA-463 passes the user index's uuid.
///
/// `branch_uuid` is the branch WRITING the sidecar; an index row on another
/// branch may reference it (carry-forward).
pub fn segment_index_uri(
    base_uri: &str,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_snapshot_uuid: &Uuid,
    segment_uuid: &Uuid,
    index_slug: &str,
    extension: &str,
) -> String {
    format!(
        "{base_uri}/{catalog_uuid}/{branch_uuid}/snapshot/{table_snapshot_uuid}/{segment_uuid}/idx_{index_slug}.{extension}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_index_uri() {
        let cat = Uuid::from_u128(1);
        let br = Uuid::from_u128(2);
        let snap = Uuid::from_u128(3);
        let seg = Uuid::from_u128(4);
        let uri = segment_index_uri("s3://bucket", &cat, &br, &snap, &seg, "row_uuid", "parquet");
        assert_eq!(
            uri,
            format!("s3://bucket/{cat}/{br}/snapshot/{snap}/{seg}/idx_row_uuid.parquet"),
        );
    }

    #[test]
    fn test_persist_segment_uri() {
        let cat = Uuid::from_u128(1);
        let br = Uuid::from_u128(2);
        let tf = Uuid::from_u128(3);
        let seg = Uuid::from_u128(4);
        let uri = persist_segment_uri("s3://bucket", &cat, &br, &tf, &seg, "parquet");
        assert_eq!(
            uri,
            format!("s3://bucket/{cat}/{br}/persist/{tf}/{seg}/data.parquet"),
        );
    }

    #[test]
    fn test_snapshot_segment_uri() {
        let cat = Uuid::from_u128(1);
        let br = Uuid::from_u128(2);
        let snap = Uuid::from_u128(3);
        let seg = Uuid::from_u128(4);
        let uri = snapshot_segment_uri("s3://bucket", &cat, &br, &snap, &seg, "parquet");
        assert_eq!(
            uri,
            format!("s3://bucket/{cat}/{br}/snapshot/{snap}/{seg}/data.parquet"),
        );
    }
}
