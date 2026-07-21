//! URI → `object_store::path::Path` conversion.
//!
//! Segment URIs are fully qualified (`s3://bucket/<key>` or
//! `file:///<local_path>/<key>`), but the underlying `ObjectStore` is
//! bucket-rooted (S3) or prefix-rooted (local) — it wants the relative
//! key, not the full URI. `Path::from(&str)` also splits on `/` and drops
//! empty segments, which would mangle `s3://bucket/...` into the literal
//! key `s3:/bucket/...`. Strip the configured base URI first so the path
//! lands at the right place.

use object_store::path::Path;

/// Convert a fully-qualified URI into a Path relative to `base_uri`.
pub fn uri_to_object_path(base_uri: &str, uri: &str) -> Path {
    let relative = uri
        .strip_prefix(base_uri)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(uri);
    Path::from(relative)
}
