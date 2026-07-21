//! Penca SQL primitives shared between hot and cold tiers — the dialect
//! contract every engine implements, plus the composite-tiebreaker merge
//! resolution kernel.
mod dialect;
mod merge_resolution;

pub use dialect::{
    Dialect, leading_comma_if_nonempty, qualify_user_cols, row_uuid_in_clause,
    row_uuid_in_clause_after,
};
pub use merge_resolution::{
    CompositeMergeResolution, build_composite_merge_resolution, lex_compare_predicate,
};
