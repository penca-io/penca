//! Shared request/response helpers for the list-RPC family.
//!
//! Pagination (page token encode/decode + the take-and-token shape that
//! every list response builds) plus the small `Option<IntegerRange>`
//! → `(Option<i64>, Option<i64>)` destructure that several list and
//! lifecycle RPCs share at the request-parse boundary.

pub(crate) fn decode_page_token(token: &str) -> i64 {
    if token.is_empty() {
        return 0;
    }
    token.parse::<i64>().unwrap_or(0)
}

pub(crate) fn encode_page_token(offset: i64) -> String {
    offset.to_string()
}

/// Take the first `page_size` rows as the response page; derive the
/// `next_page_token` from the over-fetch convention (`rows.len() >
/// page_size` ⇒ more rows exist). The caller fetches `page_size + 1`
/// rows at the data layer so the next-page detection avoids a count
/// query — this helper consumes that contract on the response-build
/// side.
pub(crate) fn take_page_and_next_token<T>(
    rows: Vec<T>,
    page_size: i64,
    offset: i64,
) -> (Vec<T>, Option<String>) {
    let has_next = rows.len() as i64 > page_size;
    let page: Vec<T> = rows.into_iter().take(page_size as usize).collect();
    let next_page_token = if has_next {
        Some(encode_page_token(offset + page_size))
    } else {
        None
    };
    (page, next_page_token)
}

/// Destructure an optional `IntegerRange` into `(min,
/// max)`. Unset filter ⇒ `(None, None)`. Used by RPCs that
/// accept a half-open window at the request boundary and feed both
/// halves into downstream SQL builders.
pub(crate) fn timestamp_bounds(
    filter: Option<&penca_proto::external::v1::IntegerRange>,
) -> (Option<i64>, Option<i64>) {
    filter.map(|t| (t.min, t.max)).unwrap_or((None, None))
}

pub(crate) fn pagination_from_request(
    pagination: Option<&penca_proto::external::v1::PaginationRequest>,
    default_page_size: i64,
) -> (i64, i64) {
    match pagination {
        Some(p) => {
            let page_size = if p.page_size > 0 {
                p.page_size as i64
            } else {
                default_page_size
            };
            let offset = decode_page_token(&p.page_token);
            (page_size, offset)
        }
        None => (default_page_size, 0),
    }
}
