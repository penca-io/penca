//! Generic paginate-to-exhaustion helper for `list_*` gRPC calls.
//!
//! Both `PencaCatalogProvider::schema_names` and
//! `PencaSchemaProvider::table_names` follow the same pagination policy:
//! 1000-item page size, drive the `next_page_token` to exhaustion, and
//! log-and-break on RPC error (the DataFusion trait surfaces consumed
//! by these calls are infallible — `-> Vec<String>` — so propagating
//! is not an option). This helper owns that policy; the call site
//! supplies the per-RPC fetcher.

use std::future::Future;

/// Drive `fetch_page` to exhaustion. Starts with an empty `page_token`;
/// after each successful page, advances to the returned token if `Some`
/// and non-empty, otherwise terminates. On RPC error, emits a
/// `tracing::error!` event with `rpc` and `error` fields and returns
/// the accumulator so far.
pub(crate) async fn paginate_to_exhaustion<T, F, Fut, E>(
    rpc_name: &'static str,
    mut fetch_page: F,
) -> Vec<T>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<(Vec<T>, Option<String>), E>>,
    E: std::fmt::Display,
{
    let mut all = Vec::new();
    let mut page_token = String::new();
    loop {
        match fetch_page(page_token).await {
            Ok((items, next_token)) => {
                all.extend(items);
                match next_token {
                    Some(t) if !t.is_empty() => page_token = t,
                    _ => break,
                }
            }
            Err(e) => {
                tracing::error!(rpc = rpc_name, error = %e, "rpc failed");
                break;
            }
        }
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[tokio::test]
    async fn empty_first_page_returns_empty_vec() {
        let result: Vec<String> =
            paginate_to_exhaustion("test", |_token| async { Ok::<_, &str>((vec![], None)) }).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn single_page_no_next_token_returns_items() {
        let result = paginate_to_exhaustion("test", |_token| async {
            Ok::<_, &str>((vec!["a".to_string(), "b".to_string()], None))
        })
        .await;
        assert_eq!(result, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn two_pages_concatenated_in_order() {
        let calls = RefCell::new(0);
        let result = paginate_to_exhaustion("test", |token| {
            let n = {
                let mut c = calls.borrow_mut();
                *c += 1;
                *c
            };
            async move {
                match n {
                    1 => {
                        assert_eq!(token, "");
                        Ok::<_, &str>((vec!["a".to_string()], Some("tok".to_string())))
                    }
                    2 => {
                        assert_eq!(token, "tok");
                        Ok((vec!["b".to_string()], None))
                    }
                    _ => panic!("unexpected extra call"),
                }
            }
        })
        .await;
        assert_eq!(result, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn empty_string_next_token_terminates() {
        let result = paginate_to_exhaustion("test", |_token| async {
            Ok::<_, &str>((vec!["a".to_string()], Some(String::new())))
        })
        .await;
        assert_eq!(result, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn error_logs_and_breaks_with_partial_accumulator() {
        let calls = RefCell::new(0);
        let result = paginate_to_exhaustion("test", |_token| {
            let n = {
                let mut c = calls.borrow_mut();
                *c += 1;
                *c
            };
            async move {
                if n == 1 {
                    Ok::<_, &str>((vec!["a".to_string()], Some("tok".to_string())))
                } else {
                    Err("boom")
                }
            }
        })
        .await;
        assert_eq!(result, vec!["a".to_string()]);
    }
}
