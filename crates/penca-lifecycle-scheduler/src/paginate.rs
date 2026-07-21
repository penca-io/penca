//! Generic loop driver for paginate-until-empty-token gRPC list calls.

use penca_proto::external::v1::PaginationRequest;

/// Drive a paginated list RPC to exhaustion. `fetch` builds one page
/// (with the supplied [`PaginationRequest`]), issues the RPC, and
/// returns the page's items plus the next page token. Termination
/// matches the existing scheduler convention: an absent token *or* an
/// empty-string token ends the loop. The first page is requested with
/// an empty token, also matching today's behavior.
pub(crate) async fn paginate_all<T, Fetch>(
    page_size: i32,
    mut fetch: Fetch,
) -> Result<Vec<T>, tonic::Status>
where
    Fetch: AsyncFnMut(PaginationRequest) -> Result<(Vec<T>, Option<String>), tonic::Status>,
{
    let mut out = Vec::new();
    let mut page_token = String::new();
    loop {
        let req = PaginationRequest {
            page_size,
            page_token: std::mem::take(&mut page_token),
        };
        let (items, next) = fetch(req).await?;
        out.extend(items);
        match next {
            Some(t) if !t.is_empty() => page_token = t,
            _ => return Ok(out),
        }
    }
}
