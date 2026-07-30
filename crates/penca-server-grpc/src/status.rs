//! Error mapping from `ApiError` to `tonic::Status`.

use penca_api::ApiError;
use tonic::Status;

/// Convert an [`ApiError`] into the appropriate [`tonic::Status`] code.
pub fn api_error_to_status(err: ApiError) -> Status {
    match &err {
        ApiError::InvalidRequest(_) => Status::invalid_argument(err.to_string()),
        ApiError::NotFound(_) => Status::not_found(err.to_string()),
        // CHA-236 — name-uniqueness collision on Create* / Update*.
        ApiError::AlreadyExists(_) => Status::already_exists(err.to_string()),
        ApiError::FailedPrecondition(_) => Status::failed_precondition(err.to_string()),
        ApiError::Unimplemented(_) => Status::unimplemented(err.to_string()),
        // ADR 0019 §"Defaults" — the cap surfaces as RESOURCE_EXHAUSTED;
        // the message body already names `query_timeout_seconds` and
        // the retry pattern.
        ApiError::QueryTimeout(_) => Status::resource_exhausted(err.to_string()),
        // `Aborted` is the canonical "concurrency conflict, retry at a higher
        // level" code — the distinction a caller needs to tell a lost lock race
        // from a genuine failure.
        ApiError::Aborted(_) => Status::aborted(err.to_string()),
        _ => Status::internal(err.to_string()),
    }
}
