//! Shared tonic server helpers: gRPC TraceLayer install for per-request spans.

use std::time::Duration;

use tonic::Code;
use tower_http::classify::{GrpcErrorsAsFailures, GrpcFailureClass, SharedClassifier};
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnBodyChunk, DefaultOnEos, DefaultOnRequest, DefaultOnResponse,
    TraceLayer,
};
use tracing::{Level, Span};

type OnGrpcFailure = fn(GrpcFailureClass, Duration, &Span);

/// `TraceLayer` configured for gRPC. The default `OnFailure` handler is
/// replaced so the boundary failure log scopes ERROR to server-fault statuses
/// (`Internal`, `Unavailable`, `Unknown`, `DataLoss`, `ResourceExhausted`) and
/// emits client-driven statuses (`NotFound`, `InvalidArgument`,
/// `AlreadyExists`, `FailedPrecondition`, `Unimplemented`, `PermissionDenied`,
/// `Unauthenticated`, `OutOfRange`) at DEBUG. This keeps ops dashboards aligned
/// with on-call signal — normal client behavior (404s, validation rejects, name
/// collisions, transaction-state precondition failures) no longer inflates the
/// error-rate gauge or pages the on-call. CHA-326.
///
/// Implementation note: we keep client errors in the failure bucket (via
/// `.on_failure(...)` rather than `GrpcErrorsAsFailures::with_success(...)`)
/// because they *are* failures from the RPC contract's perspective — the
/// emitted log just dispatches to a different level. That preserves
/// observability symmetry: every non-OK boundary outcome surfaces in the same
/// log line shape (`classification = "Code: N"`), only the level changes.
pub fn trace_layer() -> TraceLayer<
    SharedClassifier<GrpcErrorsAsFailures>,
    DefaultMakeSpan,
    DefaultOnRequest,
    DefaultOnResponse,
    DefaultOnBodyChunk,
    DefaultOnEos,
    OnGrpcFailure,
> {
    TraceLayer::new_for_grpc().on_failure(on_grpc_failure as OnGrpcFailure)
}

fn on_grpc_failure(class: GrpcFailureClass, latency: Duration, _span: &Span) {
    let latency_ms = latency.as_millis() as u64;
    match failure_level(&class) {
        Level::ERROR => {
            tracing::error!(classification = %class, latency_ms, "response failed")
        }
        _ => tracing::debug!(classification = %class, latency_ms, "response failed"),
    }
}

fn failure_level(class: &GrpcFailureClass) -> Level {
    match class {
        GrpcFailureClass::Code(code) => match Code::from_i32(code.get()) {
            Code::Internal
            | Code::Unavailable
            | Code::Unknown
            | Code::DataLoss
            | Code::ResourceExhausted => Level::ERROR,
            Code::NotFound
            | Code::InvalidArgument
            | Code::AlreadyExists
            | Code::FailedPrecondition
            | Code::Unimplemented
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::OutOfRange => Level::DEBUG,
            // Ok/Cancelled/DeadlineExceeded/Aborted: not in either enumerated
            // bucket. Ok is unreachable here (classifier only emits Code for
            // non-Ok grpc-status). The rest default to ERROR — they typically
            // indicate the server failing to make progress.
            _ => Level::ERROR,
        },
        // Transport-level failure: the request never produced a valid
        // grpc-status. Bucket as server fault — surface to on-call.
        GrpcFailureClass::Error(_) => Level::ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroI32;
    use tonic::Code;

    fn code_class(code: Code) -> GrpcFailureClass {
        GrpcFailureClass::Code(NonZeroI32::new(code as i32).expect("non-Ok code"))
    }

    #[test]
    fn failure_level_server_fault_codes_are_error() {
        for code in [
            Code::Internal,
            Code::Unavailable,
            Code::Unknown,
            Code::DataLoss,
            Code::ResourceExhausted,
        ] {
            assert_eq!(
                failure_level(&code_class(code)),
                Level::ERROR,
                "{code:?} should bucket as ERROR (server fault)",
            );
        }
    }

    #[test]
    fn failure_level_client_driven_codes_are_debug() {
        for code in [
            Code::NotFound,
            Code::InvalidArgument,
            Code::AlreadyExists,
            Code::FailedPrecondition,
            Code::Unimplemented,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::OutOfRange,
        ] {
            assert_eq!(
                failure_level(&code_class(code)),
                Level::DEBUG,
                "{code:?} should bucket as DEBUG (client driven)",
            );
        }
    }

    #[test]
    fn failure_level_transport_error_is_error() {
        let class = GrpcFailureClass::Error("connection reset".into());
        assert_eq!(failure_level(&class), Level::ERROR);
    }

    #[test]
    fn failure_level_residual_codes_default_to_error() {
        for code in [Code::Cancelled, Code::DeadlineExceeded, Code::Aborted] {
            assert_eq!(
                failure_level(&code_class(code)),
                Level::ERROR,
                "{code:?} should default to ERROR (not in either enumerated bucket)",
            );
        }
    }
}
