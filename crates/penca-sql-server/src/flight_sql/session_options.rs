//! Flight SQL `SetSessionOptions` / `GetSessionOptions` action handlers.
//!
//! ## Wire protocol
//!
//! Per the Flight SQL spec and the arrow-go reference client
//! (`apache/arrow-go arrow/flight/client.go::handleAction`), session
//! option actions ride on the standard `DoAction` RPC with:
//!
//! - `Action.type` = `"SetSessionOptions"` / `"GetSessionOptions"`
//! - `Action.body` = raw [`proto::SetSessionOptionsRequest`] /
//!   [`proto::GetSessionOptionsRequest`] bytes (**not** wrapped in
//!   `google.protobuf.Any`, unlike Flight SQL's own action family
//!   which is `Any`-wrapped).
//!
//! Responses mirror that: the result's `body` field carries raw
//! [`proto::SetSessionOptionsResult`] / [`proto::GetSessionOptionsResult`]
//! bytes.
//!
//! ## Routing
//!
//! arrow-flight 57.3's [`FlightSqlService`] trait doesn't expose
//! `do_action_set_session_options` / `do_action_get_session_options`
//! hooks — its dispatch ladder ([upstream `server.rs:868`])
//! recognizes only the FlightSQL action types and routes everything
//! else to `do_action_fallback`. Our override there intercepts the
//! two new types, decodes the body, and delegates to
//! [`crate::set::plan_set_option`] (the canonical session-mutation
//! dispatcher shared with the SQL `SET` path).
//!
//! ## Two-phase apply for multi-key requests
//!
//! `SetSessionOptions` requests carry a `map<string, SessionOptionValue>`
//! and the spec allows partial success. The handler walks every entry
//! twice: a planning pass (no side effects) followed by an apply pass.
//! That ordering is load-bearing — if a single key returns
//! [`crate::set::SetOptionError::Rejected`] (e.g. catalog mismatch
//! mid-session), the handler short-circuits with `Status::failed_precondition`
//! *before* any other key's mutation lands. Without the split, a
//! request like `{db_schema: "sales", catalog: "wrong"}` could write
//! `default_schema = "sales"` (HashMap iteration order is unspecified)
//! before failing on `catalog`, leaving the session inconsistent with
//! what the client thinks just happened.

use std::collections::HashMap;

use datafusion::execution::context::SessionContext;
use prost::Message;
use prost::bytes::Bytes;
use tonic::Status;

use crate::session::SessionSnapshot;
use crate::set::{SetOptionError, SetOptionPlan, SetOptionSurface, apply_plan, plan_set_option};

/// Generated proto types for the vendored Flight SQL SessionOptions
/// subset. See `protos/flight_sql/session_options.proto` and the
/// `build.rs` next door.
#[allow(clippy::enum_variant_names)] // upstream proto names (StringValue, BoolValue, …)
pub(crate) mod proto {
    tonic::include_proto!("penca.flight_sql.v1");
}

/// `Action.type` strings the Flight SQL spec assigns to the
/// session-options actions. Match against `request.r#type` in
/// `do_action_fallback`.
pub(crate) const SET_SESSION_OPTIONS_ACTION_TYPE: &str = "SetSessionOptions";
pub(crate) const GET_SESSION_OPTIONS_ACTION_TYPE: &str = "GetSessionOptions";

/// Decode a `SetSessionOptions` action body, plan every entry against
/// the session's state, and apply the resulting mutations.
///
/// Two-phase: collect a `Vec<(key, SetOptionPlan)>` over the request's
/// keys first, then apply. A `Rejected` outcome on any key bails the
/// whole request with `Status::failed_precondition` before any
/// mutation lands. Non-fatal failures (`InvalidName` / `InvalidValue`)
/// accumulate into the result's per-key `errors` map per the Flight
/// SQL spec, and successful keys produce no entry there.
///
/// Returns the encoded `SetSessionOptionsResult` bytes (suitable for
/// wrapping in `arrow_flight::Result.body`).
#[tracing::instrument(
    skip_all,
    fields(
        catalog_uuid = %snapshot.catalog_uuid,
        branch_uuid = %snapshot.branch_uuid,
        key_count = tracing::field::Empty,
    ),
    err,
)]
pub(crate) fn handle_set_session_options(
    snapshot: &SessionSnapshot,
    ctx: &SessionContext,
    body: &Bytes,
) -> Result<Bytes, Status> {
    let request = proto::SetSessionOptionsRequest::decode(body.as_ref()).map_err(|e| {
        Status::invalid_argument(format!(
            "SetSessionOptions: malformed request body — could not decode \
             SetSessionOptionsRequest: {e}"
        ))
    })?;
    tracing::Span::current().record("key_count", request.session_options.len());

    let mut errors: HashMap<String, proto::set_session_options_result::Error> =
        HashMap::with_capacity(request.session_options.len());
    let mut plans: Vec<SetOptionPlan> = Vec::with_capacity(request.session_options.len());

    // Phase 1: plan every key. Schema mutations are gathered into
    // `plans`; per-key errors accumulate into `errors`; the first
    // `Rejected` short-circuits with no side effects.
    for (key, proto_value) in request.session_options {
        let value = match extract_string_session_option_value(&proto_value) {
            Some(s) => s,
            None => {
                errors.insert(
                    key,
                    error(proto::set_session_options_result::ErrorValue::InvalidValue),
                );
                continue;
            }
        };
        match plan_set_option(snapshot, SetOptionSurface::Wire, &key, value) {
            Ok(plan) => plans.push(plan),
            Err(SetOptionError::InvalidName) => {
                errors.insert(
                    key,
                    error(proto::set_session_options_result::ErrorValue::InvalidName),
                );
            }
            Err(SetOptionError::Rejected(msg)) => {
                // Short-circuit the per-key error mechanism: the Go
                // ADBC driver discards per-key messages (it only
                // surfaces the key name + a generic
                // "invalid name / value / error" string), so a
                // `Rejected` outcome here would lose the
                // handshake-pinned wording that tells the JDBC tool
                // *why* the option can't be set. Surfacing it as a
                // gRPC status flows through the driver's
                // non-nil-error path, which preserves the message
                // verbatim. Because the planning pass is read-only,
                // returning here also guarantees no sibling key's
                // mutation has been applied to the session.
                return Err(Status::failed_precondition(msg));
            }
        }
    }

    // Phase 2: apply every planned mutation. `apply_plan` is infallible —
    // `WriteSchema` is the only mutation, because catalog binding is
    // handshake-only.
    for plan in plans {
        apply_plan(ctx, plan);
    }

    let result = proto::SetSessionOptionsResult { errors };
    Ok(Bytes::from(result.encode_to_vec()))
}

/// Decode a `GetSessionOptions` action body, read the session's
/// current routing knobs (catalog + default_schema) from the snapshot
/// and the cached `SessionContext`, and encode them as a
/// `GetSessionOptionsResult.session_options` map.
///
/// Three keys are emitted for the schema knob (`db_schema`, `schema`,
/// and `search_path`) so the client driver gets a hit on whichever
/// alias it expects to read back. Catalog is keyed `catalog`.
pub(crate) fn handle_get_session_options(
    snapshot: &SessionSnapshot,
    ctx: &SessionContext,
    body: &Bytes,
) -> Result<Bytes, Status> {
    // Body is `GetSessionOptionsRequest` which carries no fields; we
    // decode it strictly to surface a clear error on malformed input
    // rather than silently accepting random bytes.
    proto::GetSessionOptionsRequest::decode(body.as_ref()).map_err(|e| {
        Status::invalid_argument(format!(
            "GetSessionOptions: malformed request body — could not decode \
             GetSessionOptionsRequest: {e}"
        ))
    })?;

    let default_schema = ctx.state().config_options().catalog.default_schema.clone();
    let mut session_options = HashMap::with_capacity(4);
    session_options.insert(
        "catalog".to_string(),
        string_value(snapshot.catalog_name.clone()),
    );
    session_options.insert(
        "db_schema".to_string(),
        string_value(default_schema.clone()),
    );
    session_options.insert("schema".to_string(), string_value(default_schema.clone()));
    session_options.insert("search_path".to_string(), string_value(default_schema));

    let result = proto::GetSessionOptionsResult { session_options };
    Ok(Bytes::from(result.encode_to_vec()))
}

/// Pull the string payload out of a Flight SQL `SessionOptionValue`
/// `oneof`. Returns `None` for empty oneofs (the spec reserves that
/// for "unset", which we don't yet implement) and for non-string
/// variants — every Penca-known knob is string-valued today, so the
/// wire boundary flags anything else as a per-key `INVALID_VALUE`
/// rather than carrying type-tagged values through the dispatcher.
fn extract_string_session_option_value(value: &proto::SessionOptionValue) -> Option<&str> {
    match &value.option_value {
        Some(proto::session_option_value::OptionValue::StringValue(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn error(
    value: proto::set_session_options_result::ErrorValue,
) -> proto::set_session_options_result::Error {
    proto::set_session_options_result::Error {
        value: value as i32,
    }
}

fn string_value(s: String) -> proto::SessionOptionValue {
    proto::SessionOptionValue {
        option_value: Some(proto::session_option_value::OptionValue::StringValue(s)),
    }
}
