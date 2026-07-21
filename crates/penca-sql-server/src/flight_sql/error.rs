//! Cross-library error converters used throughout [`super::service`]
//! and [`super::codec`]. Bridges between the three external error
//! types this crate handles (`ArrowError`, `FlightError`,
//! `DataFusionError`) and [`tonic::Status`], plus the reverse
//! [`status_to_flight_error`] used when wrapping a gRPC error back
//! into a Flight decode stream.
//!
//! All four wrappers stringify the source error via `{:?}` into
//! `Status::internal` (or, for the reverse direction, `FlightError::Tonic`).
//! Behavior preserved exactly from the in-service.rs originals.

use arrow_flight::error::FlightError;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use tonic::Status;

pub(super) fn arrow_error_to_status(err: ArrowError) -> Status {
    Status::internal(format!("{err:?}"))
}

pub(super) fn flight_error_to_status(err: FlightError) -> Status {
    Status::internal(format!("{err:?}"))
}

// `pub(crate)` rather than `pub(super)` — `crate::gateway` calls it
// from outside the `flight_sql` module when planning SET / SELECT
// statements for the read-path handlers.
pub(crate) fn df_error_to_status(err: DataFusionError) -> Status {
    Status::internal(format!("{err:?}"))
}

pub(super) fn status_to_flight_error(status: Status) -> FlightError {
    FlightError::Tonic(Box::new(status))
}
