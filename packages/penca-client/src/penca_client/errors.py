"""Typed errors raised by the API manager layer.

Mirrors the Rust ``ApiError`` enum in ``crates/penca-api/src/error.rs``.
Servicers catch these and map them to gRPC status codes; clients surface
them as idiomatic Python exceptions.
"""


class ApiError(Exception):
    """Base class for all API-layer errors."""


class NotFoundError(ApiError):
    """A requested resource (catalog, schema, table, branch, tx, …) was not found."""


class InvalidRequestError(ApiError):
    """The request was structurally valid proto but semantically invalid.

    Examples: missing required identifier combinations, TTL exceeding the
    server maximum, contradictory options.
    """


class FailedPreconditionError(ApiError):
    """The request was valid but conflicts with current resource state.

    Examples: ``CommitTx`` on an aborted ``tx_uuid``; ``AbortTx`` on a
    ``tx_uuid`` that has already committed.
    """


class AlreadyExistsError(ApiError):
    """A namespace identifier collides with an existing resource.

    Surfaced by ``Create{Catalog,Schema,Table,Branch}`` when the
    server-side uniqueness check fires (CHA-236) — per-catalog
    ``UNIQUE(catalog_name)`` / ``UNIQUE(branch_name)`` constraints,
    plus the within-tx pre-check for schema and table names. Maps from
    gRPC ``ALREADY_EXISTS``.
    """


class AbortedError(ApiError):
    """A concurrency conflict; the operation made no changes and a retry is safe.

    Distinct from ``FailedPreconditionError``, which means the request itself was
    wrong and reissuing it unchanged will fail again. Raised when a server-side
    operation loses a lock race with a concurrent catalog operation — currently
    ``DeleteBranch``, whose teardown transaction takes catalog-wide locks and
    rolls back wholly on contention. Retrying is the caller's call: the server
    deliberately does not loop, since a retry with no backoff turns a loud
    conflict into a quiet livelock.
    """


class QueryTimeoutError(ApiError):
    """``read_data`` / ``audit_data`` ran past ``query_timeout_seconds``.

    ADR 0019: every ``Plan + Execute`` is bounded by the system-wide
    cap. On elapse the server returns gRPC ``RESOURCE_EXHAUSTED``;
    the client surfaces it as this typed exception. Retry with a
    fresh plan.
    """
