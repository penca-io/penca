"""Client-side gRPC status translation.

Mirror of ``penca_server_grpc.status`` — where the server maps
``ApiError`` subclasses onto ``grpc.StatusCode`` values, this module does
the inverse: takes a ``grpc.RpcError`` raised by a stub call and returns
the matching ``ApiError`` subclass so callers see a typed Python
exception rather than a bare gRPC error.
"""

from __future__ import annotations

from grpc import RpcError, StatusCode

from penca_client.errors import (
    AbortedError,
    AlreadyExistsError,
    ApiError,
    FailedPreconditionError,
    InvalidRequestError,
    NotFoundError,
    QueryTimeoutError,
)


def rpc_error_to_api_error(err: RpcError) -> Exception:
    """Convert a gRPC ``RpcError`` into an idiomatic Python exception.

    Most statuses map to an ``ApiError`` subclass; ``UNIMPLEMENTED``
    maps to Python's built-in ``NotImplementedError`` so callers can
    tell "not wired up yet" from "internal failure".

    The concrete call objects grpcio raises inherit from both
    ``RpcError`` and ``Call``; ``Call`` is what provides ``code()`` /
    ``details()``, but the type stubs only expose the bare ``RpcError``
    base.
    """
    code = err.code()  # ty: ignore[unresolved-attribute]
    detail = err.details() or str(err)  # ty: ignore[unresolved-attribute]
    if code == StatusCode.INVALID_ARGUMENT:
        return InvalidRequestError(detail)

    if code == StatusCode.NOT_FOUND:
        return NotFoundError(detail)

    if code == StatusCode.FAILED_PRECONDITION:
        return FailedPreconditionError(detail)

    if code == StatusCode.ALREADY_EXISTS:
        return AlreadyExistsError(detail)

    if code == StatusCode.UNIMPLEMENTED:
        return NotImplementedError(detail)

    # ADR 0019 / CHA-233: `read_data` / `audit_data` past
    # ``query_timeout_seconds`` surfaces as ``RESOURCE_EXHAUSTED``;
    # mirror the Rust ``ApiError::QueryTimeout`` variant so callers
    # can catch a typed Python exception.
    # ABORTED is the concurrency-conflict code: the operation made no changes,
    # so reissuing is safe. Mapped to its own class so a caller can retry it
    # without also swallowing genuine failures.
    if code == StatusCode.ABORTED:
        return AbortedError(detail)

    if code == StatusCode.RESOURCE_EXHAUSTED:
        return QueryTimeoutError(detail)

    return ApiError(detail)
