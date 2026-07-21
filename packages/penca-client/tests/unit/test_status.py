"""Unit tests for the gRPC RpcError → ApiError translation."""

from __future__ import annotations

import pytest
from penca_client.errors import (
    ApiError,
    FailedPreconditionError,
    InvalidRequestError,
    NotFoundError,
    QueryTimeoutError,
)
from penca_client.status import rpc_error_to_api_error
from grpc import RpcError, StatusCode


class _FakeRpcError(RpcError):
    """grpcio's concrete error inherits both RpcError and Call; Call is
    what provides code() / details(). Stub here matches that surface."""

    def __init__(self, code: StatusCode, detail: str) -> None:
        self._code = code
        self._detail = detail

    def code(self) -> StatusCode:
        return self._code

    def details(self) -> str:
        return self._detail


@pytest.mark.parametrize(
    ("code", "expected_type"),
    [
        (StatusCode.INVALID_ARGUMENT, InvalidRequestError),
        (StatusCode.NOT_FOUND, NotFoundError),
        (StatusCode.FAILED_PRECONDITION, FailedPreconditionError),
        (StatusCode.UNIMPLEMENTED, NotImplementedError),
        (StatusCode.RESOURCE_EXHAUSTED, QueryTimeoutError),
    ],
)
def test_known_codes_map_to_typed_exception(
    code: StatusCode, expected_type: type[Exception]
) -> None:
    err = _FakeRpcError(code, "boom")
    out = rpc_error_to_api_error(err)
    assert isinstance(out, expected_type)
    assert "boom" in str(out)


def test_unknown_code_falls_back_to_api_error() -> None:
    err = _FakeRpcError(StatusCode.INTERNAL, "everything is on fire")
    out = rpc_error_to_api_error(err)
    assert type(out) is ApiError
    assert "everything is on fire" in str(out)


def test_empty_details_falls_back_to_str_repr() -> None:
    """``RpcError.details()`` returning an empty string should not produce
    an empty exception message — the fallback uses ``str(err)``."""

    class _ErrWithRepr(_FakeRpcError):
        def __str__(self) -> str:
            return "<rpc-error-repr>"

    err = _ErrWithRepr(StatusCode.INTERNAL, "")
    out = rpc_error_to_api_error(err)
    assert "<rpc-error-repr>" in str(out)
