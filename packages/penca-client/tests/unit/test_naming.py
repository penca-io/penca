"""Golden test for the format wire-code contract.

Mirrors the Rust `penca_core::format::wire_code_goldens` test. The
`{1: lance, 2: parquet}` mapping is a stable cross-language storage
contract; since the `StorageFormat` proto enum was removed (CHA-445) the
Rust enum and this Python map are now independent hardcoded mirrors, so
pin the codes here too — a one-sided renumber then fails loudly in CI
rather than as silent on-disk corruption.
"""

from __future__ import annotations

from penca_client.naming import (
    FORMAT_EXTENSIONS,
    format_from_text,
    format_to_text,
)


def test_format_wire_codes_are_stable() -> None:
    assert FORMAT_EXTENSIONS == {1: "lance", 2: "parquet"}


def test_format_text_round_trip() -> None:
    for code, text in ((1, "lance"), (2, "parquet")):
        assert format_to_text(code) == text
        assert format_from_text(text) == code
