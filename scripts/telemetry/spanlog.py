"""Shared parsing for `PENCA_SPAN_TIMING` span-close log lines.

The one home for the wire-format primitives both telemetry summarizers
(`span_trace_table.py`, `span_window_table.py`) consume, so the two
tables can be cross-checked without parser skew: ANSI stripping, the
span-close regex, duration parsing, and span-chain name extraction.

Line shape (``tracing_subscriber::fmt`` with ``FmtSpan::CLOSE``)::

    <ts> <LEVEL> <span-chain>: <target>: close time.busy=<v><u> [time.idle=<v><u>]

Zero-dependency Python 3; imported by same-directory scripts.
"""

import re

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Level set pinned (a stray token can't masquerade as a close line);
# time.idle optional (tracing omits it for spans that never idled);
# ns included (sub-microsecond closes appear at trace level).
CLOSE_RE = re.compile(
    r"^(?P<ts>\S+)\s+(?:INFO|DEBUG|TRACE|WARN|ERROR)\s+(?P<chain>.*?):\s+"
    r"(?P<target>[\w:]+):\s+"
    r"close\s+time\.busy=(?P<busy_v>[\d.]+)(?P<busy_u>ns|µs|ms|s)"
    r"(?:\s+time\.idle=(?P<idle_v>[\d.]+)(?P<idle_u>ns|µs|ms|s))?"
)

_UNIT_TO_MS = {"ns": 1e-6, "µs": 1e-3, "ms": 1.0, "s": 1e3}


def to_ms(value: str, unit: str) -> float:
    return float(value) * _UNIT_TO_MS[unit]


def span_names(chain: str) -> list[str]:
    """Span names in a close line's chain, outermost first.

    Chain segments are ``name{fields}`` or bare ``name`` joined by ``:``.
    Walks brace depth rather than regexing so field values containing
    colons (e.g. ``db.statement=SELECT …``) cannot split a segment.
    """
    out: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in chain:
        if ch == "{":
            depth += 1
            continue

        if ch == "}":
            depth -= 1
            continue

        if depth > 0:
            continue

        if ch == ":":
            if cur:
                out.append("".join(cur))
                cur = []

            continue

        cur.append(ch)

    if cur:
        out.append("".join(cur))

    return [s.strip() for s in out if s.strip()]


def parse_close(raw_line: str) -> dict | None:
    """Parse one log line into its close-event pieces, or ``None``.

    Returns ``{ts, chain, names, target, busy_ms, idle_ms}`` with
    ``names`` outermost-first and ``idle_ms`` 0.0 when absent.
    """
    line = ANSI_RE.sub("", raw_line.rstrip("\n"))
    # Cheap pre-filter only — kept exactly as tolerant as CLOSE_RE (which
    # allows any whitespace run before `time.busy=`) so no line the regex
    # would accept is silently dropped here.
    if "time.busy=" not in line:
        return None

    m = CLOSE_RE.match(line)
    if not m:
        return None

    idle_v = m.group("idle_v")
    return {
        "ts": m.group("ts"),
        "chain": m.group("chain"),
        "names": span_names(m.group("chain")),
        "target": m.group("target"),
        "busy_ms": to_ms(m.group("busy_v"), m.group("busy_u")),
        "idle_ms": to_ms(idle_v, m.group("idle_u")) if idle_v else 0.0,
    }
