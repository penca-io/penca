"""Render a single perf run as a static HTML report (CHA-423).

Fired at the end of every ``just perf-test`` run (no flag). Reads the run's
JSONL and the accumulated SQLite history, compares them via ``comparison``, and
writes a self-contained HTML file: per-series trend charts (history points +
this run's point projected at the end) plus a summary table of the delta vs
history. Pure stdlib templating + matplotlib (headless Agg) — no server, no
browser — so it works in CI and over SSH.

Usage:
    python scripts/perf/render_report.py --json .perf/results.jsonl \
        --db .perf/perf.db --out .perf/report.html
"""

from __future__ import annotations

import argparse
import base64
import html
import io
import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from comparison import compare_run_to_history, load_history, load_run, series_label
from metrics import format_ms, format_rate, select_headlines

# Headless backend — set before any figure is created so this runs in CI / over
# SSH with no display.
matplotlib.use("Agg")


def _delta_cell(delta_pct: float | None) -> str:
    if delta_pct is None:
        return "no-baseline"

    return f"{delta_pct:+.1f}%"


def _ms_cell(value_ms: float | None) -> str:
    return "n/a" if value_ms is None else format_ms(value_ms)


def _rate_cell(entry: dict) -> str:
    """rows/s only where row_count counts rows beyond the operation count.

    For op-counting series (row_count == operations: point-read rep loops,
    TPC-B transactions) the quotient is the operation rate — already carried
    by the ms/op column — and labeling it "rows/s" is exactly the
    queries-mislabeled-as-rows confusion CHA-438 removes.
    """
    row_count = entry.get("row_count")
    if row_count is None or row_count == entry["operations"]:
        return ""

    return format_rate(entry["run_rows_per_second"])


def _chart_png_base64(entry: dict) -> str:
    """Render one series' trend (history + this run's projected point) as a
    base64-encoded PNG, in normalized ms-per-operation."""
    history = entry["history_ms_per_ops"]

    figure, axes = plt.subplots(figsize=(6, 3))
    if history:
        axes.plot(range(len(history)), history, marker="o", label="history")

    # This run is projected at the end of the series line.
    axes.plot(
        [len(history)],
        [entry["run_ms_per_op"]],
        marker="*",
        markersize=14,
        color="crimson",
        label="this run",
    )

    # Postgres baseline, when this series recorded one: a horizontal reference
    # line (it's a single per-run value, not a trend) so the run's point reads
    # against Postgres at a glance. Series with no baseline just omit the line.
    postgres_ms_per_op = entry.get("postgres_ms_per_op")
    if postgres_ms_per_op is not None:
        axes.axhline(
            postgres_ms_per_op,
            linestyle="--",
            color="seagreen",
            label="postgres baseline",
        )

    unit = entry.get("unit")
    axes.set_ylabel(f"ms per {unit}" if unit else "ms/op")
    axes.set_title(series_label(entry), fontsize=8)
    axes.legend(fontsize=7)
    figure.tight_layout()

    buffer = io.BytesIO()
    figure.savefig(buffer, format="png")
    plt.close(figure)
    return base64.b64encode(buffer.getvalue()).decode()


def _headline_html(entries: list[dict]) -> str:
    """The lead table of externally-quotable numbers; empty when the run
    measured none of the headline operations (the section is omitted, not
    rendered as an empty table)."""
    headlines = select_headlines(entries)
    if not headlines:
        return ""

    rows = "\n".join(
        f"<tr><td>{html.escape(row['title'])}</td>"
        f"<td>{html.escape(row['penca'])}</td>"
        f"<td>{html.escape(row['postgres'] or 'n/a')}</td></tr>"
        for row in headlines
    )
    return (
        "<h2>Headline numbers</h2>\n"
        "<table border=1 cellpadding=4>\n"
        "<tr><th>Headline</th><th>Penca</th><th>Postgres</th></tr>\n"
        f"{rows}\n"
        "</table>\n"
    )


def render_html(entries: list[dict], run_id: str | None) -> str:
    """Build the self-contained HTML report string from the comparison entries."""
    body_rows = []
    charts = []
    for entry in entries:
        label = html.escape(series_label(entry))
        unit = entry.get("unit")
        body_rows.append(
            f"<tr><td>{label}</td>"
            f"<td>{html.escape(unit) if unit else ''}</td>"
            f"<td>{format_ms(entry['run_ms_per_op'])}</td>"
            f"<td>{_ms_cell(entry['history_mean_ms_per_op'])}</td>"
            f"<td>{_delta_cell(entry['delta_pct'])}</td>"
            f"<td>{_rate_cell(entry)}</td>"
            f"<td>{_ms_cell(entry['postgres_ms_per_op'])}</td>"
            f"<td>{_delta_cell(entry['postgres_delta_pct'])}</td></tr>"
        )
        charts.append(
            f"<figure><img alt='{label}'"
            f" src='data:image/png;base64,{_chart_png_base64(entry)}'></figure>"
        )

    table_body = "\n".join(body_rows) or "<tr><td colspan=8>no measurements</td></tr>"
    return (
        "<!doctype html>\n"
        '<html><head><meta charset="utf-8">'
        "<title>Penca perf run report</title></head>\n"
        "<body>\n"
        "<h1>Penca perf run report</h1>\n"
        f"<p>run_id: {html.escape(run_id or 'unknown')}</p>\n"
        f"{_headline_html(entries)}"
        "<table border=1 cellpadding=4>\n"
        "<tr><th>Series</th><th>Unit</th><th>This run (ms/op)</th>"
        "<th>History mean (ms/op)</th><th>&Delta;% vs history</th>"
        "<th>rows/s</th><th>Postgres (ms/op)</th>"
        "<th>&Delta;% vs Postgres</th></tr>\n"
        f"{table_body}\n"
        "</table>\n"
        f"{''.join(charts)}\n"
        "</body></html>\n"
    )


def write_report(json_path: str, db_path: str, out_path: str) -> None:
    """Compare the run's JSONL against history and write the HTML report.

    History excludes the run's own ``run_id`` so a recorded run (``--record``
    ingested before this step) is not compared against itself; a missing DB
    just yields no baseline.
    """
    run_rows = load_run(json_path)
    run_id = run_rows[0].get("run_id") if run_rows else None
    history = load_history(db_path, exclude_run_id=run_id)
    entries = compare_run_to_history(run_rows, history)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as handle:
        handle.write(render_html(entries, run_id))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render a perf run as static HTML.")
    parser.add_argument("--json", required=True, help="path to the run's JSONL")
    parser.add_argument("--db", default=".perf/perf.db", help="SQLite history DB")
    parser.add_argument("--out", default=".perf/report.html", help="output HTML path")
    args = parser.parse_args(argv)

    write_report(args.json, args.db, args.out)
    print(f"[perf] wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
