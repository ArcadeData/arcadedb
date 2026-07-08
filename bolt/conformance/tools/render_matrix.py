#!/usr/bin/env python3
# Renders the published Bolt compatibility matrix (issue #4892, epic #4882
# Group D) from the nightly bolt-compat-matrix.json plus spec.yaml metadata and
# the driver-versions.md column set. Emits COMPATIBILITY.md (human page) and
# badge.json (shields.io endpoint). With no matrix (the PR bootstrap, or a
# nightly whose merge produced nothing) it falls back to each scenario's
# spec.yaml current_status so the page is always renderable.
"""Render the Bolt driver compatibility matrix page and badge."""
import argparse
import json
import re
import sys
from collections import namedtuple

import yaml

Column = namedtuple("Column", "language band version")
Cell = namedtuple("Cell", "glyph kind link")

LANGUAGES = {"java", "javascript", "python", "csharp", "go"}
# driver-versions.md rows: | <language> | <band> | <version> |
COLUMN_ROW_RE = re.compile(r"^\|\s*([a-z]+)\s*\|\s*([\w.-]+)\s*\|\s*([\w.]+)\s*\|")


def load_columns(md_path):
    """Ordered (language, band, version) columns from driver-versions.md."""
    columns = []
    with open(md_path, encoding="utf-8") as fh:
        for line in fh:
            match = COLUMN_ROW_RE.match(line)
            if match and match.group(1) in LANGUAGES:
                columns.append(Column(match.group(1), match.group(2), match.group(3)))
    return columns


def load_scenarios(spec_path):
    """Ordered scenario dicts from spec.yaml, with defaulted optional fields."""
    with open(spec_path, encoding="utf-8") as fh:
        spec = yaml.safe_load(fh) or {}
    scenarios = []
    for entry in spec.get("scenarios", []):
        scenarios.append({
            "id": entry["id"],
            "area": entry["area"],
            "title": entry["title"],
            "current_status": entry.get("current_status", "unverified"),
            "tracking_issue": entry.get("tracking_issue"),
            "known_limitation": entry.get("known_limitation"),
            "applicable_driver_versions": entry.get("applicable_driver_versions", "all"),
        })
    return scenarios


GLYPH = {
    "pass": "✅",
    "fail": "❌",
    "expected-fail": "⚠️",
    "not-applicable": "➖",
    "skip": "⚪",
    "unavailable": "·",
    "unreported": "·",
}


def issue_url(repo, tracking_issue):
    """Normalize a tracking_issue (#NNN / NNN / full URL) to a GitHub issue URL."""
    if not tracking_issue:
        return None
    text = str(tracking_issue).strip()
    if text.startswith("http"):
        return text
    return f"https://github.com/{repo}/issues/{text.lstrip('#')}"


def regression_url(repo):
    """Stable link to the nightly regression issue, matched by its label."""
    return (f"https://github.com/{repo}/issues?q=is%3Aissue+is%3Aopen"
            "+label%3Abolt-compat-regression")


def unavailable_columns(matrix):
    """The 'lang:version' columns with no usable data (missing or empty cells)."""
    if not matrix:
        return set()
    return set(matrix.get("missing_cells", [])) | set(matrix.get("empty_cells", []))


def _baseline_cell(scenario, repo):
    """Fallback cell derived purely from spec.yaml current_status."""
    status = scenario["current_status"]
    if status == "passing":
        return Cell(GLYPH["pass"], "baseline-pass", None)
    if status == "expected-fail":
        return Cell(GLYPH["expected-fail"], "expected-fail",
                    issue_url(repo, scenario["tracking_issue"]))
    if status == "not-applicable":
        return Cell(GLYPH["not-applicable"], "not-applicable", None)
    return Cell(GLYPH["skip"], "unverified", None)


def resolve_cell(scenario, column, matrix, unavailable, repo):
    """Resolve one scenario x column into a display Cell (glyph, kind, link)."""
    status = scenario["current_status"]
    applicable = scenario["applicable_driver_versions"]
    if status == "not-applicable":
        return Cell(GLYPH["not-applicable"], "not-applicable", None)
    if applicable != "all" and f"{column.language}:{column.band}" not in applicable:
        return Cell(GLYPH["not-applicable"], "not-applicable", None)
    if matrix is None:
        return _baseline_cell(scenario, repo)
    if f"{column.language}:{column.version}" in unavailable:
        return Cell(GLYPH["unavailable"], "unavailable", None)
    runtime = (((matrix.get("scenarios") or {}).get(scenario["id"]) or {})
               .get(column.language) or {}).get(column.version)
    if runtime == "pass":
        return Cell(GLYPH["pass"], "pass", None)
    if runtime == "fail":
        link = issue_url(repo, scenario["tracking_issue"]) or regression_url(repo)
        return Cell(GLYPH["fail"], "fail", link)
    if runtime == "skip":
        if status == "expected-fail":
            return Cell(GLYPH["expected-fail"], "expected-fail",
                        issue_url(repo, scenario["tracking_issue"]))
        return Cell(GLYPH["skip"], "skip",
                    issue_url(repo, scenario["tracking_issue"]))
    return Cell(GLYPH["unreported"], "unreported", None)


def _badge(message, color):
    return {"schemaVersion": 1, "label": "bolt drivers",
            "message": message, "color": color}


def compute_badge(scenarios, matrix):
    """shields.io endpoint dict summarizing the whole matrix."""
    if matrix is not None and matrix.get("has_failures"):
        na = {s["id"] for s in scenarios if s["current_status"] == "not-applicable"}
        fails = 0
        for sid, langs in (matrix.get("scenarios") or {}).items():
            if sid in na:
                continue
            for versions in (langs or {}).values():
                fails += sum(1 for status in (versions or {}).values()
                             if status == "fail")
        fails += len(matrix.get("missing_cells") or [])
        fails += len(matrix.get("empty_cells") or [])
        fails += len(matrix.get("unexpected_cells") or [])
        return _badge(f"{fails} failing", "red")
    if any(s["current_status"] == "expected-fail" for s in scenarios):
        return _badge("partial", "yellow")
    return _badge("all passing", "brightgreen")


AREAS = ["connection", "auth", "transactions", "causal-consistency",
         "multi-database", "result-handling", "type-roundtrip", "errors",
         "protocol"]

LEGEND = ("Legend: ✅ pass, ❌ fail, ⚠️ expected-fail / known limitation, "
          "➖ not applicable, ⚪ skipped, `·` not reported. A `·` in a listed "
          "Coverage-gaps column means no result for that driver:version.")


def _cell_md(cell):
    return f"[{cell.glyph}]({cell.link})" if cell.link else cell.glyph


def render_page(scenarios, columns, matrix, *, repo, run_url, timestamp):
    """Assemble the full COMPATIBILITY.md markdown string."""
    unavailable = unavailable_columns(matrix)
    lines = [
        "<!-- GENERATED by bolt/conformance/tools/render_matrix.py "
        "(issue #4892). Do not edit by hand. -->",
        "",
        "# Bolt Driver Compatibility Matrix",
        "",
        "Certification status of ArcadeDB's Bolt protocol against every official "
        "Neo4j driver, per the shared conformance spec "
        "([`spec.yaml`](spec.yaml), epic #4882). Columns are driver language by "
        "pinned version ([`driver-versions.md`](driver-versions.md)).",
        "",
    ]
    if matrix is None:
        lines.append("**Last verified:** pending first nightly run "
                     "(baseline from `spec.yaml` `current_status`).")
    else:
        suffix = f" ([run]({run_url}))" if run_url else ""
        lines.append(f"**Last verified:** {timestamp}{suffix}")
    lines += ["", LEGEND, ""]

    if matrix is not None:
        missing = matrix.get("missing_cells") or []
        empty = matrix.get("empty_cells") or []
        unexpected = matrix.get("unexpected_cells") or []
        if missing or empty or unexpected:
            lines += ["## Coverage gaps", "",
                      "These driver:version cells produced no usable result "
                      "this run and count against the badge:", ""]
            if missing:
                lines.append(
                    f"- missing (job produced no result): {', '.join(missing)}")
            if empty:
                lines.append(
                    f"- empty (ran, no recognized scenarios): {', '.join(empty)}")
            if unexpected:
                lines.append(
                    f"- unexpected (ran, not in driver-versions.md): {', '.join(unexpected)}")
            lines.append("")

    header = "| Scenario | " + " | ".join(
        f"{c.language}<br>{c.version}" for c in columns) + " |"
    separator = "|" + "---|" * (len(columns) + 1)

    by_area = {}
    for scenario in scenarios:
        by_area.setdefault(scenario["area"], []).append(scenario)

    ordered_areas = list(AREAS) + sorted(set(by_area) - set(AREAS))
    for area in ordered_areas:
        rows = by_area.get(area, [])
        if not rows:
            continue
        lines += [f"## {area}", "", header, separator]
        for scenario in rows:
            cells = [_cell_md(resolve_cell(scenario, column, matrix,
                                           unavailable, repo))
                     for column in columns]
            lines.append(f"| **{scenario['id']}** {scenario['title']} | "
                         + " | ".join(cells) + " |")
        lines.append("")
        notes = [s for s in rows if s.get("known_limitation")]
        for scenario in notes:
            text = " ".join(scenario["known_limitation"].split())
            lines.append(f"> **{scenario['id']}**: {text}")
        if notes:
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", default="",
                        help="bolt-compat-matrix.json; omit for spec.yaml baseline")
    parser.add_argument("--spec", required=True, help="path to spec.yaml")
    parser.add_argument("--versions", required=True, help="path to driver-versions.md")
    parser.add_argument("--repo", default="ArcadeData/arcadedb")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--timestamp", default="")
    parser.add_argument("--out-page", required=True)
    parser.add_argument("--out-badge", required=True)
    args = parser.parse_args(argv)

    matrix = None
    if args.matrix:
        try:
            with open(args.matrix, encoding="utf-8") as fh:
                matrix = json.load(fh)
        except (OSError, ValueError) as err:
            print(f"warning: matrix unreadable ({err}); using spec.yaml baseline",
                  file=sys.stderr)
            matrix = None

    scenarios = load_scenarios(args.spec)
    columns = load_columns(args.versions)
    page = render_page(scenarios, columns, matrix, repo=args.repo,
                       run_url=args.run_url,
                       timestamp=args.timestamp or "unknown")
    badge = compute_badge(scenarios, matrix)
    with open(args.out_page, "w", encoding="utf-8") as fh:
        fh.write(page)
    with open(args.out_badge, "w", encoding="utf-8") as fh:
        json.dump(badge, fh, indent=2, sort_keys=True)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
