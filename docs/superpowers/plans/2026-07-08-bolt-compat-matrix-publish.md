# Bolt Compatibility Matrix Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish ArcadeDB's Bolt driver compatibility matrix as a committed Markdown page + a README badge, auto-generated from the nightly `bolt-compat-matrix.json` artifact plus `spec.yaml` and `driver-versions.md`.

**Architecture:** A single pure-stdlib+PyYAML Python renderer (`bolt/conformance/tools/render_matrix.py`) reads the merged nightly matrix JSON, the conformance `spec.yaml` (scenario metadata), and `driver-versions.md` (column set), and emits `bolt/conformance/COMPATIBILITY.md` + `bolt/conformance/badge.json` (a shields.io endpoint). A new `publish-matrix` job in `bolt-nightly.yml` regenerates and commits them to `main` (`[skip ci]`, only-if-changed). A fallback mode renders from `spec.yaml` `current_status` when no matrix is present, so the PR ships a real all-green baseline page immediately.

**Tech Stack:** Python 3.12, PyYAML (already a conformance dep), GitHub Actions, shields.io endpoint badge, Markdown.

## Global Constraints

- **No new dependencies.** Only the Python stdlib + `pyyaml>=6.0` (already in `bolt/conformance/requirements.txt`). Do not add anything to requirements.
- **Match the existing conformance tool style** (`junit_to_matrix.py`, `merge_matrix.py`): `#!/usr/bin/env python3` shebang, a short comment header explaining the trust/why, a one-line module docstring, `argparse`, `main(argv=None)`, `json.dump(..., indent=2, sort_keys=True)`. **No license header** on these tool files (the existing ones have none). No type annotations (the existing tools use none).
- **Tests use `unittest`** (not pytest), importing the module directly (e.g. `from render_matrix import load_columns`), runnable via `python3 -m unittest test_render_matrix.py -v` from `bolt/conformance/tools`.
- **Workflow files carry no license header** (repo convention).
- **Do not use the em dash character** (`-`) anywhere. Use a normal dash, comma, or rephrase.
- **Do not add Claude as author/co-author** on any source or workflow commit.
- **`main` branch** is the publish target; the badge/page are read from `https://raw.githubusercontent.com/ArcadeData/arcadedb/main/...`.
- **This issue only consumes** the spec/matrix/driver-versions pipeline from #4891/#5115. Do not modify `spec.yaml`, the nightly matrix jobs, `merge_matrix.py`, `junit_to_matrix.py`, or the driver bands.

## Prerequisite (run once before Task 1)

Install the conformance tooling deps so `import yaml` works locally:

```bash
pip install -r bolt/conformance/requirements.txt
```

All paths below are relative to the repo root of this worktree (`.worktrees/feat/4892-bolt-compat-matrix-publish`).

## File Structure

- Create: `bolt/conformance/tools/render_matrix.py` - the renderer (all functions below).
- Create: `bolt/conformance/tools/test_render_matrix.py` - its unit tests.
- Create: `bolt/conformance/COMPATIBILITY.md` - generated page (committed, fallback baseline in the PR).
- Create: `bolt/conformance/badge.json` - generated shields endpoint (committed).
- Modify: `README.md` - add the badge to the existing shields row.
- Modify: `.github/workflows/bolt-conformance-spec.yml` - run `test_render_matrix.py`.
- Modify: `.github/workflows/bolt-nightly.yml` - add the `publish-matrix` job.

### Module API (the whole `render_matrix.py` surface, so tasks compose)

```
Column = namedtuple("Column", "language band version")
Cell   = namedtuple("Cell", "glyph kind link")

load_columns(md_path) -> list[Column]           # ordered from driver-versions.md
load_scenarios(spec_path) -> list[dict]         # each: id, area, title, current_status,
                                                #       tracking_issue, known_limitation,
                                                #       applicable_driver_versions
issue_url(repo, tracking_issue) -> str | None   # "#4997"/"4997"/URL -> issue URL
regression_url(repo) -> str                     # nightly regression issue by label
unavailable_columns(matrix) -> set[str]         # "lang:version" with no usable data
resolve_cell(scenario, column, matrix, unavailable, repo) -> Cell
compute_badge(scenarios, matrix) -> dict         # shields endpoint schema
render_page(scenarios, columns, matrix, *, repo, run_url, timestamp) -> str
main(argv=None) -> int
```

Status vocabularies (do not conflate them):
- **matrix JSON** per-cell runtime status: `"pass"`, `"fail"`, `"skip"` (from JUnit).
- **spec.yaml** `current_status`: `"passing"`, `"expected-fail"`, `"not-applicable"`, `"unverified"`.

---

### Task 1: Module scaffold + `load_columns`

**Files:**
- Create: `bolt/conformance/tools/render_matrix.py`
- Test: `bolt/conformance/tools/test_render_matrix.py`

**Interfaces:**
- Produces: `Column = namedtuple("Column","language band version")`; `load_columns(md_path) -> list[Column]` parsing `driver-versions.md` rows `| <lang> | <band> | <version> |` in file order, keeping only the five known languages.

- [ ] **Step 1: Write the failing test**

Create `bolt/conformance/tools/test_render_matrix.py`:

```python
#!/usr/bin/env python3
import os
import unittest

from render_matrix import load_columns, Column

HERE = os.path.dirname(__file__)
DRIVER_VERSIONS_MD = os.path.join(HERE, "..", "driver-versions.md")


class LoadColumnsTest(unittest.TestCase):
    def test_parses_all_fifteen_columns_in_order(self):
        cols = load_columns(DRIVER_VERSIONS_MD)
        self.assertEqual(len(cols), 15)
        # File order: java first, go last; header/separator rows ignored.
        self.assertEqual(cols[0], Column("java", "oldest-supported-4.x", "4.4.20"))
        self.assertEqual(cols[-1], Column("go", "latest", "5.28.4"))
        self.assertTrue(all(c.language in
            {"java", "javascript", "python", "csharp", "go"} for c in cols))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'render_matrix'`.

- [ ] **Step 3: Write minimal implementation**

Create `bolt/conformance/tools/render_matrix.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/render_matrix.py bolt/conformance/tools/test_render_matrix.py
git commit -m "feat(#4892): render_matrix column parser for driver-versions.md"
```

---

### Task 2: `load_scenarios`

**Files:**
- Modify: `bolt/conformance/tools/render_matrix.py`
- Test: `bolt/conformance/tools/test_render_matrix.py`

**Interfaces:**
- Produces: `load_scenarios(spec_path) -> list[dict]`, each dict has keys `id`, `area`, `title`, `current_status` (default `"unverified"`), `tracking_issue` (default `None`), `known_limitation` (default `None`), `applicable_driver_versions` (default `"all"`), in `spec.yaml` file order.

- [ ] **Step 1: Write the failing test**

Append to `test_render_matrix.py` (add import `load_scenarios` to the existing import line):

```python
SPEC_YAML = os.path.join(HERE, "..", "spec.yaml")


class LoadScenariosTest(unittest.TestCase):
    def test_loads_scenarios_with_defaults(self):
        scen = load_scenarios(SPEC_YAML)
        self.assertGreaterEqual(len(scen), 39)
        first = scen[0]
        for key in ("id", "area", "title", "current_status",
                    "tracking_issue", "known_limitation",
                    "applicable_driver_versions"):
            self.assertIn(key, first)
        # ERR-003 is the one not-applicable scenario today.
        err003 = next(s for s in scen if s["id"] == "ERR-003")
        self.assertEqual(err003["current_status"], "not-applicable")
        self.assertIsNotNone(err003["known_limitation"])
        # No scenario carries a tracking_issue today; default is None.
        self.assertTrue(all(s["tracking_issue"] is None for s in scen))
        self.assertTrue(all(s["applicable_driver_versions"] == "all" for s in scen))
```

Update the import line to:

```python
from render_matrix import load_columns, load_scenarios, Column
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: FAIL with `ImportError: cannot import name 'load_scenarios'`.

- [ ] **Step 3: Write minimal implementation**

Append to `render_matrix.py`:

```python
def load_scenarios(spec_path):
    """Ordered scenario dicts from spec.yaml, with defaulted optional fields."""
    with open(spec_path, encoding="utf-8") as fh:
        spec = yaml.safe_load(fh)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/render_matrix.py bolt/conformance/tools/test_render_matrix.py
git commit -m "feat(#4892): load conformance scenarios from spec.yaml"
```

---

### Task 3: Cell resolution (`resolve_cell` + helpers + glyphs)

**Files:**
- Modify: `bolt/conformance/tools/render_matrix.py`
- Test: `bolt/conformance/tools/test_render_matrix.py`

**Interfaces:**
- Produces: `GLYPH` dict; `Cell` namedtuple `(glyph, kind, link)`; `issue_url(repo, tracking_issue)`, `regression_url(repo)`, `unavailable_columns(matrix)`, and `resolve_cell(scenario, column, matrix, unavailable, repo) -> Cell`.
- Consumes: `Column`, scenario dicts from Task 2, matrix dict shape `{"scenarios":{sid:{lang:{ver:status}}}, "missing_cells":[...], "empty_cells":[...], "unexpected_cells":[...], "has_failures":bool, "languages":[...]}`.

Resolution rules (implement exactly):
1. Scenario `current_status == "not-applicable"` -> `➖` for every column.
2. Scenario restricted (`applicable_driver_versions != "all"`) and `"{language}:{band}"` not listed -> `➖`.
3. `matrix is None` (fallback) -> baseline from `current_status`: `passing`->`✅`, `expected-fail`->`⚠️` (linked), `not-applicable`->`➖`, else `⚪`.
4. Column `"{language}:{version}"` in `unavailable` (missing/empty) -> `·` (no misleading baseline).
5. Runtime `pass`->`✅`, `fail`->`❌` (link `tracking_issue` else regression), `skip`->`⚠️` if `expected-fail` else `⚪` (link `tracking_issue` if any).
6. Runtime absent but column available -> `·` (ran, this scenario not reported).

- [ ] **Step 1: Write the failing test**

Append to `test_render_matrix.py` (extend the import line to include the new names):

```python
class ResolveCellTest(unittest.TestCase):
    REPO = "ArcadeData/arcadedb"

    def _scen(self, **kw):
        base = {"id": "TYPE-011", "area": "type-roundtrip", "title": "Duration",
                "current_status": "passing", "tracking_issue": None,
                "known_limitation": None, "applicable_driver_versions": "all"}
        base.update(kw)
        return base

    def _col(self, lang="java", band="latest-6.x", ver="6.2.0"):
        return Column(lang, band, ver)

    def _matrix(self, status):
        return {"scenarios": {"TYPE-011": {"java": {"6.2.0": status}}},
                "missing_cells": [], "empty_cells": [], "unexpected_cells": [],
                "languages": ["java"], "has_failures": status == "fail"}

    def test_runtime_pass(self):
        cell = resolve_cell(self._scen(), self._col(),
                            self._matrix("pass"), set(), self.REPO)
        self.assertEqual(cell.glyph, "✅")
        self.assertIsNone(cell.link)

    def test_runtime_fail_links_tracking_issue(self):
        scen = self._scen(current_status="expected-fail", tracking_issue="#4997")
        cell = resolve_cell(scen, self._col(), self._matrix("fail"), set(), self.REPO)
        self.assertEqual(cell.glyph, "❌")
        self.assertEqual(cell.link,
                         "https://github.com/ArcadeData/arcadedb/issues/4997")

    def test_runtime_fail_without_issue_links_regression(self):
        cell = resolve_cell(self._scen(), self._col(),
                            self._matrix("fail"), set(), self.REPO)
        self.assertEqual(cell.glyph, "❌")
        self.assertIn("label%3Abolt-compat-regression", cell.link)

    def test_not_applicable_scenario_is_dash(self):
        scen = self._scen(current_status="not-applicable")
        cell = resolve_cell(scen, self._col(), self._matrix("skip"), set(), self.REPO)
        self.assertEqual(cell.glyph, "➖")

    def test_expected_fail_skip_is_warning_linked(self):
        scen = self._scen(current_status="expected-fail", tracking_issue="4998")
        cell = resolve_cell(scen, self._col(), self._matrix("skip"), set(), self.REPO)
        self.assertEqual(cell.glyph, "⚠️")
        self.assertEqual(cell.link,
                         "https://github.com/ArcadeData/arcadedb/issues/4998")

    def test_unavailable_column_is_dot(self):
        cell = resolve_cell(self._scen(), self._col(),
                            self._matrix("pass"), {"java:6.2.0"}, self.REPO)
        self.assertEqual(cell.glyph, "·")

    def test_absent_runtime_available_column_is_dot(self):
        matrix = {"scenarios": {"OTHER-001": {"java": {"6.2.0": "pass"}}},
                  "missing_cells": [], "empty_cells": [], "unexpected_cells": [],
                  "languages": ["java"], "has_failures": False}
        cell = resolve_cell(self._scen(), self._col(), matrix, set(), self.REPO)
        self.assertEqual(cell.glyph, "·")

    def test_fallback_baseline_passing(self):
        cell = resolve_cell(self._scen(), self._col(), None, set(), self.REPO)
        self.assertEqual(cell.glyph, "✅")

    def test_fallback_baseline_not_applicable(self):
        scen = self._scen(current_status="not-applicable")
        cell = resolve_cell(scen, self._col(), None, set(), self.REPO)
        self.assertEqual(cell.glyph, "➖")
```

Extend the import line to:

```python
from render_matrix import (Cell, Column, load_columns, load_scenarios,
                           resolve_cell)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: FAIL with `ImportError: cannot import name 'resolve_cell'`.

- [ ] **Step 3: Write minimal implementation**

Append to `render_matrix.py`:

```python
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
    runtime = (matrix.get("scenarios", {}).get(scenario["id"], {})
               .get(column.language, {}).get(column.version))
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: PASS (all ResolveCellTest cases + prior tests).

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/render_matrix.py bolt/conformance/tools/test_render_matrix.py
git commit -m "feat(#4892): cell resolution combining runtime status and spec metadata"
```

---

### Task 4: `compute_badge`

**Files:**
- Modify: `bolt/conformance/tools/render_matrix.py`
- Test: `bolt/conformance/tools/test_render_matrix.py`

**Interfaces:**
- Produces: `compute_badge(scenarios, matrix) -> dict` returning `{"schemaVersion":1,"label":"bolt drivers","message":<str>,"color":<str>}`.

Rules (implement exactly):
- Matrix present and `has_failures` true -> **red**, message `"{k} failing"` where `k` = count of `fail` cells + `len(missing_cells)` + `len(empty_cells)` + `len(unexpected_cells)`.
- Otherwise (no real failures): **yellow** message `"partial"` if any scenario `current_status == "expected-fail"`, else **brightgreen** message `"{L}/{L} passing"` where `L` = `len(matrix["languages"])` (or 5 in fallback). `not-applicable` scenarios never change the color.

- [ ] **Step 1: Write the failing test**

Append to `test_render_matrix.py` (add `compute_badge` to the import line):

```python
class ComputeBadgeTest(unittest.TestCase):
    def _matrix(self, scenarios, **extra):
        base = {"scenarios": scenarios, "missing_cells": [], "empty_cells": [],
                "unexpected_cells": [], "languages": ["java", "javascript",
                "python", "csharp", "go"], "has_failures": False}
        base.update(extra)
        return base

    def test_all_pass_is_green(self):
        m = self._matrix({"CONN-001": {"java": {"6.2.0": "pass"}}})
        badge = compute_badge([{"current_status": "passing"}], m)
        self.assertEqual(badge["color"], "brightgreen")
        self.assertEqual(badge["message"], "5/5 passing")
        self.assertEqual(badge["label"], "bolt drivers")
        self.assertEqual(badge["schemaVersion"], 1)

    def test_not_applicable_stays_green(self):
        m = self._matrix({"CONN-001": {"java": {"6.2.0": "pass"}}})
        badge = compute_badge(
            [{"current_status": "passing"}, {"current_status": "not-applicable"}], m)
        self.assertEqual(badge["color"], "brightgreen")

    def test_expected_fail_is_yellow(self):
        m = self._matrix({"CONN-001": {"java": {"6.2.0": "pass"}}})
        badge = compute_badge([{"current_status": "expected-fail"}], m)
        self.assertEqual(badge["color"], "yellow")
        self.assertEqual(badge["message"], "partial")

    def test_failures_are_red_with_count(self):
        m = self._matrix({"CONN-001": {"java": {"6.2.0": "fail"}}},
                         has_failures=True, missing_cells=["go:5.28.4"])
        badge = compute_badge([{"current_status": "passing"}], m)
        self.assertEqual(badge["color"], "red")
        self.assertEqual(badge["message"], "2 failing")

    def test_fallback_no_matrix_is_green(self):
        badge = compute_badge([{"current_status": "passing"}], None)
        self.assertEqual(badge["color"], "brightgreen")
        self.assertEqual(badge["message"], "5/5 passing")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: FAIL with `ImportError: cannot import name 'compute_badge'`.

- [ ] **Step 3: Write minimal implementation**

Append to `render_matrix.py`:

```python
def _badge(message, color):
    return {"schemaVersion": 1, "label": "bolt drivers",
            "message": message, "color": color}


def compute_badge(scenarios, matrix):
    """shields.io endpoint dict summarizing the whole matrix."""
    languages = 5
    if matrix is not None:
        languages = len(matrix.get("languages", [])) or 5
        if matrix.get("has_failures"):
            fails = 0
            for langs in matrix.get("scenarios", {}).values():
                for versions in langs.values():
                    fails += sum(1 for status in versions.values()
                                 if status == "fail")
            fails += len(matrix.get("missing_cells", []))
            fails += len(matrix.get("empty_cells", []))
            fails += len(matrix.get("unexpected_cells", []))
            return _badge(f"{fails} failing", "red")
    if any(s["current_status"] == "expected-fail" for s in scenarios):
        return _badge("partial", "yellow")
    return _badge(f"{languages}/{languages} passing", "brightgreen")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/render_matrix.py bolt/conformance/tools/test_render_matrix.py
git commit -m "feat(#4892): badge status computation with green/yellow/red thresholds"
```

---

### Task 5: `render_page`

**Files:**
- Modify: `bolt/conformance/tools/render_matrix.py`
- Test: `bolt/conformance/tools/test_render_matrix.py`

**Interfaces:**
- Produces: `render_page(scenarios, columns, matrix, *, repo, run_url, timestamp) -> str` returning the full `COMPATIBILITY.md` markdown: a generated-file banner, title, intro, a Last-verified line (timestamp+run link, or "pending first nightly" when `matrix is None`), a legend, and one Markdown table per non-empty area (columns `lang<br>version`, rows `**ID** title`), with `known_limitation` blockquote footnotes per area.

- [ ] **Step 1: Write the failing test**

Append to `test_render_matrix.py` (add `render_page` to the import line):

```python
class RenderPageTest(unittest.TestCase):
    REPO = "ArcadeData/arcadedb"

    def _cols(self):
        return [Column("java", "latest-6.x", "6.2.0"),
                Column("go", "latest", "5.28.4")]

    def _scen(self):
        return [
            {"id": "CONN-001", "area": "connection", "title": "bolt:// connects",
             "current_status": "passing", "tracking_issue": None,
             "known_limitation": None, "applicable_driver_versions": "all"},
            {"id": "ERR-003", "area": "errors", "title": "unauth forbidden",
             "current_status": "not-applicable", "tracking_issue": None,
             "known_limitation": "Not reachable via any official driver API.",
             "applicable_driver_versions": "all"},
        ]

    def test_page_has_header_columns_and_cells(self):
        matrix = {"scenarios": {"CONN-001": {"java": {"6.2.0": "pass"},
                                             "go": {"5.28.4": "pass"}}},
                  "missing_cells": [], "empty_cells": [], "unexpected_cells": [],
                  "languages": ["java", "go"], "has_failures": False}
        page = render_page(self._scen(), self._cols(), matrix, repo=self.REPO,
                           run_url="https://example/run/1", timestamp="2026-07-08 03:00 UTC")
        self.assertIn("# Bolt Driver Compatibility Matrix", page)
        self.assertIn("**Last verified:** 2026-07-08 03:00 UTC", page)
        self.assertIn("https://example/run/1", page)
        self.assertIn("java<br>6.2.0", page)
        self.assertIn("go<br>5.28.4", page)
        self.assertIn("## connection", page)
        self.assertIn("**CONN-001** bolt:// connects", page)
        self.assertIn("✅", page)
        self.assertIn("➖", page)                      # ERR-003 not-applicable
        self.assertIn("Not reachable via any official driver API.", page)  # footnote
        self.assertTrue(page.endswith("\n"))

    def test_fallback_page_marks_pending(self):
        page = render_page(self._scen(), self._cols(), None, repo=self.REPO,
                           run_url="", timestamp="")
        self.assertIn("pending first nightly", page)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: FAIL with `ImportError: cannot import name 'render_page'`.

- [ ] **Step 3: Write minimal implementation**

Append to `render_matrix.py`:

```python
AREAS = ["connection", "auth", "transactions", "causal-consistency",
         "multi-database", "result-handling", "type-roundtrip", "errors",
         "protocol"]

LEGEND = ("Legend: ✅ pass, ❌ fail, ⚠️ expected-fail / known limitation, "
          "➖ not applicable, ⚪ skipped, `·` not reported.")


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

    header = "| Scenario | " + " | ".join(
        f"{c.language}<br>{c.version}" for c in columns) + " |"
    separator = "|" + "---|" * (len(columns) + 1)

    by_area = {}
    for scenario in scenarios:
        by_area.setdefault(scenario["area"], []).append(scenario)

    for area in AREAS:
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/render_matrix.py bolt/conformance/tools/test_render_matrix.py
git commit -m "feat(#4892): render the COMPATIBILITY.md page by area"
```

---

### Task 6: `main` CLI (file IO + fallback wiring)

**Files:**
- Modify: `bolt/conformance/tools/render_matrix.py`
- Test: `bolt/conformance/tools/test_render_matrix.py`

**Interfaces:**
- Produces: `main(argv=None) -> int`. Args: `--matrix` (optional path), `--spec` (required), `--versions` (required), `--repo` (default `ArcadeData/arcadedb`), `--run-url` (default ""), `--timestamp` (default ""), `--out-page` (required), `--out-badge` (required). Writes the page and the pretty-printed badge JSON (trailing newline). An unreadable/absent `--matrix` degrades to fallback with a stderr warning.

- [ ] **Step 1: Write the failing test**

Append to `test_render_matrix.py` (add `main` to the import line; add `import json`, `import tempfile` at top):

```python
class MainTest(unittest.TestCase):
    def test_writes_page_and_badge_in_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            page = os.path.join(tmp, "COMPATIBILITY.md")
            badge = os.path.join(tmp, "badge.json")
            rc = main(["--spec", SPEC_YAML, "--versions", DRIVER_VERSIONS_MD,
                       "--out-page", page, "--out-badge", badge])
            self.assertEqual(rc, 0)
            with open(page, encoding="utf-8") as fh:
                text = fh.read()
            self.assertIn("# Bolt Driver Compatibility Matrix", text)
            self.assertIn("pending first nightly", text)
            with open(badge, encoding="utf-8") as fh:
                data = json.load(fh)
            self.assertEqual(data["label"], "bolt drivers")
            self.assertEqual(data["color"], "brightgreen")   # today: all passing
            self.assertTrue(open(badge, encoding="utf-8").read().endswith("\n"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: FAIL with `ImportError: cannot import name 'main'`.

- [ ] **Step 3: Write minimal implementation**

Append to `render_matrix.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/render_matrix.py bolt/conformance/tools/test_render_matrix.py
git commit -m "feat(#4892): render_matrix CLI entrypoint with fallback mode"
```

---

### Task 7: Generate & commit the initial page + badge; add the README badge

**Files:**
- Create: `bolt/conformance/COMPATIBILITY.md` (generated)
- Create: `bolt/conformance/badge.json` (generated)
- Modify: `README.md`

**Interfaces:**
- Consumes: the `main()` CLI from Task 6.

- [ ] **Step 1: Generate the initial artifacts in fallback mode**

Run from repo root:

```bash
python3 bolt/conformance/tools/render_matrix.py \
  --spec bolt/conformance/spec.yaml \
  --versions bolt/conformance/driver-versions.md \
  --repo ArcadeData/arcadedb \
  --out-page bolt/conformance/COMPATIBILITY.md \
  --out-badge bolt/conformance/badge.json
```

- [ ] **Step 2: Verify the generated output**

Run: `sed -n '1,20p' bolt/conformance/COMPATIBILITY.md && echo '---' && cat bolt/conformance/badge.json`
Expected: the page shows the title, "pending first nightly", the legend, and per-area tables; `badge.json` is `{"color":"brightgreen","label":"bolt drivers","message":"5/5 passing","schemaVersion":1}` (today's data is all-passing).

- [ ] **Step 3: Add the badge to the README shields row**

In `README.md`, inside the `<p align="center">` badge block, add one entry after the existing DeepWiki badge line (`<a href="https://deepwiki.com/...">...</a>` then `&nbsp;`). Insert:

```html
  <a href="bolt/conformance/COMPATIBILITY.md"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ArcadeData/arcadedb/main/bolt/conformance/badge.json" alt="Bolt drivers"></a>
  &nbsp;
```

- [ ] **Step 4: Commit**

```bash
git add bolt/conformance/COMPATIBILITY.md bolt/conformance/badge.json README.md
git commit -m "docs(#4892): initial Bolt compatibility matrix page and README badge"
```

---

### Task 8: CI wiring (spec-validation test + nightly publish job)

**Files:**
- Modify: `.github/workflows/bolt-conformance-spec.yml`
- Modify: `.github/workflows/bolt-nightly.yml`

**Interfaces:**
- Consumes: `render_matrix.py`, `test_render_matrix.py`, `bolt-compat-matrix` artifact produced by the `merge-matrix` job.

- [ ] **Step 1: Add the renderer unit tests to the spec-validation workflow**

In `.github/workflows/bolt-conformance-spec.yml`, in the "Run conformance tools unit tests" step, append the render test after the two existing lines:

```yaml
      - name: Run conformance tools unit tests
        working-directory: bolt/conformance/tools
        run: |
          python3 -m unittest test_junit_to_matrix.py -v
          python3 -m unittest test_merge_matrix.py -v
          python3 -m unittest test_render_matrix.py -v
```

- [ ] **Step 2: Add the `publish-matrix` job to the nightly workflow**

In `.github/workflows/bolt-nightly.yml`, append this job at the end of the `jobs:` map (after `resolve-regression`). Keep the pinned action SHAs identical to the ones already used elsewhere in this file:

```yaml
  publish-matrix:
    needs: merge-matrix
    if: ${{ always() }}
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0
        with:
          ref: main
          fetch-depth: 0
      - name: Set up Python
        uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6.3.0
        with:
          python-version: "3.12"
      - name: Install conformance tooling deps
        run: pip install -r bolt/conformance/requirements.txt
      - name: Download compatibility matrix
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        continue-on-error: true
        with:
          name: bolt-compat-matrix
      - name: Render page and badge
        run: |
          matrix_arg=""
          if [ -f bolt-compat-matrix.json ]; then
            matrix_arg="--matrix bolt-compat-matrix.json"
          fi
          python3 bolt/conformance/tools/render_matrix.py \
            $matrix_arg \
            --spec bolt/conformance/spec.yaml \
            --versions bolt/conformance/driver-versions.md \
            --repo "${{ github.repository }}" \
            --run-url "${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}" \
            --timestamp "$(date -u +'%Y-%m-%d %H:%M UTC')" \
            --out-page bolt/conformance/COMPATIBILITY.md \
            --out-badge bolt/conformance/badge.json
      - name: Commit refreshed matrix if changed
        run: |
          if git diff --quiet -- bolt/conformance/COMPATIBILITY.md bolt/conformance/badge.json; then
            echo "No matrix changes to publish."
            exit 0
          fi
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add bolt/conformance/COMPATIBILITY.md bolt/conformance/badge.json
          git commit -m "docs(bolt): refresh driver compatibility matrix [skip ci]"
          git push origin HEAD:main
```

- [ ] **Step 3: Lint the workflows**

Run: `actionlint .github/workflows/bolt-nightly.yml .github/workflows/bolt-conformance-spec.yml`
Expected: no new findings for the added `publish-matrix` job or the edited test step. (If `actionlint` is not installed: `docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest -color .github/workflows/bolt-nightly.yml`.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/bolt-conformance-spec.yml .github/workflows/bolt-nightly.yml
git commit -m "ci(#4892): run render tests and publish the compatibility matrix nightly"
```

---

### Task 9: End-to-end dry-run verification + open the PR

**Files:** none (verification + PR).

**Interfaces:**
- Consumes: everything above; the committed sample JUnit in `bolt/conformance/tools/testdata/`.

- [ ] **Step 1: Full pipeline dry-run against real sample JUnit**

Reproduce the nightly path locally (sample JUnit -> matrix -> page) to prove the three tools compose. Run from repo root:

```bash
cd bolt/conformance/tools
python3 junit_to_matrix.py testdata/sample-junit.xml \
  --language java --driver-version 6.2.0 --spec ../spec.yaml -o /tmp/cell-java.json
python3 merge_matrix.py /tmp/cell-java.json --expect-from ../driver-versions.md -o /tmp/matrix.json
python3 render_matrix.py --matrix /tmp/matrix.json --spec ../spec.yaml \
  --versions ../driver-versions.md --run-url https://example/run/1 \
  --timestamp "2026-07-08 03:00 UTC" \
  --out-page /tmp/COMPATIBILITY.md --out-badge /tmp/badge.json
cd ../../..
sed -n '1,15p' /tmp/COMPATIBILITY.md && echo '---' && cat /tmp/badge.json
```

Expected: a rendered page with the "Last verified: 2026-07-08 03:00 UTC ([run])" line and a `badge.json`. (A one-language partial matrix will legitimately show `·` for the other columns and a red `N failing` badge because `merge_matrix` flags the missing cells - that is correct; it only proves composition, not a green run.)

- [ ] **Step 2: Run the whole renderer test suite once more**

Run: `cd bolt/conformance/tools && python3 -m unittest test_render_matrix.py -v && cd ../../..`
Expected: PASS, all tests.

- [ ] **Step 3: Push the branch and open the PR**

```bash
git push -u origin feat/4892-bolt-compat-matrix-publish
```

Then open the PR (via `gh` or the GitHub MCP) with:
- Title: `docs(#4892): publish the Bolt driver compatibility matrix (page + badge)`
- Body: what it adds (renderer, page, badge, README badge, nightly publish job), how it works (consumes #4891/#5115's `bolt-compat-matrix.json` + `spec.yaml`, fallback baseline), verification (unit tests in `bolt-conformance-spec.yml`, dry-run), `Closes #4892`, and a note that this completes epic #4882 Group D so **#4882 can be closed** by the maintainer.
- End the PR body with the Claude Code generation footer (PR bodies only, not code commits).

- [ ] **Step 4: Post-PR review loop**

Run the review cycle at least four times: wait for Claude and Gemini reviews on the PR, then respond to each using the `superpowers:receiving-code-review` skill (verify each point technically before acting; do not perform agreement). Between cycles, keep the tracking doc lean (Symptom/Root cause/Fix/Tests/Impact) and never commit session tracking files to the repo root.

---

## Self-Review

**Spec coverage** (each spec section -> task):
- AC1 "matrix page covering scenario x driver x version" -> Tasks 5, 7 (page rendered from all 15 columns x all scenarios).
- AC2 "badge generated and linked from README" -> Tasks 4, 7.
- AC3 "generation automated from CI artifacts" -> Task 8 (`publish-matrix` job); "last verified" timestamp -> Task 5.
- AC4 "every non-passing cell links to its limitation issue" -> Task 3 (`resolve_cell` links `tracking_issue`, falls back to the regression issue).
- Fallback/bootstrap -> Tasks 3, 6, 7.
- Badge not-applicable-excluded rule -> Task 4 (`test_not_applicable_stays_green`).
- Verification plan -> Tasks 1-6 unit tests, Task 9 dry-run, Task 8 `actionlint`.
- Closure of #4892 / epic #4882 -> Task 9 PR body.

**Placeholder scan:** none. Every code step shows complete code; every command has an expected result.

**Type consistency:** `Column(language, band, version)` and `Cell(glyph, kind, link)` are used identically across Tasks 1, 3, 5. `resolve_cell(scenario, column, matrix, unavailable, repo)`, `compute_badge(scenarios, matrix)`, and `render_page(..., repo=, run_url=, timestamp=)` signatures match between their defining task, their tests, and their callers in `render_page`/`main`. The matrix JSON shape (`scenarios`/`missing_cells`/`empty_cells`/`unexpected_cells`/`languages`/`has_failures`) is consistent with `merge_matrix.py`'s output.
