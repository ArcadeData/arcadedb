#!/usr/bin/env python3
import json
import os
import tempfile
import unittest

from render_matrix import (Column, compute_badge, issue_url,
                           load_columns, load_scenarios, main, regression_url,
                           render_page, resolve_cell, unavailable_columns)

HERE = os.path.dirname(__file__)
DRIVER_VERSIONS_MD = os.path.join(HERE, "..", "driver-versions.md")
SPEC_YAML = os.path.join(HERE, "..", "spec.yaml")


class LoadColumnsTest(unittest.TestCase):
    def test_parses_all_columns_in_order(self):
        cols = load_columns(DRIVER_VERSIONS_MD)
        # 14 cells: 3 bands each for java/python/csharp/go + 2 for javascript
        # (no 4.x band - its suite needs driver-5.x client APIs).
        self.assertEqual(len(cols), 14)
        # File order: java first, go last; header/separator rows ignored.
        self.assertEqual(cols[0], Column("java", "oldest-supported-4.x", "4.4.20"))
        self.assertEqual(cols[-1], Column("go", "latest", "5.28.4"))
        self.assertNotIn(Column("javascript", "oldest-supported-4.x", "4.4.11"), cols)
        self.assertTrue(all(c.language in
            {"java", "javascript", "python", "csharp", "go"} for c in cols))


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

    def test_empty_spec_file_returns_no_scenarios(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml",
                                          delete=False) as fh:
            path = fh.name
        try:
            self.assertEqual(load_scenarios(path), [])
        finally:
            os.remove(path)


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

    def test_explicit_null_intermediate_does_not_crash(self):
        matrix = {"scenarios": {"TYPE-011": None}, "missing_cells": [],
                  "empty_cells": [], "unexpected_cells": [], "languages": ["java"],
                  "has_failures": False}
        cell = resolve_cell(self._scen(), self._col(), matrix, set(), self.REPO)
        self.assertEqual(cell.glyph, "·")

    def test_applicable_driver_versions_restricts_to_band(self):
        scen = self._scen(applicable_driver_versions=["java:latest-6.x"])
        matrix = {"scenarios": {"TYPE-011": {"java": {"6.2.0": "pass"},
                                             "go": {"5.28.4": "pass"}}},
                  "missing_cells": [], "empty_cells": [], "unexpected_cells": [],
                  "languages": ["java", "go"], "has_failures": False}
        out_of_band = resolve_cell(scen, self._col(lang="go", band="latest",
                                                    ver="5.28.4"),
                                   matrix, set(), self.REPO)
        self.assertEqual(out_of_band.glyph, "➖")
        in_band = resolve_cell(scen, self._col(lang="java", band="latest-6.x",
                                                ver="6.2.0"),
                               matrix, set(), self.REPO)
        self.assertEqual(in_band.glyph, "✅")


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
        self.assertEqual(badge["message"], "all passing")
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
        self.assertEqual(badge["message"], "all passing")

    def test_null_valued_keys_do_not_crash(self):
        # Every container key explicitly null must not raise. The exact color is
        # not asserted: a has_failures matrix with no countable fails legitimately
        # falls through rather than reading a contradictory red "0 failing".
        m = {"has_failures": True, "scenarios": None, "missing_cells": None,
             "empty_cells": None, "unexpected_cells": None, "languages": None}
        badge = compute_badge([{"current_status": "passing"}], m)
        self.assertEqual(badge["schemaVersion"], 1)
        self.assertIn(badge["color"], ("red", "yellow", "brightgreen"))

    def test_real_fail_with_null_sibling_keys_is_red(self):
        # A genuine fail cell still counts to red even when the other container
        # keys are null (exercises the or-{}/or-[] guards alongside a real fail).
        m = {"has_failures": True,
             "scenarios": {"CONN-001": {"java": {"6.2.0": "fail"}}},
             "missing_cells": None, "empty_cells": None,
             "unexpected_cells": None, "languages": None}
        badge = compute_badge([{"current_status": "passing"}], m)
        self.assertEqual(badge["color"], "red")
        self.assertEqual(badge["message"], "1 failing")

    def test_not_applicable_only_fail_does_not_read_red_zero(self):
        # If the only fail cell belongs to a not-applicable scenario it is
        # excluded from the count, and a count of 0 must not render a
        # contradictory red "0 failing" - it falls through to green.
        m = self._matrix({"ERR-003": {"java": {"6.2.0": "fail"}}},
                         has_failures=True)
        badge = compute_badge([{"id": "ERR-003",
                                "current_status": "not-applicable"}], m)
        self.assertEqual(badge["color"], "brightgreen")
        self.assertEqual(badge["message"], "all passing")

    def test_unverified_scenario_is_partial(self):
        m = self._matrix({"CONN-001": {"java": {"6.2.0": "pass"}}})
        badge = compute_badge([{"current_status": "unverified"}], m)
        self.assertEqual(badge["color"], "yellow")
        self.assertEqual(badge["message"], "partial")
        # also in the no-matrix fallback path
        fallback = compute_badge([{"current_status": "unverified"}], None)
        self.assertEqual(fallback["color"], "yellow")

    def test_empty_and_unexpected_cells_count_toward_red_total(self):
        m = self._matrix({"CONN-001": {"java": {"6.2.0": "fail"}}},
                         has_failures=True, empty_cells=["python:6.2.0"],
                         unexpected_cells=["csharp:6.2.0"])
        badge = compute_badge([{"current_status": "passing"}], m)
        self.assertEqual(badge["message"], "3 failing")


class IssueUrlTest(unittest.TestCase):
    REPO = "ArcadeData/arcadedb"

    def test_full_url_passed_through_unchanged(self):
        url = "https://github.com/x/y/issues/9"
        self.assertEqual(issue_url(self.REPO, url), url)

    def test_regression_url_contains_label(self):
        self.assertIn("label%3Abolt-compat-regression",
                      regression_url(self.REPO))


class UnavailableColumnsTest(unittest.TestCase):
    def test_combines_missing_and_empty_cells(self):
        result = unavailable_columns(
            {"missing_cells": ["go:5.28.4"], "empty_cells": ["python:6.2.0"]})
        self.assertEqual(result, {"go:5.28.4", "python:6.2.0"})

    def test_no_matrix_is_empty_set(self):
        self.assertEqual(unavailable_columns(None), set())


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

    def test_unknown_area_is_still_rendered(self):
        scenarios = self._scen() + [
            {"id": "FUT-001", "area": "future-area", "title": "new capability",
             "current_status": "passing", "tracking_issue": None,
             "known_limitation": None, "applicable_driver_versions": "all"},
        ]
        page = render_page(scenarios, self._cols(), None, repo=self.REPO,
                           run_url="", timestamp="")
        self.assertIn("## future-area", page)
        self.assertIn("**FUT-001** new capability", page)

    def test_missing_cell_column_renders_dot(self):
        matrix = {"scenarios": {"CONN-001": {"java": {"6.2.0": "pass"},
                                             "go": {"5.28.4": "pass"}}},
                  "missing_cells": ["go:5.28.4"], "empty_cells": [],
                  "unexpected_cells": [], "languages": ["java", "go"],
                  "has_failures": False}
        page = render_page(self._scen(), self._cols(), matrix, repo=self.REPO,
                           run_url="", timestamp="2026-07-08 03:00 UTC")
        lines = [line for line in page.splitlines()
                 if line.startswith("| **CONN-001**")]
        self.assertEqual(len(lines), 1)
        cells = [cell.strip() for cell in lines[0].split("|")]
        # cells: ['', 'scenario', 'java column', 'go column', '']
        self.assertEqual(cells[2], "✅")
        self.assertEqual(cells[3], "·")

    def test_coverage_gaps_section_lists_missing_cell(self):
        matrix = {"scenarios": {"CONN-001": {"java": {"6.2.0": "pass"}}},
                  "missing_cells": ["go:5.28.4"], "empty_cells": [],
                  "unexpected_cells": [], "languages": ["java", "go"],
                  "has_failures": False}
        page = render_page(self._scen(), self._cols(), matrix, repo=self.REPO,
                           run_url="", timestamp="2026-07-08 03:00 UTC")
        self.assertIn("## Coverage gaps", page)
        self.assertIn("missing (job produced no result): go:5.28.4", page)


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
                badge_text = fh.read()
            data = json.loads(badge_text)
            self.assertEqual(data["label"], "bolt drivers")
            self.assertEqual(data["color"], "brightgreen")   # today: all passing
            self.assertTrue(badge_text.endswith("\n"))


if __name__ == "__main__":
    unittest.main()
