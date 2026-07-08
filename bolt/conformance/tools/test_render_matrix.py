#!/usr/bin/env python3
import os
import unittest

from render_matrix import (Cell, Column, load_columns, load_scenarios,
                           resolve_cell)

HERE = os.path.dirname(__file__)
DRIVER_VERSIONS_MD = os.path.join(HERE, "..", "driver-versions.md")
SPEC_YAML = os.path.join(HERE, "..", "spec.yaml")


class LoadColumnsTest(unittest.TestCase):
    def test_parses_all_fifteen_columns_in_order(self):
        cols = load_columns(DRIVER_VERSIONS_MD)
        self.assertEqual(len(cols), 15)
        # File order: java first, go last; header/separator rows ignored.
        self.assertEqual(cols[0], Column("java", "oldest-supported-4.x", "4.4.20"))
        self.assertEqual(cols[-1], Column("go", "latest", "5.28.4"))
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


if __name__ == "__main__":
    unittest.main()
