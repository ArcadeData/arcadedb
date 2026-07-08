#!/usr/bin/env python3
import os
import unittest

from merge_matrix import merge, load_expected_cells

HERE = os.path.dirname(__file__)
DRIVER_VERSIONS_MD = os.path.join(HERE, "..", "driver-versions.md")


class MergeTest(unittest.TestCase):
    def _cell(self, lang, ver, scen):
        return {"language": lang, "driver_version": ver, "scenarios": scen}

    def test_nests_scenario_language_version(self):
        a = self._cell("python", "6.2.0", {"CONN-001": "pass", "ERR-002": "fail"})
        b = self._cell("go", "5.28.4", {"CONN-001": "pass"})
        out = merge([a, b])
        self.assertEqual(out["scenarios"]["CONN-001"]["python"]["6.2.0"], "pass")
        self.assertEqual(out["scenarios"]["CONN-001"]["go"]["5.28.4"], "pass")
        self.assertEqual(out["scenarios"]["ERR-002"]["python"]["6.2.0"], "fail")

    def test_languages_sorted_and_deduped(self):
        out = merge([self._cell("go", "1", {}), self._cell("java", "2", {}), self._cell("go", "3", {})])
        self.assertEqual(out["languages"], ["go", "java"])

    def test_has_failures_flag(self):
        self.assertTrue(merge([self._cell("py", "1", {"X-001": "fail"})])["has_failures"])
        self.assertFalse(merge([self._cell("py", "1", {"X-001": "pass"})])["has_failures"])
        self.assertFalse(merge([self._cell("py", "1", {"X-001": "skip"})])["has_failures"])

    def test_missing_version_cell_flags_failure(self):
        # python:6.2.0 ran, python:5.28.4 died with no artifact -> still flagged
        # even though the python language is present via its other version.
        out = merge([self._cell("python", "6.2.0", {"CONN-001": "pass"})],
                    expected_cells=["python:6.2.0", "python:5.28.4", "go:5.28.4"])
        self.assertEqual(out["missing_cells"], ["go:5.28.4", "python:5.28.4"])
        self.assertTrue(out["has_failures"])

    def test_no_cells_with_expectations_is_all_missing(self):
        out = merge([], expected_cells=["python:6.2.0", "go:5.28.4"])
        self.assertEqual(out["missing_cells"], ["go:5.28.4", "python:6.2.0"])
        self.assertTrue(out["has_failures"])
        self.assertEqual(out["scenarios"], {})

    def test_empty_cell_is_coverage_floor(self):
        # A cell that ran but yielded zero recognized scenarios (e.g. the id
        # never reached the JUnit test name) must not read as a pass.
        out = merge([self._cell("csharp", "6.2.1", {})],
                    expected_cells=["csharp:6.2.1"])
        self.assertEqual(out["empty_cells"], ["csharp:6.2.1"])
        self.assertTrue(out["has_failures"])

    def test_unexpected_cell_flags_drift(self):
        out = merge([self._cell("go", "9.9.9", {"CONN-001": "pass"})],
                    expected_cells=["go:5.28.4"])
        self.assertEqual(out["unexpected_cells"], ["go:9.9.9"])
        self.assertTrue(out["has_failures"])


class LoadExpectedCellsTest(unittest.TestCase):
    def test_parses_driver_versions_md(self):
        cells = load_expected_cells(DRIVER_VERSIONS_MD)
        # Sanity: the five languages and a couple of known pins are present, and
        # the header/separator rows are not misread as cells.
        self.assertIn("java:4.4.20", cells)
        self.assertIn("go:5.28.4", cells)
        self.assertEqual(len(cells), 15)
        self.assertTrue(all(":" in c for c in cells))


if __name__ == "__main__":
    unittest.main()
