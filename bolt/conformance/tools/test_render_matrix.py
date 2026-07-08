#!/usr/bin/env python3
import os
import unittest

from render_matrix import load_columns, load_scenarios, Column

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


if __name__ == "__main__":
    unittest.main()
