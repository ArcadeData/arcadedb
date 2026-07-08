#!/usr/bin/env python3
import unittest

from merge_matrix import merge


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


if __name__ == "__main__":
    unittest.main()
