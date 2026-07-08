#!/usr/bin/env python3
import os
import unittest

from junit_to_matrix import parse_junit, build_matrix

HERE = os.path.dirname(__file__)
SAMPLE = os.path.join(HERE, "testdata", "sample-junit.xml")
UNKNOWN = os.path.join(HERE, "testdata", "unknown-id-junit.xml")
KNOWN = {"CONN-001", "AUTH-003", "TYPE-007", "ERR-002"}


class ParseJunitTest(unittest.TestCase):
    def test_maps_status_by_child_element(self):
        result = parse_junit(SAMPLE)
        self.assertEqual(result["CONN-001"], "pass")
        self.assertEqual(result["AUTH-003"], "fail")
        self.assertEqual(result["TYPE-007"], "skip")
        self.assertEqual(result["ERR-002"], "fail")

    def test_extracts_id_from_testcase_name_prefix(self):
        result = parse_junit(SAMPLE)
        self.assertEqual(set(result.keys()), KNOWN)


class BuildMatrixTest(unittest.TestCase):
    def test_shape_and_metadata(self):
        m = build_matrix(SAMPLE, "python", "6.2.0", KNOWN)
        self.assertEqual(m["language"], "python")
        self.assertEqual(m["driver_version"], "6.2.0")
        self.assertEqual(m["scenarios"]["CONN-001"], "pass")

    def test_unknown_scenario_id_raises(self):
        with self.assertRaises(ValueError):
            build_matrix(UNKNOWN, "python", "6.2.0", KNOWN)


if __name__ == "__main__":
    unittest.main()
