#!/usr/bin/env python3
import os
import unittest

from junit_to_matrix import parse_junit, build_matrix

HERE = os.path.dirname(__file__)
SAMPLE = os.path.join(HERE, "testdata", "sample-junit.xml")
UNKNOWN = os.path.join(HERE, "testdata", "unknown-id-junit.xml")
GO_STYLE = os.path.join(HERE, "testdata", "go-junit.xml")
PY_STYLE = os.path.join(HERE, "testdata", "py-junit.xml")
NS_STYLE = os.path.join(HERE, "testdata", "ns-junit.xml")
JAVA_STYLE = os.path.join(HERE, "testdata", "java-junit.xml")
LEGACY_STYLE = os.path.join(HERE, "testdata", "legacy-junit.xml")
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

    def test_underscore_separated_id_normalizes_to_hyphen(self):
        # Go test funcs are named Test_CONN_001_...; the id must normalize to CONN-001.
        result = parse_junit(GO_STYLE)
        self.assertEqual(result, {"CONN-001": "pass"})

    def test_python_lowercase_prefix_id(self):
        # pytest emits name="test_CONN_001_connect"; the id must still resolve.
        result = parse_junit(PY_STYLE)
        self.assertEqual(result, {"CONN-001": "pass"})

    def test_java_failsafe_method_name_form(self):
        # Maven Failsafe writes the method name (conn001_boltScheme), not the
        # @DisplayName, so the id must be recovered from the lowercase form.
        result = parse_junit(JAVA_STYLE)
        self.assertEqual(result, {"CONN-001": "pass", "AUTH-003": "skip"})

    def test_java_legacy_method_names_embed_id(self):
        # RemoteBoltLegacyDriverIT methods must embed the id (conn001_legacy...)
        # so the java/4.4.20 cell is not permanently empty.
        result = parse_junit(LEGACY_STYLE)
        self.assertEqual(result, {"CONN-001": "pass", "PROTO-001": "pass", "ERR-001": "pass"})

    def test_namespaced_junit_document(self):
        result = parse_junit(NS_STYLE)
        self.assertEqual(result, {"CONN-001": "pass"})


class BuildMatrixTest(unittest.TestCase):
    def test_shape_and_metadata(self):
        m = build_matrix(SAMPLE, "python", "6.2.0", KNOWN)
        self.assertEqual(m["language"], "python")
        self.assertEqual(m["driver_version"], "6.2.0")
        self.assertEqual(m["scenarios"]["CONN-001"], "pass")

    def test_unknown_scenario_id_dropped_not_raised(self):
        # A monitoring workflow must not lose a whole cell over one stray id.
        m = build_matrix(UNKNOWN, "python", "6.2.0", KNOWN)
        self.assertEqual(m["scenarios"], {})


if __name__ == "__main__":
    unittest.main()
