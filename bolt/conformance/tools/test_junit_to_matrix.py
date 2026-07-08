#!/usr/bin/env python3
import os
import tempfile
import unittest

from junit_to_matrix import parse_junit, parse_junit_files, build_matrix

HERE = os.path.dirname(__file__)
SAMPLE = os.path.join(HERE, "testdata", "sample-junit.xml")
UNKNOWN = os.path.join(HERE, "testdata", "unknown-id-junit.xml")
GO_STYLE = os.path.join(HERE, "testdata", "go-junit.xml")
PY_STYLE = os.path.join(HERE, "testdata", "py-junit.xml")
NS_STYLE = os.path.join(HERE, "testdata", "ns-junit.xml")
JAVA_STYLE = os.path.join(HERE, "testdata", "java-junit.xml")
LEGACY_STYLE = os.path.join(HERE, "testdata", "legacy-junit.xml")
GUARD_STYLE = os.path.join(HERE, "testdata", "guard-junit.xml")
# A path that never exists: the report a failed test step did not write.
MISSING = os.path.join(HERE, "testdata", "does-not-exist-junit.xml")
KNOWN = {"CONN-001", "AUTH-003", "TYPE-007", "ERR-002"}

# A malformed report is written at runtime rather than committed as a fixture:
# the pre-commit check-xml hook rejects a broken .xml on disk, which is exactly
# what this test needs to feed the parser.
_TMPDIR = tempfile.TemporaryDirectory()
MALFORMED = os.path.join(_TMPDIR.name, "malformed-junit.xml")
with open(MALFORMED, "w", encoding="utf-8") as _fh:
    _fh.write("this is not xml <<< totally malformed\n")


def tearDownModule():
    _TMPDIR.cleanup()


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

    def test_four_digit_and_two_digit_runs_are_not_ids(self):
        # Locks the (?!\d)/3-digit guards: a 4-digit year or a 2-digit token
        # must not be misread as a scenario id.
        self.assertEqual(parse_junit(GUARD_STYLE), {})


class BuildMatrixTest(unittest.TestCase):
    def test_shape_and_metadata(self):
        m = build_matrix([SAMPLE], "python", "6.2.0", KNOWN)
        self.assertEqual(m["language"], "python")
        self.assertEqual(m["driver_version"], "6.2.0")
        self.assertEqual(m["scenarios"]["CONN-001"], "pass")

    def test_unknown_scenario_id_dropped_not_raised(self):
        # A monitoring workflow must not lose a whole cell over one stray id.
        m = build_matrix([UNKNOWN], "python", "6.2.0", KNOWN)
        self.assertEqual(m["scenarios"], {})

    def test_no_files_yields_empty_scenarios(self):
        # A glob that matched nothing (test produced no reports) must not crash.
        m = build_matrix([], "java", "5.28.5", KNOWN)
        self.assertEqual(m["scenarios"], {})


class ParseJunitFilesTest(unittest.TestCase):
    def test_folds_split_nested_class_reports(self):
        # Failsafe may split @Nested classes into per-class files; folding the
        # Go fixture (CONN-001 pass) with the Java fixture (CONN-001 pass,
        # AUTH-003 skip) keeps every scenario and applies fail-dominates.
        combined = parse_junit_files([GO_STYLE, JAVA_STYLE])
        self.assertEqual(combined, {"CONN-001": "pass", "AUTH-003": "skip"})

    def test_missing_report_is_skipped_not_raised(self):
        # A suite whose test step failed before writing its report (e.g. a build
        # or reporter error) passes a path that does not exist; the conversion
        # must yield an empty map, not crash and drop the whole cell.
        self.assertEqual(parse_junit_files([MISSING]), {})

    def test_malformed_report_is_skipped_not_raised(self):
        # A truncated/garbled report must not crash the conversion either.
        self.assertEqual(parse_junit_files([MALFORMED]), {})

    def test_unreadable_sibling_does_not_drop_good_report(self):
        # One broken report among several must not lose the scenarios that did run.
        combined = parse_junit_files([MISSING, GO_STYLE, MALFORMED])
        self.assertEqual(combined, {"CONN-001": "pass"})


class BuildMatrixResilienceTest(unittest.TestCase):
    def test_missing_single_report_yields_empty_cell(self):
        # The JS/py/csharp/go suites pass one explicit report path; when the
        # harness produced none, build_matrix must still emit a cell (empty
        # scenarios) so merge_matrix records an empty cell, not a missing one.
        m = build_matrix([MISSING], "javascript", "4.4.11", KNOWN)
        self.assertEqual(m["language"], "javascript")
        self.assertEqual(m["driver_version"], "4.4.11")
        self.assertEqual(m["scenarios"], {})


if __name__ == "__main__":
    unittest.main()
