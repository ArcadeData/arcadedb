#!/usr/bin/env python3
import unittest

from validate_spec import validate

AREAS = [
    "connection", "auth", "transactions", "causal-consistency",
    "multi-database", "result-handling", "type-roundtrip",
]
GAP_AREAS = ["errors", "protocol", "connection", "type-roundtrip"]


def minimal_valid_spec():
    scenarios = [
        {
            "id": f"{area.upper().replace('-', '')}-001",
            "area": area,
            "title": "t",
            "steps": ["s"],
            "applicable_driver_versions": "all",
            "current_status": "passing",
        }
        for area in AREAS
    ]
    for area in GAP_AREAS:
        scenarios.append(
            {
                "id": f"{area.upper().replace('-', '')}-900",
                "area": area,
                "title": "t",
                "steps": ["s"],
                "applicable_driver_versions": "all",
                "current_status": "expected-fail",
                "known_limitation": "x",
                "tracking_issue": "#4890",
            }
        )
    return {
        "version": 1,
        "driver_version_bands": {
            lang: {"band_names": ["a"], "driver_artifact": "x"}
            for lang in ("java", "javascript", "python", "csharp", "go")
        },
        "fixtures": {"beer": {}, "type_matrix": {}},
        "scenarios": scenarios,
    }


class ValidateSpecTest(unittest.TestCase):
    def test_minimal_valid_spec_passes(self):
        self.assertEqual(validate(minimal_valid_spec()), [])

    def test_missing_area_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"] = [s for s in spec["scenarios"] if s["area"] != "auth"]
        errors = validate(spec)
        self.assertTrue(any("auth" in e for e in errors), errors)

    def test_duplicate_id_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"].append(dict(spec["scenarios"][0]))
        errors = validate(spec)
        self.assertTrue(any("duplicate scenario id" in e for e in errors), errors)

    def test_invalid_status_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["current_status"] = "kinda-works"
        errors = validate(spec)
        self.assertTrue(any("current_status" in e for e in errors), errors)

    def test_expected_fail_without_known_limitation_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["current_status"] = "expected-fail"
        errors = validate(spec)
        self.assertTrue(any("known_limitation" in e for e in errors), errors)

    def test_expected_fail_without_tracking_issue_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["current_status"] = "expected-fail"
        spec["scenarios"][0]["known_limitation"] = "x"
        errors = validate(spec)
        self.assertTrue(any("tracking_issue" in e for e in errors), errors)

    def test_missing_gap_tracking_area_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"] = [s for s in spec["scenarios"] if s["id"] != "TYPEROUNDTRIP-900"]
        errors = validate(spec)
        self.assertTrue(
            any("type-roundtrip" in e for e in errors), errors
        )

    def test_missing_driver_language_is_flagged(self):
        spec = minimal_valid_spec()
        del spec["driver_version_bands"]["go"]
        errors = validate(spec)
        self.assertTrue(any("go" in e for e in errors), errors)

    def test_missing_fixture_is_flagged(self):
        spec = minimal_valid_spec()
        del spec["fixtures"]["type_matrix"]
        errors = validate(spec)
        self.assertTrue(any("type_matrix" in e for e in errors), errors)


if __name__ == "__main__":
    unittest.main()
