#!/usr/bin/env python3
import unittest

from validate_spec import AREA_ID_PREFIXES, validate

AREAS = [
    "connection", "auth", "transactions", "causal-consistency",
    "multi-database", "result-handling", "type-roundtrip",
]
GAP_AREAS = ["errors", "protocol", "connection", "type-roundtrip"]


def minimal_valid_spec():
    scenarios = [
        {
            "id": f"{AREA_ID_PREFIXES[area]}-001",
            "area": area,
            "title": "t",
            "fixture": "beer",
            "preconditions": [],
            "steps": ["s"],
            "applicable_driver_versions": "all",
            "current_status": "passing",
        }
        for area in AREAS
    ]
    for area in GAP_AREAS:
        scenarios.append(
            {
                "id": f"{AREA_ID_PREFIXES[area]}-900",
                "area": area,
                "title": "t",
                "steps": ["s"],
                "applicable_driver_versions": ["java:lts"],
                "current_status": "expected-fail",
                "known_limitation": "x",
                "tracking_issue": "#4890",
            }
        )
    return {
        "version": 1,
        "driver_version_bands": {
            lang: {"band_names": ["lts"], "driver_artifact": "x"}
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
        # REQUIRED_GAP_AREAS is empty once every #4890-tracked gap is closed, so the missing-gap
        # mechanism is exercised against a temporarily required area rather than the live config.
        import validate_spec
        original = validate_spec.REQUIRED_GAP_AREAS
        validate_spec.REQUIRED_GAP_AREAS = {"connection"}
        try:
            spec = minimal_valid_spec()
            spec["scenarios"] = [s for s in spec["scenarios"] if s["id"] != "CONN-900"]
            errors = validate(spec)
            self.assertTrue(any("connection" in e for e in errors), errors)
        finally:
            validate_spec.REQUIRED_GAP_AREAS = original

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

    def test_non_mapping_spec_is_flagged_not_crashed(self):
        errors = validate(["not", "a", "mapping"])
        self.assertTrue(any("spec must be a mapping" in e for e in errors), errors)

    def test_non_list_scenarios_is_flagged_not_crashed(self):
        spec = minimal_valid_spec()
        spec["scenarios"] = "not-a-list"
        errors = validate(spec)
        self.assertTrue(any("scenarios must be a list" in e for e in errors), errors)

    def test_non_mapping_scenario_is_flagged_not_crashed(self):
        spec = minimal_valid_spec()
        spec["scenarios"].append("not-a-mapping")
        errors = validate(spec)
        self.assertTrue(any("scenario must be a mapping" in e for e in errors), errors)

    def test_non_mapping_driver_version_bands_is_flagged_not_crashed(self):
        spec = minimal_valid_spec()
        spec["driver_version_bands"] = "not-a-mapping"
        errors = validate(spec)
        self.assertTrue(any("driver_version_bands must be a mapping" in e for e in errors), errors)

    def test_non_mapping_band_entry_is_flagged_not_crashed(self):
        spec = minimal_valid_spec()
        spec["driver_version_bands"]["java"] = "not-a-mapping"
        errors = validate(spec)
        self.assertTrue(any("driver_version_bands.java must be a mapping" in e for e in errors), errors)

    def test_non_mapping_fixtures_is_flagged_not_crashed(self):
        spec = minimal_valid_spec()
        spec["fixtures"] = "not-a-mapping"
        errors = validate(spec)
        self.assertTrue(any("fixtures must be a mapping" in e for e in errors), errors)

    def test_non_list_preconditions_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["preconditions"] = "not-a-list"
        errors = validate(spec)
        self.assertTrue(any("preconditions must be a list" in e for e in errors), errors)

    def test_non_list_steps_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["steps"] = "not-a-list"
        errors = validate(spec)
        self.assertTrue(any("steps must be a list" in e for e in errors), errors)

    def test_unknown_fixture_reference_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["fixture"] = "no_such_fixture"
        errors = validate(spec)
        self.assertTrue(any("no_such_fixture" in e for e in errors), errors)

    def test_id_prefix_mismatch_with_area_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["id"] = "AUTH-999"  # area is connection, prefix should be CONN
        errors = validate(spec)
        self.assertTrue(any("does not match area" in e for e in errors), errors)

    def test_malformed_id_format_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["id"] = "CONN_not_a_number"
        errors = validate(spec)
        self.assertTrue(any("does not match" in e for e in errors), errors)

    def test_driver_version_ref_unknown_language_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["applicable_driver_versions"] = ["ruby:lts"]
        errors = validate(spec)
        self.assertTrue(any("ruby" in e for e in errors), errors)

    def test_driver_version_ref_unknown_band_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["applicable_driver_versions"] = ["java:nonexistent-band"]
        errors = validate(spec)
        self.assertTrue(any("nonexistent-band" in e for e in errors), errors)

    def test_driver_version_ref_malformed_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["applicable_driver_versions"] = ["java-lts-no-colon"]
        errors = validate(spec)
        self.assertTrue(any("java-lts-no-colon" in e for e in errors), errors)


if __name__ == "__main__":
    unittest.main()
