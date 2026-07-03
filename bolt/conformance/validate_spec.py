#!/usr/bin/env python3
"""Validates bolt/conformance/spec.yaml structural integrity.

This is a reference/authoring aid only - no per-language Bolt e2e suite
imports or executes this script. See README.md for the consumption model.
"""
import pathlib
import sys

import yaml

REQUIRED_AREAS = {
    "connection",
    "auth",
    "transactions",
    "causal-consistency",
    "multi-database",
    "result-handling",
    "type-roundtrip",
    "errors",
    "protocol",
}
VALID_STATUSES = {"passing", "expected-fail", "not-applicable", "unverified"}
REQUIRED_LANGUAGES = {"java", "javascript", "python", "csharp", "go"}
KNOWN_GAP_TRACKING_ISSUE = "#4890"
# Areas that must each carry at least one #4890-tracked expected-fail scenario:
# connection (single-node-only ROUTE), errors (missing Neo.TransientError.*),
# protocol (no Bolt 5.x negotiation), type-roundtrip (temporal-as-string,
# missing Duration, missing Point).
REQUIRED_GAP_AREAS = {"connection", "errors", "protocol", "type-roundtrip"}


def validate(spec: dict) -> list[str]:
    errors = []

    if spec.get("version") != 1:
        errors.append("spec.version must be 1")

    bands = spec.get("driver_version_bands", {})
    missing_langs = REQUIRED_LANGUAGES - bands.keys()
    if missing_langs:
        errors.append(f"driver_version_bands missing languages: {sorted(missing_langs)}")
    for lang, band in bands.items():
        if not band.get("band_names"):
            errors.append(f"driver_version_bands.{lang} missing band_names")
        if not band.get("driver_artifact"):
            errors.append(f"driver_version_bands.{lang} missing driver_artifact")

    fixtures = spec.get("fixtures", {})
    for name in ("beer", "type_matrix"):
        if name not in fixtures:
            errors.append(f"fixtures.{name} is missing")

    scenarios = spec.get("scenarios", [])
    if not scenarios:
        errors.append("scenarios list is empty")

    seen_ids = set()
    areas_seen = set()
    gap_tracking_areas = set()

    for s in scenarios:
        sid = s.get("id")
        if not sid:
            errors.append(f"scenario missing id: {s}")
            continue
        if sid in seen_ids:
            errors.append(f"duplicate scenario id: {sid}")
        seen_ids.add(sid)

        area = s.get("area")
        if area not in REQUIRED_AREAS:
            errors.append(f"{sid}: invalid area '{area}'")
        else:
            areas_seen.add(area)

        status = s.get("current_status")
        if status not in VALID_STATUSES:
            errors.append(f"{sid}: invalid current_status '{status}'")

        if status == "expected-fail":
            if not s.get("known_limitation"):
                errors.append(f"{sid}: expected-fail requires known_limitation")
            if not s.get("tracking_issue"):
                errors.append(f"{sid}: expected-fail requires tracking_issue")
            elif s["tracking_issue"] == KNOWN_GAP_TRACKING_ISSUE:
                gap_tracking_areas.add(area)

        if not s.get("title"):
            errors.append(f"{sid}: missing title")
        if not s.get("steps"):
            errors.append(f"{sid}: missing steps")
        if "applicable_driver_versions" not in s:
            errors.append(f"{sid}: missing applicable_driver_versions")

    missing_area_coverage = REQUIRED_AREAS - areas_seen
    if missing_area_coverage:
        errors.append(f"areas with zero scenarios: {sorted(missing_area_coverage)}")

    missing_gap_areas = REQUIRED_GAP_AREAS - gap_tracking_areas
    if missing_gap_areas:
        errors.append(
            f"areas missing a #4890-tracked expected-fail scenario: {sorted(missing_gap_areas)}"
        )

    return errors


def main():
    default_path = pathlib.Path(__file__).parent / "spec.yaml"
    path = sys.argv[1] if len(sys.argv) > 1 else str(default_path)
    with open(path) as f:
        spec = yaml.safe_load(f) or {}
    errors = validate(spec)
    if errors:
        print(f"FAIL: {len(errors)} error(s)")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    areas = {s["area"] for s in spec.get("scenarios", [])}
    print(f"OK: {len(spec.get('scenarios', []))} scenarios across {len(areas)} areas")


if __name__ == "__main__":
    main()
