#!/usr/bin/env python3
"""Validates bolt/conformance/spec.yaml structural integrity.

This is a reference/authoring aid only - no per-language Bolt e2e suite
imports or executes this script. See README.md for the consumption model.
"""
import pathlib
import re
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
# Areas that must each carry at least one #4890-tracked expected-fail scenario.
# All #4890-tracked gaps are now closed and flipped to passing: connection
# (CONN-004, HA-aware ROUTE), errors (ERR-002), type-roundtrip (TYPE-003/011/012),
# and protocol (PROTO-002) each previously appeared here as the only #4890-tracked
# gap in their area. None remain, so this set is empty.
REQUIRED_GAP_AREAS = set()
# Canonical AREA-NNN id prefix per area - not derivable from the area string
# (e.g. transactions -> TX, multi-database -> MDB, causal-consistency ->
# CAUSAL), so scenario ids are checked against this table rather than a
# generic slugify of the area name.
AREA_ID_PREFIXES = {
    "connection": "CONN",
    "auth": "AUTH",
    "transactions": "TX",
    "causal-consistency": "CAUSAL",
    "multi-database": "MDB",
    "result-handling": "RESULT",
    "type-roundtrip": "TYPE",
    "errors": "ERR",
    "protocol": "PROTO",
}


def validate(spec: dict) -> list[str]:
    if not isinstance(spec, dict):
        return ["spec must be a mapping"]

    errors = []

    if spec.get("version") != 1:
        errors.append("spec.version must be 1")

    bands = spec.get("driver_version_bands", {})
    if not isinstance(bands, dict):
        errors.append("driver_version_bands must be a mapping")
        bands = {}
    missing_langs = REQUIRED_LANGUAGES - bands.keys()
    if missing_langs:
        errors.append(f"driver_version_bands missing languages: {sorted(missing_langs)}")
    for lang, band in bands.items():
        if not isinstance(band, dict):
            errors.append(f"driver_version_bands.{lang} must be a mapping")
            continue
        if not band.get("band_names"):
            errors.append(f"driver_version_bands.{lang} missing band_names")
        if not band.get("driver_artifact"):
            errors.append(f"driver_version_bands.{lang} missing driver_artifact")

    fixtures = spec.get("fixtures", {})
    if not isinstance(fixtures, dict):
        errors.append("fixtures must be a mapping")
        fixtures = {}
    for name in ("beer", "type_matrix"):
        if name not in fixtures:
            errors.append(f"fixtures.{name} is missing")

    scenarios = spec.get("scenarios", [])
    if not isinstance(scenarios, list):
        errors.append("scenarios must be a list")
        scenarios = []
    elif not scenarios:
        errors.append("scenarios list is empty")

    seen_ids = set()
    areas_seen = set()
    gap_tracking_areas = set()

    for s in scenarios:
        if not isinstance(s, dict):
            errors.append(f"scenario must be a mapping, got: {s!r}")
            continue

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
            prefix = AREA_ID_PREFIXES[area]
            if not re.match(rf"^{re.escape(prefix)}-\d+$", sid):
                errors.append(f"{sid}: id does not match area '{area}' (expected prefix '{prefix}-NNN')")

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

        preconditions = s.get("preconditions")
        if preconditions is not None and not isinstance(preconditions, list):
            errors.append(f"{sid}: preconditions must be a list")

        steps = s.get("steps")
        if not steps:
            errors.append(f"{sid}: missing steps")
        elif not isinstance(steps, list):
            errors.append(f"{sid}: steps must be a list")

        fixture = s.get("fixture")
        if fixture and fixture not in fixtures:
            errors.append(f"{sid}: unknown fixture reference '{fixture}'")

        adv = s.get("applicable_driver_versions")
        if adv is None:
            errors.append(f"{sid}: missing applicable_driver_versions")
        elif adv != "all":
            if not isinstance(adv, list):
                errors.append(f"{sid}: applicable_driver_versions must be 'all' or a list")
            else:
                for ref in adv:
                    lang, _, band = ref.partition(":") if isinstance(ref, str) else ("", "", "")
                    if not lang or not band:
                        errors.append(f"{sid}: malformed driver version ref '{ref}' (expected 'language:band_name')")
                    elif lang not in bands:
                        errors.append(f"{sid}: driver version ref '{ref}' has unknown language '{lang}'")
                    elif isinstance(bands[lang], dict) and band not in bands[lang].get("band_names", []):
                        errors.append(f"{sid}: driver version ref '{ref}' has unknown band '{band}' for language '{lang}'")

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
