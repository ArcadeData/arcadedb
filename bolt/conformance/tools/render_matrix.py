#!/usr/bin/env python3
# Renders the published Bolt compatibility matrix (issue #4892, epic #4882
# Group D) from the nightly bolt-compat-matrix.json plus spec.yaml metadata and
# the driver-versions.md column set. Emits COMPATIBILITY.md (human page) and
# badge.json (shields.io endpoint). With no matrix (the PR bootstrap, or a
# nightly whose merge produced nothing) it falls back to each scenario's
# spec.yaml current_status so the page is always renderable.
"""Render the Bolt driver compatibility matrix page and badge."""
import argparse
import json
import re
import sys
from collections import namedtuple

import yaml

Column = namedtuple("Column", "language band version")
Cell = namedtuple("Cell", "glyph kind link")

LANGUAGES = {"java", "javascript", "python", "csharp", "go"}
# driver-versions.md rows: | <language> | <band> | <version> |
COLUMN_ROW_RE = re.compile(r"^\|\s*([a-z]+)\s*\|\s*([\w.-]+)\s*\|\s*([\w.]+)\s*\|")


def load_columns(md_path):
    """Ordered (language, band, version) columns from driver-versions.md."""
    columns = []
    with open(md_path, encoding="utf-8") as fh:
        for line in fh:
            match = COLUMN_ROW_RE.match(line)
            if match and match.group(1) in LANGUAGES:
                columns.append(Column(match.group(1), match.group(2), match.group(3)))
    return columns


def load_scenarios(spec_path):
    """Ordered scenario dicts from spec.yaml, with defaulted optional fields."""
    with open(spec_path, encoding="utf-8") as fh:
        spec = yaml.safe_load(fh)
    scenarios = []
    for entry in spec.get("scenarios", []):
        scenarios.append({
            "id": entry["id"],
            "area": entry["area"],
            "title": entry["title"],
            "current_status": entry.get("current_status", "unverified"),
            "tracking_issue": entry.get("tracking_issue"),
            "known_limitation": entry.get("known_limitation"),
            "applicable_driver_versions": entry.get("applicable_driver_versions", "all"),
        })
    return scenarios
