#!/usr/bin/env python3
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
#

"""Fails when a release SBOM lacks a field that BSI TR-03183-2 or the NTIA minimum elements require.

An SBOM that is syntactically valid but thin is worse than none: consumers (Dependency-Track,
grype, a customer's procurement check) reject it, or silently match nothing, long after the
release is published and cannot be amended. cyclonedx-maven-plugin drops a hash when an artifact
is not resolvable, a license when the pom declares none and the supplier when the parent pom
loses its <organization>, and none of that fails the build. This script is the tripwire.

Per file it checks:
  - bomFormat is "CycloneDX" and specVersion is at least 1.5 (compared numerically, so 1.10 > 1.9)
  - metadata.timestamp is present (not with --module, see Usage)
  - the supplier is "Arcade Data Ltd": metadata.supplier.name, metadata.manufacturer.name,
    metadata.component.supplier.name or metadata.component.publisher (cyclonedx-maven-plugin only
    fills the last one, from the pom's <organization>)
  - metadata.tools or metadata.authors is non-empty
  - dependencies is a non-empty array
  - every component (nested ones included) of type library or application has name, version, purl,
    at least one hash with alg SHA-256 or SHA-512, and a license with an id, a name or an expression
    (a component with property arcadedb:embedded=true nested directly in a library component, which
    merge-sbom.py adds for an artifact packed inside a shipped jar, needs neither: it is not a file of its
    own and its pom may declare no license; the marker on a top-level or application-child component
    exempts nothing;
    the count of those without a license is printed as a `<file>: notice:` line, never an error)
  - a structurally wrong document prints `<file>: malformed <path>` instead of a traceback

Each violation prints `<file>: <component purl>: missing <field>` (`<file>: metadata: ...` for the
document-level ones) and the exit status is 1 if there is any, else 0.

Usage:
  check-sbom-fields.py [--module] <sbom.cdx.json> [...]

  --module  allow a missing metadata.timestamp. The per-module SBOMs published to Maven Central are
            reproducible-build outputs: cyclonedx-maven-plugin deliberately drops the timestamp when
            project.build.outputTimestamp is set (Maven 3.10 always sets it). Release-level SBOMs
            are checked without the flag and must carry a timestamp.
"""


import json
import sys
from pathlib import Path

SUPPLIER = "Arcade Data Ltd"
MIN_SPEC = (1, 5)
CHECKED_TYPES = ("library", "application")
HASH_ALGS = ("SHA-256", "SHA-512")


def parse_spec(value):
    """'1.6' -> (1, 6); None when it is not a dotted pair of integers."""
    try:
        return tuple(int(part) for part in str(value).split("."))
    except ValueError:
        return None


def has_license(licenses):
    """True when at least one entry names a license (id or name) or carries an expression."""
    for entry in licenses:
        if not isinstance(entry, dict):
            continue
        inner = entry.get("license")
        if entry.get("expression") or (isinstance(inner, dict) and (inner.get("id") or inner.get("name"))):
            return True
    return False


def has_author_or_tool(metadata):
    tools = metadata.get("tools")
    if isinstance(tools, dict):  # CycloneDX 1.5+: {"components": [...], "services": [...]}
        tools = [t for key in ("components", "services") for t in (tools.get(key) or []) if isinstance(t, dict)]
    return bool(tools) or bool(metadata.get("authors"))


def is_embedded(component):
    """True for a component merge-sbom.py found inside a shipped jar (property arcadedb:embedded=true)."""
    properties = component.get("properties")
    return isinstance(properties, list) and any(
        isinstance(p, dict) and p.get("name") == "arcadedb:embedded" and p.get("value") == "true" for p in properties)


def check_components(path, components, where_prefix, err, malformed, embedded_without_license, parent_type=None):
    if not isinstance(components, list):
        malformed(f"{where_prefix}components")
        return
    for index, component in enumerate(components):
        here = f"{where_prefix}components[{index}]"
        if not isinstance(component, dict):
            malformed(here)
            continue
        if component.get("components") is not None:
            check_components(path, component["components"], here + ".", err, malformed, embedded_without_license,
                             component.get("type"))
        if component.get("type") not in CHECKED_TYPES:
            continue
        where = component.get("purl") or component.get("name") or "<unnamed component>"
        if not isinstance(where, str):
            where = here
        for field in ("name", "version", "purl"):
            if not component.get(field):
                err(where, field)
        hashes = component.get("hashes") or []
        licenses = component.get("licenses") or []
        if not isinstance(hashes, list):
            malformed(here + ".hashes")
            hashes = []
        if not isinstance(licenses, list):
            malformed(here + ".licenses")
            licenses = []
        # Only inside a library (the jar that carries it): anywhere else the marker exempts nothing.
        if parent_type == "library" and is_embedded(component):
            # Packed inside a jar the SBOM already hashes, not a file of its own; its license is whatever its
            # pom declares, which may be nothing. Counted, not failed.
            if not has_license(licenses):
                embedded_without_license[0] += 1
            continue
        if not any(isinstance(h, dict) and h.get("alg") in HASH_ALGS and h.get("content") for h in hashes):
            err(where, "hashes")
        if not has_license(licenses):
            err(where, "licenses")


def check(path, require_timestamp=True):
    try:
        bom = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return [f"{path}: metadata: unreadable ({e})"]
    errors = []

    def err(where, field):
        errors.append(f"{path}: {where}: missing {field}")

    def malformed(where):
        errors.append(f"{path}: malformed {where}")

    if not isinstance(bom, dict):
        malformed("<root>")
        return errors

    if bom.get("bomFormat") != "CycloneDX":
        err("metadata", "bomFormat CycloneDX")
    spec = parse_spec(bom.get("specVersion"))
    if spec is None or spec < MIN_SPEC:
        err("metadata", "specVersion >= 1.5")

    metadata = bom.get("metadata")
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, dict):
        malformed("metadata")
        metadata = {}
    if require_timestamp and not metadata.get("timestamp"):
        err("metadata", "timestamp")
    supplier = None
    for key in ("supplier", "manufacturer"):
        party = metadata.get(key)
        if party is None:
            continue
        if not isinstance(party, dict):
            malformed(f"metadata.{key}")
        elif supplier is None:
            supplier = party.get("name")
    component = metadata.get("component")
    if isinstance(component, dict):
        if supplier != SUPPLIER and isinstance(component.get("supplier"), dict):
            supplier = component["supplier"].get("name")
        if supplier != SUPPLIER:
            supplier = component.get("publisher")
    if supplier != SUPPLIER:
        err("metadata", f"supplier {SUPPLIER}")
    if not has_author_or_tool(metadata):
        err("metadata", "author/tool")

    if not bom.get("dependencies"):
        err("metadata", "dependencies")
    elif not isinstance(bom["dependencies"], list):
        malformed("dependencies")

    if bom.get("components") is not None:
        embedded_without_license = [0]
        check_components(path, bom["components"], "", err, malformed, embedded_without_license)
        if embedded_without_license[0]:
            print(f"{path}: notice: {embedded_without_license[0]} embedded component(s) without a license")
    return errors


def main(argv):
    require_timestamp = "--module" not in argv
    files = [a for a in argv if a != "--module"]
    if not files:
        print("Usage: check-sbom-fields.py [--module] <sbom.cdx.json> [...]", file=sys.stderr)
        return 2
    errors = [line for path in files for line in check(path, require_timestamp)]
    for line in errors:
        print(line)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
