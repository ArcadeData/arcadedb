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

"""Writes a CycloneDX 1.6 SBOM of the npm packages a package-lock.json ships (dev dependencies left out).

The Maven SBOM sees Studio only as arcadedb-studio.jar, not the JavaScript libraries bundled in it
that the browser runs. This reads the lockfile (v2/v3 `packages` map) with the standard library
only, so it runs offline and adds no npm dependency to Studio:

  - every entry except the root "", those marked `dev: true` and `link: true` symlinks (their target
    entry is the package, and dependencies on the link resolve to it) becomes a library component with
    name, version, purl pkg:npm/<name>@<version> (a scope's @ encoded as %40), the `integrity`
    hashes converted from base64 to hex (sha512 -> SHA-512, sha384 -> SHA-384, sha256 -> SHA-256)
    and the `license` the lockfile recorded: one SPDX id as a license id, anything compound as an
    expression, anything else as a license name
  - `dependencies` follows each entry's dependencies, optionalDependencies and peerDependencies with
    Node's resolution (the nearest node_modules up the tree), the root's going to the metadata
    component
  - a shipped package with no `license` or no `integrity` fails the run with its lockfile path: a
    license is never made up, and the strict SBOM check would reject a component without either

The bom-ref of a component is its lockfile path, the one key that is unique when two versions of a
package are installed side by side. metadata.timestamp honours SOURCE_DATE_EPOCH, and the serial
number is derived from the content, so a reproducible build stays reproducible.

Usage:
  npm-lock-to-cdx.py <package-lock.json> <out.cdx.json>
"""

import base64
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

HASH_ALGS = {"sha512": "SHA-512", "sha384": "SHA-384", "sha256": "SHA-256"}
SPDX_ID = re.compile(r"^[A-Za-z0-9.+-]+$")
EXPRESSION = re.compile(r"\s(AND|OR|WITH)\s|[()]")


def purl(name, version):
    return f"pkg:npm/{quote(name, safe='/')}@{quote(version, safe='')}"


def package_name(path, entry):
    if entry.get("name"):
        return entry["name"]
    if "node_modules/" in path:
        return path.rpartition("node_modules/")[2]
    return path.rstrip("/").rpartition("/")[2]


def hashes(integrity):
    out = []
    for token in (integrity or "").split():
        alg, _, digest = token.partition("-")
        if alg in HASH_ALGS and digest:
            out.append({"alg": HASH_ALGS[alg], "content": base64.b64decode(digest).hex()})
    return out


def licenses(value):
    if isinstance(value, dict):  # legacy {"type": "MIT", "url": ...}
        value = value.get("type")
    if not isinstance(value, str) or not value.strip():
        return []
    value = value.strip()
    if EXPRESSION.search(value):
        return [{"expression": value}]
    if SPDX_ID.match(value):
        return [{"license": {"id": value}}]
    return [{"license": {"name": value}}]


def resolve(packages, from_path, dependency):
    """Node's lookup: <from>/node_modules/<dep>, then each ancestor's node_modules, then the top level."""
    base = from_path
    while True:
        candidate = f"{base}/node_modules/{dependency}" if base else f"node_modules/{dependency}"
        if candidate in packages:
            entry = packages[candidate]
            return entry.get("resolved") if entry.get("link") else candidate
        if not base:
            return None
        cut = base.rfind("/node_modules/")
        base = base[:cut] if cut >= 0 else ""


def timestamp():
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    moment = datetime.fromtimestamp(int(epoch), timezone.utc) if epoch else datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def convert(lock):
    packages = lock.get("packages") if isinstance(lock, dict) else None
    if not isinstance(packages, dict):
        raise ValueError("no `packages` map: lockfileVersion 2 or 3 is required (run npm install with npm 7 or later)")
    root = packages.get("") or {}
    root_name = root.get("name") or lock.get("name") or "unnamed"
    root_version = root.get("version") or lock.get("version") or "0.0.0"
    root_ref = purl(root_name, root_version)

    # A `link: true` entry is the symlink in node_modules; its `resolved` target entry is the package.
    shipped = {path: entry for path, entry in packages.items()
               if path and not entry.get("dev") and not entry.get("link")}
    components, problems = [], []
    for path, entry in shipped.items():
        name = package_name(path, entry)
        component = {"type": "library", "bom-ref": path, "name": name, "version": entry.get("version", "")}
        component["hashes"] = hashes(entry.get("integrity"))
        component["licenses"] = licenses(entry.get("license"))
        component["purl"] = purl(name, component["version"])
        if not component["licenses"]:
            problems.append(f"{path}: no license in the lockfile")
        if not component["hashes"]:
            problems.append(f"{path}: no sha256/sha384/sha512 integrity in the lockfile")
        components.append(component)
    if problems:
        raise ValueError("\n".join(problems))

    def edges(from_path, entry):
        out = []
        for key in ("dependencies", "optionalDependencies", "peerDependencies"):
            for dependency in entry.get(key) or {}:
                target = resolve(packages, from_path, dependency)
                if target in shipped and target not in out:
                    out.append(target)
        return out

    dependencies = [{"ref": root_ref, "dependsOn": edges("", root)}]
    dependencies += [{"ref": path, "dependsOn": edges(path, entry)} for path, entry in shipped.items()]

    root_component = {"type": "application", "bom-ref": root_ref, "name": root_name, "version": root_version,
                      "licenses": licenses(root.get("license")), "purl": root_ref}
    bom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "version": 1,
        "metadata": {
            "timestamp": timestamp(),
            "tools": {"components": [{"type": "application", "name": "npm-lock-to-cdx.py",
                                      "publisher": "Arcade Data Ltd"}]},
            "component": root_component,
        },
        "components": components,
        "dependencies": dependencies,
    }
    content = json.dumps(bom, sort_keys=True, separators=(",", ":"))
    bom["serialNumber"] = f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, 'arcadedb-npm-sbom:' + content)}"
    return bom


def main(argv):
    if len(argv) != 2:
        print("Usage: npm-lock-to-cdx.py <package-lock.json> <out.cdx.json>", file=sys.stderr)
        return 2
    try:
        lock = json.loads(Path(argv[0]).read_text(encoding="utf-8"))
        bom = convert(lock)
    except (OSError, ValueError) as e:
        print(f"{argv[0]}: {e}", file=sys.stderr)
        return 1
    out = Path(argv[1])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(bom, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
