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

"""Builds the CycloneDX SBOM of one ArcadeDB distribution from the package module's BOM.

cyclonedx-maven-plugin describes the package module's whole runtime classpath, which is neither
what a given assembly ships (base leaves out Studio, the console and every wire protocol) nor
complete (Studio's JavaScript is an npm tree that Maven never sees). Publishing that BOM as the
SBOM of a distribution would list jars the archive does not contain and miss the libraries the
browser actually runs. This script closes both gaps:

  --lib-dir   keeps only the library components whose jar is in the assembly's lib/ directory,
              matched by file name and checked by SHA-512 (SHA-256 when that is all the component
              has), and fails when a jar there has no component, a component with neither hash, or a
              different hash, so the SBOM provably lists what the archive ships
  --nested    nests the components of an npm BOM (npm-lock-to-cdx.py) under a new application
              component named --name, versioned like the root BOM, anchored to the root's library
              component of the same name (the jar that bundles the npm assets: its hashes are the
              application's). With --lib-dir, nothing is nested when that jar is not shipped. The npm
              BOM's tools join metadata.tools
  --module-boms
              a glob of the per-module SBOMs (<module>/target/*-cyclonedx.json). Each library
              component with classifier `shaded` gets, as nested components, its module SBOM's
              libraries minus what the module pom's maven-shade-plugin <artifactSet> leaves out, so a
              scanner sees the TinkerPop or Netty inside an uber-jar. A shaded jar without a module
              SBOM fails the run
  --maven-repo
              with --lib-dir, every META-INF/maven/<g>/<a>/pom.properties inside a shipped jar that is
              neither the jar itself nor already in the document (purl without qualifiers) is nested
              under the first jar that carries it as a library marked arcadedb:embedded=true and
              arcadedb:embedded-in=<every carrier jar, comma-separated, sorted>, with an edge from each
              carrier,
              so an artifact a dependency shaded in (Netty in ratis-thirdparty in the ha-raft uber-jar)
              is visible to an SBOM scan. Its license comes from <g>/<a>/<v>/<a>-<v>.pom in this local
              repository (default ~/.m2/repository), following up to 5 parents, as an SPDX id when
              the name or URL names one beyond doubt (SPDX_BY_NAME / SPDX_BY_URL), else by name, and is
              left out when not found
  --supplier, --supplier-url, --timestamp
              stamp the fields a release-level SBOM needs and cyclonedx-maven-plugin leaves out:
              metadata.supplier and metadata.timestamp (UTC, SOURCE_DATE_EPOCH when set, so a
              reproducible build stays reproducible)

The root metadata (tools, component) is kept, bom-refs are de-duplicated (first one wins,
dependency edges are merged), and the serial number is derived from the content, because the
output is a different document from its root BOM.

Usage:
  merge-sbom.py --root <jvm.cdx.json> [--nested <npm.cdx.json> --name <component>]
                [--lib-dir <dir>] [--module-boms <glob>] [--maven-repo <dir>] [--supplier <name>] [--supplier-url <url>] [--timestamp]
                --out <file>
"""

import argparse
import fnmatch
import glob
import hashlib
import json
import os
import sys
import uuid
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote


class MergeError(Exception):
    pass


def load(path):
    try:
        bom = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise MergeError(f"{path}: unreadable ({e})")
    if not isinstance(bom, dict) or bom.get("bomFormat") != "CycloneDX":
        raise MergeError(f"{path}: not a CycloneDX JSON document")
    return bom


def jar_name(component):
    """The file name maven-assembly-plugin gives the component's artifact, or None when it is not a Maven artifact."""
    purl = component.get("purl") or ""
    if not purl.startswith("pkg:maven/"):
        return None
    path, _, query = purl.partition("?")
    qualifiers = dict(part.partition("=")[::2] for part in query.split("&") if "=" in part)
    version = unquote(path.rpartition("@")[2])
    classifier = qualifiers.get("classifier")
    extension = qualifiers.get("type", "jar")
    return f"{component.get('name')}-{version}{'-' + classifier if classifier else ''}.{extension}"


def file_digest(path, algorithm):
    digest = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def expected_hash(component):
    """(hashlib name, set of expected hex digests) for the strongest of SHA-512 / SHA-256 the component carries."""
    for alg, name in (("SHA-512", "sha512"), ("SHA-256", "sha256")):
        values = {h.get("content", "").lower() for h in component.get("hashes") or []
                  if isinstance(h, dict) and h.get("alg") == alg and h.get("content")}
        if values:
            return name, values
    return None, set()


def filter_to_lib(bom, lib_dir):
    """Keeps the top-level library components shipped in lib_dir; fails on a jar the BOM does not describe."""
    lib = Path(lib_dir)
    jars = sorted(p for p in lib.glob("*.jar") if p.is_file()) if lib.is_dir() else []
    if not jars:
        raise MergeError(f"{lib_dir}: no jar to describe (is this the assembly's lib/ directory?)")
    by_file = {}
    for component in bom.get("components") or []:
        name = jar_name(component) if component.get("type") == "library" else None
        if name:
            by_file.setdefault(name, component)
    kept, problems = [], []
    for jar in jars:
        component = by_file.get(jar.name)
        if component is None:
            problems.append(f"{jar}: shipped but not in the SBOM")
            continue
        algorithm, expected = expected_hash(component)
        if not expected:
            problems.append(f"{jar}: {component.get('purl')} has no SHA-256 or SHA-512 to check the jar against")
            continue
        if file_digest(jar, algorithm) not in expected:
            problems.append(f"{jar}: hash differs from the SBOM's {algorithm.upper()} for {component.get('purl')}")
            continue
        kept.append((jar, component))
    if problems:
        raise MergeError("\n".join(problems))
    shipped = {id(c) for _, c in kept}
    # Non-library entries (none today) describe something other than a jar under lib/: keep them.
    bom["components"] = [c for c in bom.get("components") or [] if c.get("type") != "library" or id(c) in shipped]
    return kept


POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def purl_qualifiers(component):
    query = (component.get("purl") or "").partition("?")[2]
    return dict(part.partition("=")[::2] for part in query.split("&") if "=" in part)


def artifact_set(pom):
    """The (includes, excludes) patterns of maven-shade-plugin's <artifactSet> in a module pom, ([], []) without one."""
    try:
        root = ET.parse(pom).getroot()
    except (OSError, ET.ParseError) as e:
        raise MergeError(f"{pom}: unreadable ({e})")
    plugin = ".//m:build/m:plugins/m:plugin[m:artifactId='maven-shade-plugin']//m:artifactSet"
    sets = root.findall(plugin, POM_NS)
    patterns = lambda kind: [e.text.strip() for s in sets for e in s.findall(f"m:{kind}s/m:{kind}", POM_NS) if e.text]
    return patterns("include"), patterns("exclude")


def matches(pattern, component):
    """A maven-shade artifactSet pattern, * wildcards, against a component.

    Tokens map as maven-shade's ArtifactId does: g:a (any type and classifier), g:a:classifier (any
    type) and g:a:type:classifier. A 3-token pattern names the classifier, not the type.
    """
    tokens = pattern.split(":")
    if len(tokens) == 3:
        tokens = [tokens[0], tokens[1], "*", tokens[2]]
    tokens += ["*"] * (4 - len(tokens))
    qualifiers = purl_qualifiers(component)
    coordinates = [component.get("group") or "", component.get("name") or "", qualifiers.get("type", "jar"),
                   qualifiers.get("classifier", "")]
    return all(fnmatch.fnmatchcase(value, token) for token, value in zip(tokens, coordinates))


def load_module_boms(pattern):
    """(group, name, version) -> (bom, path) for every module SBOM the glob matches."""
    modules = {}
    for path in sorted(glob.glob(pattern)):
        bom = load(path)
        component = (bom.get("metadata") or {}).get("component") or {}
        modules.setdefault((component.get("group"), component.get("name"), component.get("version")), (bom, path))
    return modules


def nest_shaded(bom, modules):
    """Nests under each shaded jar the libraries its module shades in; fails on a shaded jar no module SBOM describes."""
    deps = bom.setdefault("dependencies", [])
    missing = []
    for jar in bom.get("components") or []:
        if jar.get("type") != "library" or purl_qualifiers(jar).get("classifier") != "shaded":
            continue
        found = modules.get((jar.get("group"), jar.get("name"), jar.get("version")))
        if found is None:
            missing.append(jar.get("purl"))
            continue
        module, path = found
        includes, excludes = artifact_set(Path(path).parent.parent / "pom.xml")
        renamed = {}
        nested = []
        for component in module.get("components") or []:
            if component.get("type") != "library":
                continue
            if includes and not any(matches(p, component) for p in includes):
                continue
            if any(matches(p, component) for p in excludes):
                continue
            ref = f"{jar['bom-ref']}|{component.get('bom-ref') or component.get('purl')}"
            renamed[component.get("bom-ref")] = ref
            nested.append(dict(component, **{"bom-ref": ref}))
        jar["components"] = (jar.get("components") or []) + nested
        module_root = ((module.get("metadata") or {}).get("component") or {}).get("bom-ref")
        renamed[module_root] = jar["bom-ref"]
        for entry in module.get("dependencies") or []:
            ref = renamed.get(entry.get("ref"))
            if ref:
                deps.append({"ref": ref, "dependsOn": [renamed[r] for r in entry.get("dependsOn") or [] if r in renamed]})
    if missing:
        raise MergeError("no module SBOM for the shaded jar(s): " + ", ".join(missing))


def local_name(element):
    return element.tag.rpartition("}")[2]


def child(element, name):
    return next((c for c in element if local_name(c) == name), None)


def child_text(element, name):
    found = child(element, name) if element is not None else None
    return (found.text or "").strip() if found is not None and found.text else ""


# Pom license names and URLs that name one SPDX license beyond doubt. Matched after normalisation (lower case,
# single spaces, no trailing punctuation; URLs without scheme, www., trailing slash or .txt/.html/.php).
SPDX_BY_NAME = {
    "Apache-2.0": ("apache-2.0", "apache 2.0", "apache 2", "apache license 2.0", "apache license, version 2.0",
                   "apache license version 2.0", "the apache license, version 2.0",
                   "the apache software license, version 2.0", "apache software license - version 2.0"),
    "MIT": ("mit", "mit license", "the mit license", "the mit license (mit)"),
    "BSD-2-Clause": ("bsd-2-clause", "bsd 2-clause license", "the bsd 2-clause license", "simplified bsd license"),
    "BSD-3-Clause": ("bsd-3-clause", "bsd 3-clause license", "the bsd 3-clause license", "new bsd license",
                     "revised bsd license", "the new bsd license"),
    "EPL-1.0": ("epl-1.0", "eclipse public license 1.0", "eclipse public license - v 1.0",
                "eclipse public license, version 1.0", "eclipse public license v1.0"),
    "EPL-2.0": ("epl-2.0", "eclipse public license 2.0", "eclipse public license - v 2.0",
                "eclipse public license, version 2.0", "eclipse public license v2.0"),
    "LGPL-2.1-or-later": ("lgpl-2.1-or-later", "lgpl-2.1+", "gnu lesser general public license v2.1 or later",
                          "gnu lesser general public license, version 2.1 or later"),
    "MPL-2.0": ("mpl-2.0", "mpl 2.0", "mozilla public license 2.0", "mozilla public license, version 2.0",
                "mozilla public license version 2.0"),
    "CDDL-1.0": ("cddl-1.0", "cddl 1.0", "common development and distribution license 1.0",
                 "common development and distribution license (cddl) v1.0"),
    "CDDL-1.1": ("cddl-1.1", "cddl 1.1", "common development and distribution license 1.1",
                 "common development and distribution license (cddl) v1.1"),
}
SPDX_BY_URL = {
    "Apache-2.0": ("apache.org/licenses/license-2.0", "opensource.org/licenses/apache-2.0"),
    "MIT": ("opensource.org/licenses/mit", "opensource.org/licenses/mit-license"),
    "BSD-2-Clause": ("opensource.org/licenses/bsd-2-clause",),
    "BSD-3-Clause": ("opensource.org/licenses/bsd-3-clause",),
    "EPL-1.0": ("eclipse.org/legal/epl-v10", "opensource.org/licenses/epl-1.0"),
    "EPL-2.0": ("eclipse.org/legal/epl-2.0", "eclipse.org/legal/epl-v20", "opensource.org/licenses/epl-2.0"),
    "MPL-2.0": ("mozilla.org/mpl/2.0", "opensource.org/licenses/mpl-2.0"),
    "CDDL-1.0": ("opensource.org/licenses/cddl-1.0",),
    "CDDL-1.1": ("oss.oracle.com/licenses/cddl-1.1",),
}
NAME_TO_SPDX = {name: spdx for spdx, names in SPDX_BY_NAME.items() for name in names}
URL_TO_SPDX = {url: spdx for spdx, urls in SPDX_BY_URL.items() for url in urls}


def spdx_id(name, url):
    """The SPDX id a pom license names beyond doubt, by name or by URL; None when unknown or when they disagree."""
    by_name = NAME_TO_SPDX.get(" ".join(name.lower().split()).rstrip(".;, ")) if name else None
    by_url = None
    if url:
        normalised = url.lower().strip().split("://", 1)[-1].removeprefix("www.").rstrip("/")
        for suffix in (".txt", ".html", ".php"):
            normalised = normalised.removesuffix(suffix)
        by_url = URL_TO_SPDX.get(normalised)
    if by_name and by_url and by_name != by_url:
        return None
    return by_name or by_url


def pom_licenses(repo, group, artifact, version, depth=0):
    """<licenses> of g:a:v's pom in the local repository, inherited from up to 5 parents; [] when unresolvable."""
    pom = Path(repo) / group.replace(".", "/") / artifact / version / f"{artifact}-{version}.pom"
    try:
        project = ET.parse(pom).getroot()
    except (OSError, ET.ParseError):
        return []
    licenses = []
    for entry in (child(project, "licenses") or []):
        name, url = child_text(entry, "name"), child_text(entry, "url")
        if name and "${" not in name:
            identifier = spdx_id(name, url)
            entry = {"id": identifier} if identifier else {"name": name}
            if url:
                entry["url"] = url
            licenses.append({"license": entry})
    if licenses or depth >= 5:
        return licenses
    parent = child(project, "parent")
    if parent is None:
        return []
    coordinates = [child_text(parent, n) for n in ("groupId", "artifactId", "version")]
    return pom_licenses(repo, *coordinates, depth + 1) if all(coordinates) else []


def embedded_coordinates(jar):
    """(groupId, artifactId, version) of every META-INF/maven/<g>/<a>/pom.properties inside a jar."""
    found = []
    try:
        with zipfile.ZipFile(jar) as archive:
            for name in archive.namelist():
                parts = name.split("/")
                if len(parts) != 5 or parts[0] != "META-INF" or parts[1] != "maven" or parts[4] != "pom.properties":
                    continue
                values = {}
                for line in archive.read(name).decode("utf-8", "replace").splitlines():
                    line = line.strip()
                    if line and line[0] not in "#!" and "=" in line:
                        key, _, value = line.partition("=")
                        values[key.strip()] = value.strip()
                coordinates = (values.get("groupId"), values.get("artifactId"), values.get("version"))
                if all(coordinates) and coordinates not in found:
                    found.append(coordinates)
    except zipfile.BadZipFile:
        # Already matched to its BOM component by hash, so it is the artifact the BOM names; a file that is
        # not a zip carries no pom.properties to report.
        print(f"merge-sbom.py: {jar}: not a zip, no embedded artifacts read", file=sys.stderr)
    except OSError as e:
        raise MergeError(f"{jar}: unreadable ({e})")
    return sorted(found)


def purl_base(component):
    return (component.get("purl") or "").partition("?")[0]


def all_components(components):
    for component in components or []:
        yield component
        yield from all_components(component.get("components"))


def add_embedded(bom, shipped, repo):
    """Nests under each shipped jar the Maven artifacts it carries that the document does not list yet.

    A jar's META-INF/maven/<g>/<a>/pom.properties entries name every artifact packed into it,
    including the ones a dependency had itself shaded in (Netty inside ratis-thirdparty inside the
    ha-raft uber-jar), which no module SBOM lists. Each coordinate that is neither the jar itself nor
    already in the document (purl compared without qualifiers) becomes a library component nested
    under the jar, marked arcadedb:embedded, with the license its pom declares in the local
    repository when it is there. Returns {jar file name: number added}.
    """
    present = {purl_base(c) for c in all_components(bom.get("components"))}
    deps = bom.setdefault("dependencies", [])
    embedded_by_base = {}   # purl base -> (component, set of carrier jar names)
    added = {}
    for jar, component in shipped:
        own = (component.get("group"), component.get("name"), component.get("version"))
        nested, edges = [], []
        for group, artifact, version in embedded_coordinates(jar):
            if (group, artifact, version) == own:
                continue
            base = f"pkg:maven/{group}/{artifact}@{version}"
            if base in embedded_by_base:
                # Already added under an earlier jar: one component, but this jar carries it too.
                embedded, carriers = embedded_by_base[base]
                carriers.add(jar.name)
                edges.append(embedded["bom-ref"])
                continue
            if base in present:
                continue
            present.add(base)
            embedded = {"type": "library", "bom-ref": f"{component['bom-ref']}|embedded:{base}", "group": group,
                        "name": artifact, "version": version, "purl": f"{base}?type=jar"}
            licenses = pom_licenses(repo, group, artifact, version)
            if licenses:
                embedded["licenses"] = licenses
            embedded_by_base[base] = (embedded, {jar.name})
            nested.append(embedded)
            edges.append(embedded["bom-ref"])
        if nested:
            component["components"] = (component.get("components") or []) + nested
            added[jar.name] = len(nested)
        if edges:
            deps.append({"ref": component["bom-ref"], "dependsOn": edges})
    for embedded, carriers in embedded_by_base.values():
        embedded["properties"] = [{"name": "arcadedb:embedded", "value": "true"},
                                  {"name": "arcadedb:embedded-in", "value": ",".join(sorted(carriers))}]
    return added


def all_refs(components):
    for component in components or []:
        if component.get("bom-ref"):
            yield component["bom-ref"]
        yield from all_refs(component.get("components"))


def prune_dependencies(bom):
    """Drops edges to removed components and re-attaches orphans to the root, so nothing kept is unreachable."""
    root_ref = ((bom.get("metadata") or {}).get("component") or {}).get("bom-ref")
    known = set(all_refs(bom.get("components")))
    if root_ref:
        known.add(root_ref)
    deps = []
    for entry in bom.get("dependencies") or []:
        if entry.get("ref") in known:
            deps.append({"ref": entry["ref"], "dependsOn": [r for r in entry.get("dependsOn") or [] if r in known]})
    if root_ref:
        reached = {r for d in deps if d["ref"] != root_ref for r in d["dependsOn"]}
        root_entry = next((d for d in deps if d["ref"] == root_ref), None)
        if root_entry is None:
            root_entry = {"ref": root_ref, "dependsOn": []}
            deps.insert(0, root_entry)
        reached.update(root_entry["dependsOn"])
        for component in bom.get("components") or []:
            ref = component.get("bom-ref")
            if ref and ref not in reached:
                root_entry["dependsOn"].append(ref)
    # A nested component nothing points at (a shaded-in library reached only through an artifact the
    # shade excluded) belongs to the component it is nested in.
    reached = {r for d in deps for r in d["dependsOn"]}
    by_ref = {d["ref"]: d for d in deps}
    for parent in bom.get("components") or []:
        orphans = [c.get("bom-ref") for c in parent.get("components") or []
                   if c.get("bom-ref") and c.get("bom-ref") not in reached]
        if orphans and parent.get("bom-ref"):
            entry = by_ref.get(parent["bom-ref"])
            if entry is None:
                entry = by_ref[parent["bom-ref"]] = {"ref": parent["bom-ref"], "dependsOn": []}
                deps.append(entry)
            entry["dependsOn"].extend(orphans)
    bom["dependencies"] = deps


def merge_tools(bom, nested):
    """Adds the nested BOM's generators to metadata.tools.components, so the merged SBOM names every tool behind it."""
    nested_tools = ((nested.get("metadata") or {}).get("tools") or {})
    if not isinstance(nested_tools, dict) or not nested_tools.get("components"):
        return
    tools = bom.setdefault("metadata", {}).setdefault("tools", {})
    if not isinstance(tools, dict):
        return
    target = tools.setdefault("components", [])
    known = {(t.get("group"), t.get("name"), t.get("version")) for t in target}
    for tool in nested_tools["components"]:
        key = (tool.get("group"), tool.get("name"), tool.get("version"))
        if key not in known:
            known.add(key)
            target.append(tool)


def dedupe(components, seen):
    out = []
    for component in components or []:
        ref = component.get("bom-ref")
        if ref and ref in seen:
            continue
        if ref:
            seen.add(ref)
        if component.get("components"):
            component = dict(component, components=dedupe(component["components"], seen))
        out.append(component)
    return out


def nest(bom, nested, name):
    """Appends nested's components under a new application component `name`; False when the anchor jar is absent."""
    anchor = next((c for c in bom.get("components") or [] if c.get("type") == "library" and c.get("name") == name), None)
    if anchor is None:
        return False
    version = ((bom.get("metadata") or {}).get("component") or {}).get("version") or anchor.get("version")
    purl = f"pkg:npm/{name}@{version}"
    nested_root = (nested.get("metadata") or {}).get("component") or {}
    application = {
        "type": "application",
        "bom-ref": purl,
        "name": name,
        "version": version,
        "description": f"npm packages bundled in {anchor.get('purl')}",
        "hashes": anchor.get("hashes") or [],
        "licenses": nested_root.get("licenses") or anchor.get("licenses") or [],
        "purl": purl,
    }
    for field in ("publisher", "supplier"):
        if anchor.get(field):
            application[field] = anchor[field]
    seen = set(all_refs(bom.get("components")))
    seen.add(purl)
    application["components"] = dedupe(nested.get("components"), seen)
    merge_tools(bom, nested)
    bom.setdefault("components", []).append(application)

    old_root = nested_root.get("bom-ref")
    deps = bom.setdefault("dependencies", [])
    deps.append({"ref": anchor["bom-ref"], "dependsOn": [purl]})
    for entry in nested.get("dependencies") or []:
        ref = purl if entry.get("ref") == old_root else entry.get("ref")
        deps.append({"ref": ref, "dependsOn": [purl if r == old_root else r for r in entry.get("dependsOn") or []]})
    return True


def merge_dependencies(deps):
    """One entry per ref, the union of its edges, first-seen order."""
    merged = {}
    for entry in deps:
        target = merged.setdefault(entry["ref"], [])
        for r in entry.get("dependsOn") or []:
            if r not in target:
                target.append(r)
    return [{"ref": ref, "dependsOn": on} for ref, on in merged.items()]


def timestamp():
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    moment = datetime.fromtimestamp(int(epoch), timezone.utc) if epoch else datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def build(args):
    bom = load(args.root)
    shipped = filter_to_lib(bom, args.lib_dir) if args.lib_dir else []
    if args.module_boms:
        nest_shaded(bom, load_module_boms(args.module_boms))
    if shipped:
        repo = args.maven_repo or str(Path.home() / ".m2" / "repository")
        for jar_name, count in add_embedded(bom, shipped, repo).items():
            print(f"merge-sbom.py: {jar_name}: {count} embedded component(s) added", file=sys.stderr)
    if args.nested:
        if not nest(bom, load(args.nested), args.name):
            if not args.lib_dir:
                raise MergeError(f"{args.root}: no library component named {args.name} to nest {args.nested} under")
            print(f"merge-sbom.py: {args.name} is not shipped in {args.lib_dir}, {args.nested} not nested", file=sys.stderr)
    bom["components"] = dedupe(bom.get("components"), set())
    bom["dependencies"] = merge_dependencies(bom.get("dependencies") or [])
    prune_dependencies(bom)

    metadata = bom.setdefault("metadata", {})
    if args.timestamp:
        metadata["timestamp"] = timestamp()
    if args.supplier:
        supplier = {"name": args.supplier}
        if args.supplier_url:
            supplier["url"] = [args.supplier_url]
        metadata["supplier"] = supplier

    bom.pop("serialNumber", None)
    content = json.dumps(bom, sort_keys=True, separators=(",", ":"))
    bom["serialNumber"] = f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, 'arcadedb-sbom:' + content)}"
    bom["version"] = 1
    return bom


def main(argv):
    parser = argparse.ArgumentParser(prog="merge-sbom.py", description=__doc__.split("\n")[0])
    parser.add_argument("--root", required=True)
    parser.add_argument("--nested")
    parser.add_argument("--name", default="arcadedb-studio")
    parser.add_argument("--lib-dir")
    parser.add_argument("--module-boms")
    parser.add_argument("--maven-repo")
    parser.add_argument("--supplier")
    parser.add_argument("--supplier-url")
    parser.add_argument("--timestamp", action="store_true")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    try:
        bom = build(args)
    except MergeError as e:
        print(e, file=sys.stderr)
        return 1
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(bom, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
