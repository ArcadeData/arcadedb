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

"""Fails when the native image's CPU baseline (-march) is unstated, unsafe, or undocumented.

native-image compiles for one CPU baseline, chosen with -march, and a binary started on a CPU below
that baseline exits at once with "The current machine does not support all of the following CPU
features that are required by the image". The baseline is therefore the native image's minimum CPU
requirement. Until #10581 native/pom.xml passed no -march at all, so the published amd64 binary
silently required x86-64-v3 (AVX2, BMI2, FMA) - GraalVM's default - and nothing said so.

Three things are enforced:

  1. native/pom.xml passes exactly one -march build arg, in the base native-maven-plugin
     configuration (a profile's buildArgs are appended after it, and a second -march would win), and
     it resolves to an explicit baseline for every architecture CI builds on: amd64 (Linux, Windows),
     x86_64 (macOS) and aarch64. The value comes from the `native.march` property, overridden by
     profiles activated on <os><arch>, so this resolves it the same way Maven does for each arch.
  2. No resolved baseline, and no workflow or build script, uses `native`. -march=native targets the
     CPU of the machine running the build, so a CI-built binary would require whatever the runner
     happened to have and refuse to start on a CPU with less.
  3. docs/native-image.md names every resolved baseline, so the documented CPU requirement cannot
     drift away from the one the binary is built for.

Usage:
  check-native-march.py [pom] [docs] [file-to-scan ...]

With no arguments it checks the repository's own native/pom.xml and docs/native-image.md, and scans
native-image.yml and the native build scripts for -march=native.
"""

import re
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path

PROPERTY = "native.march"

# The os.arch values a JVM reports on the platforms native-image.yml and the local scripts build on.
# Linux and Windows report amd64, macOS reports x86_64, for the same architecture. An arch outside
# this table falls back to the pom's native.march default and is not checked here.
#
# These patterns only reject a baseline of the wrong family; they deliberately accept a RAISED one
# such as x86-64-v4 (AVX-512) or armv8.4-a. Raising the baseline drops CPUs, so it is the docs check
# in main() that catches it: the new value is not in docs/native-image.md until someone says so.
ARCHES = {
    "amd64": re.compile(r"^(compatibility|x86-64(-v[1-4])?)$"),
    "x86_64": re.compile(r"^(compatibility|x86-64(-v[1-4])?)$"),
    "aarch64": re.compile(r"^(compatibility|armv8(\.[1-9])?-a(\+[a-z]+)*)$"),
}

NATIVE_MARCH = re.compile(r"(-march=native|native\.march=native)\b")


def strip_namespaces(root):
    for element in root.iter():
        if isinstance(element.tag, str) and "}" in element.tag:
            element.tag = element.tag.split("}", 1)[1]
    return root


def build_args(plugin_parent):
    """Yields the buildArg texts of every native-maven-plugin under a <plugins> parent element."""
    for plugin in plugin_parent.findall("plugins/plugin"):
        if (plugin.findtext("artifactId") or "").strip() != "native-maven-plugin":
            continue
        for arg in plugin.findall("configuration/buildArgs/buildArg"):
            yield (arg.text or "").strip()


def resolve(value, properties):
    return re.sub(r"\$\{([^}]+)\}", lambda m: properties.get(m.group(1), m.group(0)), value)


def check_pom(pom, problems):
    """Returns {arch: resolved -march value} for every arch it could resolve, appending problems."""
    try:
        root = strip_namespaces(ElementTree.parse(pom).getroot())
    except ElementTree.ParseError as e:
        problems.append(f"{pom}: not parseable as XML ({e})")
        return {}

    base_build = root.find("build")
    base_args = [a for a in build_args(base_build) if a.startswith("-march")] if base_build is not None else []
    profile_args = []
    for profile in root.findall("profiles/profile"):
        build = profile.find("build")
        if build is not None:
            profile_args += [(profile.findtext("id"), a) for a in build_args(build) if a.startswith("-march")]

    for profile_id, arg in profile_args:
        problems.append(
            f"{pom}: profile '{profile_id}' passes {arg}. A profile's buildArgs are appended after the base "
            f"ones and the last -march wins, so the baseline would depend on which profiles are active. "
            f"Set the {PROPERTY} property in the profile instead."
        )

    if not base_args:
        problems.append(
            f"{pom}: the native-maven-plugin passes no -march build arg, so the binary gets whatever CPU "
            f"baseline the builder's GraalVM defaults to (x86-64-v3 on amd64 for GraalVM 25), and nothing "
            f"states the CPU the published image requires (#10581)."
        )
        return {}
    if len(base_args) > 1:
        problems.append(f"{pom}: the native-maven-plugin passes -march {len(base_args)} times ({', '.join(base_args)}); "
                        f"pass it once.")

    defaults = {}
    for element in root.findall("properties/*"):
        defaults[element.tag] = (element.text or "").strip()

    resolved = {}
    for arch, valid in ARCHES.items():
        properties = dict(defaults)
        for profile in root.findall("profiles/profile"):
            if (profile.findtext("activation/os/arch") or "").strip() == arch:
                for element in profile.findall("properties/*"):
                    properties[element.tag] = (element.text or "").strip()

        arg = resolve(base_args[0], properties)
        value = arg.split("=", 1)[1] if "=" in arg else ""
        if "${" in value or value == "":
            problems.append(f"{pom}: {base_args[0]} does not resolve to a baseline on {arch} (got '{arg}').")
            continue
        if value == "native" or value.startswith("native+"):
            problems.append(
                f"{pom}: on {arch} the binary is built with -march={value}, which targets the CPU of the machine "
                f"running the build. A CI-built binary would then refuse to start on any CPU the runner outdoes."
            )
            continue
        if not valid.match(value):
            problems.append(f"{pom}: on {arch} -march resolves to '{value}', which is not a baseline for that architecture.")
            continue
        resolved[arch] = value

    return resolved


def main(argv):
    root = Path(__file__).resolve().parents[2]
    pom = Path(argv[1]) if len(argv) > 1 else root / "native" / "pom.xml"
    docs = Path(argv[2]) if len(argv) > 2 else root / "docs" / "native-image.md"
    scanned = [Path(p) for p in argv[3:]] if len(argv) > 3 else [
        root / ".github" / "workflows" / "native-image.yml",
        root / "native" / "scripts" / "build-native.sh",
        root / "native" / "scripts" / "build-native-docker.sh",
    ]

    for path in [pom, docs, *scanned]:
        if not path.is_file():
            print(f"{path}: not found")
            return 1

    problems = []
    resolved = check_pom(pom, problems)

    documentation = docs.read_text()
    for value in sorted(set(resolved.values())):
        if f"`{value}`" not in documentation:
            arches = ", ".join(a for a, v in resolved.items() if v == value)
            problems.append(
                f"{docs}: does not name `{value}`, the CPU baseline {pom} builds the {arches} binary for. "
                f"Users whose CPU lacks it get a startup error and need to find out why."
            )

    for path in scanned:
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            if NATIVE_MARCH.search(line):
                problems.append(
                    f"{path}:{number}: builds with -march=native, which ties the binary to the CPU of the "
                    f"machine running the build: {line.strip()}"
                )

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("The -march baseline is the native image's minimum CPU: a binary started on a CPU without it")
        print("exits at once. See the native.march property in native/pom.xml (#10581).")
        return 1

    summary = ", ".join(f"{arch}={value}" for arch, value in resolved.items())
    print(f"Native CPU baseline explicit and documented: {summary}.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
