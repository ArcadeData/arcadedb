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

"""Checks native/pom.xml's claim about GraalVM's default -march against the builder itself (#11254).

check-native-march.py compares each resolved -march with the `native.march.expected.<arch>` record in
native/pom.xml. That stops an unannounced change, but the record is only as right as whoever wrote it:
#10581 and #10715 both asserted what "GraalVM's default" is, and for AArch64 both were wrong (the
builder defaults to armv8.1-a, not armv8-a). Nothing offline can check such a claim, so this script
runs where the builder is, in the native job, and asks it.

The builder states its defaults in the help of -H:MicroArchitecture ("Defaults to 'x86-64-v3' on AMD64
and 'armv8.1-a' on AArch64"), printed by `native-image --expert-options-all`. The pom says, per
architecture, whether its baseline is that default or a deliberate departure from it:

  native.march.pins-builder-default.<arch> = true    the baseline IS the builder's default
  native.march.pins-builder-default.<arch> = false   the baseline is chosen on purpose, and differs

For the architecture running the build this fails when the claim is false: a `true` whose baseline
differs from the builder's default (the wording of the pom would mislead), and a `false` whose
baseline equals it (the record is stale). It also fails when the builder's default cannot be read,
because then the claim is unverified, which is the situation this script exists to end.

Usage:
  check-native-march-builder-default.py [pom] [native-image executable] [machine]

With no arguments it checks native/pom.xml against the `native-image` on the PATH, for the architecture
of the machine it runs on (`machine` is a platform.machine() value, for the tests to pick another).
"""

import importlib.util
import platform
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path

PINS_PROPERTY = "native.march.pins-builder-default"

# platform.machine() on the platforms native-image.yml builds on -> the pom's per-arch records.
# Linux and Windows report amd64 to the pom's profiles, macOS reports x86_64: same architecture.
FAMILIES = {
    "x86_64": ("amd64", "x86_64"),
    "amd64": ("amd64", "x86_64"),
    "aarch64": ("aarch64",),
    "arm64": ("aarch64",),
}

DEFAULT_SENTENCE = re.compile(r"Defaults to '([^']+)' on AMD64 and '([^']+)' on AArch64")


def load_checker():
    """Loads check-native-march.py, so the pom is resolved here exactly the way the lint resolves it."""
    path = Path(__file__).resolve().with_name("check-native-march.py")
    spec = importlib.util.spec_from_file_location("check_native_march", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def builder_defaults(native_image):
    """Returns {"amd64": ..., "aarch64": ...} as the builder reports them, or raises ValueError."""
    executable = shutil.which(native_image)
    if executable is None:
        raise ValueError(f"'{native_image}' is not an executable on this machine")
    result = subprocess.run([executable, "--expert-options-all"], capture_output=True, text=True, timeout=300)
    text = re.sub(r"\s+", " ", result.stdout + " " + result.stderr)
    found = DEFAULT_SENTENCE.search(text)
    if found is None:
        raise ValueError(
            f"`{native_image} --expert-options-all` (exit {result.returncode}) has no \"Defaults to 'X' on AMD64 and "
            f"'Y' on AArch64\" sentence in the help of -H:MicroArchitecture. The builder's wording changed: read "
            f"`{native_image} --expert-options-all | grep -A4 MicroArchitecture` and update DEFAULT_SENTENCE."
        )
    return {"amd64": found.group(1), "aarch64": found.group(2)}


def pins_records(pom):
    document = ElementTree.parse(pom).getroot()
    result = {}
    for element in document.iter():
        tag = element.tag.split("}", 1)[-1]
        if tag.startswith(PINS_PROPERTY + "."):
            result[tag[len(PINS_PROPERTY) + 1:]] = (element.text or "").strip()
    return result


def main(argv):
    root = Path(__file__).resolve().parents[2]
    pom = Path(argv[1]) if len(argv) > 1 else root / "native" / "pom.xml"
    native_image = argv[2] if len(argv) > 2 else "native-image"
    if not pom.is_file():
        print(f"{pom}: not found")
        return 1

    machine = (argv[3] if len(argv) > 3 else platform.machine()).lower()
    keys = FAMILIES.get(machine)
    if keys is None:
        print(f"Build architecture '{machine}' is not one the pom records a baseline for: nothing to verify.")
        return 0

    problems = []
    resolved = load_checker().check_pom(pom, problems)
    pins = pins_records(pom)
    try:
        defaults = builder_defaults(native_image)
    except (ValueError, OSError, subprocess.SubprocessError) as e:
        print(f"Cannot read the builder's default -march, so the pom's claim about it is unverified: {e}")
        return 1

    family = "aarch64" if keys == ("aarch64",) else "amd64"
    builder_default = defaults[family]
    for arch in keys:
        baseline = resolved.get(arch)
        claim = pins.get(arch, "")
        if baseline is None:
            problems.append(f"{pom}: no resolvable -march baseline for {arch}.")
        elif claim not in ("true", "false"):
            problems.append(f"{pom}: <{PINS_PROPERTY}.{arch}> is '{claim}', not true or false: nothing says whether the "
                            f"{arch} baseline is the builder's default or a deliberate departure from it.")
        elif claim == "true" and baseline != builder_default:
            problems.append(
                f"{pom}: on {arch} the baseline is '{baseline}' and <{PINS_PROPERTY}.{arch}> says it is the builder's "
                f"default, but this builder defaults to '{builder_default}'. Either the pom's wording is wrong (set the "
                f"record to false and say why '{baseline}' is chosen), or the baseline should be '{builder_default}'. "
                f"Changing a baseline drops or admits CPUs: that is a decision with a release note (#10715)."
            )
        elif claim == "false" and baseline == builder_default:
            problems.append(
                f"{pom}: on {arch} <{PINS_PROPERTY}.{arch}> says the baseline '{baseline}' departs from the builder's "
                f"default, but this builder defaults to '{baseline}' too. Set the record to true and fix the pom comment."
            )

    if problems:
        for problem in problems:
            print(problem)
        return 1

    print(f"Native -march claim verified against the builder on {family}: builder default '{builder_default}', "
          f"pom baseline '{resolved[keys[0]]}' ({PINS_PROPERTY}={pins[keys[0]]}).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
