#!/usr/bin/env python3
"""Fails when native/pom.xml's GraalVM pin and native-image.yml's builder JDK disagree.

The native image embeds GraalJS, so the build runs Truffle's own SVM feature
(TruffleBaseFeature.afterRegistration) inside the native-image builder. That feature comes from the
builder's JDK, the Truffle API it calls comes from the Maven coordinates pinned by
native/pom.xml's `native.graalvm.version`, and the two are compiled against each other. Let them
drift and the build dies at feature registration with:

    NoSuchMethodError: OptimizedTruffleRuntime.getLoopNodeFactory()

which names neither the pom nor the workflow, so the cause has to be known rather than read.

This has now shipped twice from a Dependabot bump nobody could refuse in time (3fecba4bf ->
25.2.4, then b53c48c1e2 -> 25.3.4.1), each time taking out every leg of the Native Image workflow.
A comment in the pom did not stop the second one: dependabot-auto-merge.yml merges a Dependabot PR
on one approval with no green-CI condition, and a merge pushed with GITHUB_TOKEN starts no
workflow run, so main never re-tests it either. Hence a check that runs in the always-on `lint`
job on every PR.

Two things are enforced:

  1. `native.graalvm.version` equals the version every graalvm/setup-graalvm step in
     native-image.yml installs. That is the step's `version` input when it has one and its
     `java-version` otherwise, which is the same precedence setup-graalvm's own main.ts applies
     (`graalVMVersion.length > 0 ? setUpGraalVMJDKCE(...) : setUpGraalVMJDKCEByJavaVersion(...)`).
  2. That version is parseable as three-component semver. Both of setup-graalvm's CE resolution
     paths require it, so a version that is not is not installable by ANY input:

       - `java-version` (setUpGraalVMJDKCEByJavaVersion) throws outright unless the resolved
         version splits into exactly three dot-components, then fetches the `jdk-<version>` release.
       - `version` (setUpGraalVMJDKCE -> findReleaseTag -> findLatestGraalVMCEVersion) looks up the
         `graal-<version>` tag first and the `jdk-<version>` tag second, but skips any matching tag
         that `semver.valid()` rejects.

     This is what makes 25.3.4.1 unreachable: the tag `graal-25.3.4.1` exists, but node-semver
     rejects four numeric components, so findLatestGraalVMCEVersion logs "Skipping unexpected
     GraalVM CE release 25.3.4.1", finds no `jdk-25.3.4.1` either, and throws. Note the corollary,
     because it is easy to get backwards: an INTERMEDIATE release is not itself the problem.
     `graal-25.2.4` is an intermediate release with a three-component tag, and
     `version: "25.2.4"` installs it fine - it is only unreachable through `java-version`, which
     can address `jdk-*` tags alone.

If native-image.yml stops using graalvm/setup-graalvm, this check fails rather than passing
vacuously: a guard that silently stops guarding is how the pin drifted in the first place.

Usage:
  check-native-graalvm-pin.py [pom] [workflow]   # defaults to the repository's own two files

@author Luca Garulli (l.garulli@arcadedata.com)
"""

import re
import sys
from pathlib import Path

import yaml

SETUP_ACTION = "graalvm/setup-graalvm"
POM_PROPERTY = "native.graalvm.version"

# What node-semver's semver.valid() accepts, which is what both of setup-graalvm's CE resolution
# paths require: exactly three numeric components, e.g. 25.0.2. See the module docstring.
INSTALLABLE_VERSION = re.compile(r"^\d+\.\d+\.\d+$")

PROPERTY_PATTERN = re.compile(rf"<{re.escape(POM_PROPERTY)}>\s*([^<\s]+)\s*</{re.escape(POM_PROPERTY)}>")


def pinned_version(pom):
    """Returns the value of native.graalvm.version, or None when the property is absent."""
    match = PROPERTY_PATTERN.search(pom.read_text())
    return match.group(1) if match else None


def builder_versions(workflow):
    """Yields (job, step name, input name, version) for every graalvm/setup-graalvm step.

    `version` wins over `java-version` when both are set, matching setup-graalvm's own main.ts.
    """
    document = yaml.safe_load(workflow.read_text())

    for job_id, job in (document.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            uses = step.get("uses", "")
            # `uses` carries a SHA pin and a version comment, so match the action, not the whole string.
            if not uses.startswith(f"{SETUP_ACTION}@"):
                continue
            with_block = step.get("with") or {}
            for input_name in ("version", "java-version"):
                value = with_block.get(input_name)
                if value is not None and str(value) != "":
                    yield job_id, step.get("name", uses), input_name, str(value)
                    break
            else:
                yield job_id, step.get("name", uses), None, None


def main(argv):
    root = Path(__file__).resolve().parents[2]
    pom = Path(argv[1]) if len(argv) > 1 else root / "native" / "pom.xml"
    workflow = Path(argv[2]) if len(argv) > 2 else root / ".github" / "workflows" / "native-image.yml"

    for path in (pom, workflow):
        if not path.is_file():
            print(f"{path}: not found")
            return 1

    pinned = pinned_version(pom)
    if pinned is None:
        print(f"{pom}: no <{POM_PROPERTY}> property found.")
        print("The native image build pins the Truffle/polyglot artifacts through that property; if it")
        print("was renamed, rename it here too rather than dropping the check.")
        return 1

    try:
        setups = list(builder_versions(workflow))
    except yaml.YAMLError as e:
        print(f"{workflow}: not parseable as YAML ({e})")
        return 1

    if not setups:
        print(f"{workflow}: no {SETUP_ACTION} step found.")
        print(f"This check exists to keep the version that step installs equal to {POM_PROPERTY}")
        print(f"({pinned}). If the builder is now installed some other way, teach this script how to read")
        print("it - leaving the check to pass on nothing is how the pin drifted before.")
        return 1

    problems = []

    if not INSTALLABLE_VERSION.match(pinned):
        problems.append(
            f"{pom}: {POM_PROPERTY} is {pinned}, which node-semver's semver.valid() rejects "
            f"(it takes exactly three numeric components). Both of setup-graalvm's CE resolution "
            f"paths require that, so no builder can be installed at this version through either "
            f"`java-version` or `version` - the matching graal-* tag, if one exists, is skipped as "
            f"an unexpected release. Pin a three-component version instead."
        )

    for job_id, step_name, input_name, version in setups:
        if version is None:
            problems.append(
                f"{workflow}: job '{job_id}', step '{step_name}' sets neither `version` nor "
                f"`java-version`, so the builder is unpinned."
            )
        elif version != pinned:
            problems.append(
                f"{workflow}: job '{job_id}', step '{step_name}' installs {input_name} {version}, "
                f"but {pom}'s {POM_PROPERTY} is {pinned}."
            )
        elif step_name and re.search(r"\d+\.\d+\.\d+", step_name) and pinned not in step_name:
            problems.append(
                f"{workflow}: job '{job_id}', step name '{step_name}' names a version other than the "
                f"{pinned} it actually installs."
            )

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("The native image embeds GraalJS, so a builder/Truffle skew aborts the image build with")
        print("NoSuchMethodError OptimizedTruffleRuntime.getLoopNodeFactory() - an error that names")
        print("neither file. Move both sides together, or neither.")
        return 1

    print(f"GraalVM pin consistent: {POM_PROPERTY}={pinned} matches {len(setups)} {SETUP_ACTION} step(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
