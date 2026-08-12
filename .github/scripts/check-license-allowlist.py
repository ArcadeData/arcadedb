#!/usr/bin/env python3
#
# Fails when a Maven dependency in the reactor declares a license that is not on
# ArcadeDB's allow-list (CLAUDE.md's "General design principles" section).
#
# Context (issue #5651): the dependency-review-config.yml deny-list and the npm
# `license-checker --onlyAllow` gate both stop known-bad licenses, but only the npm
# side had an allow-list - a Maven dependency under a license that was neither
# explicitly allowed nor explicitly denied (MPL-2.0, CDDL, a bare GPL, ...) passed
# both gates silently. This script is the Maven-side `--onlyAllow`.
#
# It supersedes the old "grep for GPL/AGPL/SSPL" deny-only step: an allow-list is
# strictly stronger than a deny-list (anything not allowed is denied, whatever it is
# called), and a naive substring grep for "GPL" mis-fires on dependencies this
# project already ships under a license that merely mentions GPL in passing, e.g.
# "GNU General Public License (GPL), version 2, with the Classpath exception"
# (org.openjdk.jmh, test/benchmark-scope only) or "CDDL + GPLv2 with classpath
# exception" (javax.annotation-api) - the classpath exception is exactly what makes
# those safe to depend on without copyleft attaching to this project.
#
# Input is a THIRD-PARTY.txt produced by org.codehaus.mojo:license-maven-plugin's
# aggregate-add-third-party goal, invoked via its bare Maven coordinates (no pinned
# version) so it aggregates the whole reactor and picks up the excludedLicenses /
# licenseMerges <configuration> declared for the plugin in the root pom.xml, e.g.:
#   mvn org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party
# Pinning an explicit version on the command line bypasses both and reintroduces the
# gap this closes (that is exactly how license-compliance.yml regressed before).
#
# ALLOWED_CLAUSES below is a curated allow-list of exact license clause strings, not
# a regex/substring guess: license-maven-plugin's raw dependency metadata is messy
# free text (the same license shows up under a dozen spellings - "Apache License
# 2.0", "The Apache Software License, Version 2.0", "Apache 2", ...) and a license
# that is actually fine can share a substring with one that is not (e.g. plain
# "GPL" is a substring of the classpath-exception variant above). Matching exact,
# reviewed strings means an unrecognized spelling fails closed - loudly, with the
# offending line printed - rather than silently passing or silently blocking a
# dependency someone just phrased differently in their POM.
#
# Usage:
#   check-license-allowlist.py <path-to-THIRD-PARTY.txt>
#
import sys

# Every family in CLAUDE.md's ALLOWED list, plus ISC (already used - org.mindrot:jbcrypt
# - and already allowed on the npm side; CLAUDE.md's Maven list omitting it looks like an
# oversight rather than a deliberate call), plus three narrow, evidence-based additions
# made together with this script (see CLAUDE.md for the reasoning):
#   - MPL-2.0 and CDDL, both restricted to "for libraries" the same way LGPL 2.1+ already
#     is: file-level weak copyleft that imposes no obligation on a project that merely
#     depends on the library unmodified. Already shipping: org.mozilla:rhino (MPL-2.0),
#     javax.annotation:javax.annotation-api (CDDL + GPLv2 with classpath exception).
#   - GPL-2.0 WITH the Classpath Exception specifically (never a bare GPL/AGPL/SSPL): the
#     exception is the standard OpenJDK-ecosystem mechanism for making a GPL-licensed jar
#     safe to link against without the copyleft attaching to the linking project. Already
#     shipping: org.openjdk.jmh:jmh-core / jmh-generator-annprocess (test/benchmark scope).
#
# Each entry is one license clause exactly as license-maven-plugin renders it - i.e. the
# text of one top-level "(...)" group in a THIRD-PARTY.txt heading, without its own outer
# parentheses. Keep this in sync with CLAUDE.md's ALLOWED list.
ALLOWED_CLAUSES = {
    # Apache License 2.0 - every spelling actually seen, plus the two obvious others.
    "Apache 2",
    "Apache 2.0",
    "Apache-2.0",
    "Apache License 2.0",
    "Apache License Version 2.0",
    "Apache License, Version 2",
    "Apache License, Version 2.0",
    "Apache Software License, version 2.0",
    "The Apache License, Version 2.0",
    "The Apache Software License, Version 2.0",
    # MIT.
    "MIT",
    "MIT License",
    "MIT license",
    "MIT-0",
    # BSD (2/3-Clause), including the unusual per-project spellings Maven Central reports.
    "BSD",
    "BSD 2-Clause",
    "BSD-2-Clause",
    "BSD 3-Clause",
    "BSD-3-Clause",
    "BSD 3-Clause \"New\" or \"Revised\" License (BSD-3-Clause)",
    "BSD Licence 3",
    "BSD New license",
    "The BSD License",
    "Go License",  # com.google.re2j:re2j - the Go project's own BSD-3-Clause-style license.
    # EPL 1.0 / 2.0.
    "EPL 1.0",
    "EPL-1.0",
    "Eclipse Public License 1.0",
    "Eclipse Public License - v 1.0",
    "EPL 2.0",
    "EPL-2.0",
    "Eclipse Public License v2.0",
    "Eclipse Public License, Version 2.0",
    "Eclipse Public License - v 2.0",
    # UPL 1.0.
    "UPL 1.0",
    "UPL-1.0",
    "Universal Permissive License, Version 1.0",
    # EDL 1.0.
    "EDL 1.0",
    "EDL-1.0",
    "Eclipse Distribution License - v 1.0",
    # LGPL 2.1+ ("or later" reaches LGPLv3, which is the "or greater" spelling below).
    "LGPL 2.1",
    "LGPL-2.1",
    "LGPL-2.1-only",
    "LGPL-2.1-or-later",
    "GNU Lesser General Public License",
    "GNU Lesser General Public License v2.1",
    "Lesser General Public License, version 3 or greater",
    # CC0 / Public Domain.
    "CC0",
    "CC0-1.0",
    "CC0 1.0 Universal",
    "Public Domain",
    "Public Domain, per Creative Commons CC0",
    # ISC - MIT/BSD-2-Clause equivalent; already used (org.mindrot:jbcrypt) and already
    # allowed on the npm side.
    "ISC",
    # Unicode/ICU License - permissive, MIT-style; already used (GraalVM's shadowed icu4j).
    "Unicode/ICU License",
    # MPL-2.0, for libraries (see the module docstring above).
    "Mozilla Public License, Version 2.0",
    "MPL-2.0",
    # CDDL, for libraries (see the module docstring above).
    "CDDL + GPLv2 with classpath exception",
    "CDDL-1.0",
    "CDDL-1.1",
    # GPL-2.0 WITH Classpath Exception specifically - never a bare GPL/AGPL/SSPL (see the
    # module docstring above).
    "GNU General Public License (GPL), version 2, with the Classpath exception",
    "GPL2 w/ CPE",
    "GPL-2.0-with-classpath-exception",
}


def leading_license_clauses(line: str) -> tuple[list[str], str]:
    """Splits the leading run of balanced "(...)" groups off the front of a
    THIRD-PARTY.txt dependency line, e.g.

        (EPL 2.0) (GPL2 w/ CPE) Jakarta Annotations API (jakarta.annotation:...)

    returns (["EPL 2.0", "GPL2 w/ CPE"], "Jakarta Annotations API (jakarta.annotation:...)").
    Nesting inside a single clause - e.g. "(GNU General Public License (GPL), version 2,
    with the Classpath exception)" - stays part of that one clause rather than splitting it,
    because depth only returns to 0, and the group only closes, at the outer ")".
    """
    clauses = []
    i, n = 0, len(line)
    while i < n and line[i] == "(":
        depth = 0
        j = i
        while j < n:
            if line[j] == "(":
                depth += 1
            elif line[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if depth != 0:
            break  # Unbalanced - stop treating the rest of the line as license clauses.
        clauses.append(line[i + 1:j].strip())
        i = j + 1
        while i < n and line[i] == " ":
            i += 1
    return clauses, line[i:]


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <path-to-THIRD-PARTY.txt>", file=sys.stderr)
        return 2

    report_path = sys.argv[1]
    try:
        with open(report_path, encoding="utf-8") as f:
            lines = f.readlines()
    except OSError as e:
        print(f"License report not found: {report_path} ({e})", file=sys.stderr)
        return 2

    dependency_lines = 0
    violations = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line.startswith("("):
            continue  # Preamble ("Lists of N third-party dependencies.") or a blank line.
        clauses, rest = leading_license_clauses(line)
        if not clauses:
            continue
        dependency_lines += 1
        if not any(clause in ALLOWED_CLAUSES for clause in clauses):
            violations.append(line)

    if dependency_lines == 0:
        print(f"No dependency lines found in {report_path} - nothing to check.", file=sys.stderr)
        print("This usually means the report was generated with the non-aggregate goal (which", file=sys.stderr)
        print("only lists the root aggregator pom's own, near-empty direct dependencies) instead", file=sys.stderr)
        print("of aggregate-add-third-party, which covers the whole reactor.", file=sys.stderr)
        return 2

    if violations:
        print("Dependencies with licenses outside ArcadeDB's allow-list (see CLAUDE.md):", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        print(file=sys.stderr)
        print("If this license should be permitted: get maintainer sign-off, add its exact", file=sys.stderr)
        print("clause text to ALLOWED_CLAUSES in this script, and update CLAUDE.md's ALLOWED", file=sys.stderr)
        print("list and ATTRIBUTIONS.md to match.", file=sys.stderr)
        return 1

    print(f"OK: all {dependency_lines} Maven dependency license(s) are on the allow-list.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
