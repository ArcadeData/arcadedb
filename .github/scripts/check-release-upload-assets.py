#!/usr/bin/env python3
"""Fails when a `gh release upload` step names its assets with a shell glob.

`gh release upload` expands each asset argument itself and treats a pattern that matches nothing as
a fatal error - not as "nothing to upload":

    no matches found for `native/target/arcadedb-*.zip`

That is what took out the whole 26.9.1 Native Image release (run 33796693420). The step passed
three patterns unconditionally:

    gh release upload "$TAG" \\
      native/target/arcadedb-*.tar.gz native/target/arcadedb-*.zip native/target/*.sha256 --clobber

but each matrix leg packages exactly ONE archive: the three Unix legs produce a .tar.gz and no
.zip, the Windows leg a .zip and no .tar.gz. So on every leg one pattern matched nothing and every
leg failed - after the binary had been built, smoke-tested, packaged and uploaded as a workflow
artifact. The build was fine; only the publish step was wrong, and it had never run before, because
this step is gated on `github.event_name == 'release'` and 26.8.1's native assets were uploaded by
hand.

A glob is the wrong tool here whatever the pattern: the step cannot see whether it matched, and a
matrix makes "always matches" a per-leg claim rather than a property of the workflow. Name the
assets, or compute the names in an earlier step - which is what native-image.yml's "Package
artifact" step now does, publishing the archive it actually created as a step output, and what
publish-contract.yml has always done by interpolating "$TAG" into each filename.

If a future step genuinely needs a pattern, expand it in the shell first and fail on zero matches
there, where the error can say which leg and which directory; do not hand the pattern to `gh`.

Usage:
  check-release-upload-assets.py [workflow ...]   # defaults to .github/workflows/*.yml

@author Luca Garulli (l.garulli@arcadedata.com)
"""

import re
import shlex
import sys
from pathlib import Path

import yaml

COMMAND = ("gh", "release", "upload")

# Shell/`filepath.Glob` metacharacters. `\` is deliberately absent: it is an escape, not a wildcard,
# and flagging it would reject ordinary Windows paths.
GLOB_CHARS = "*?["

# `${{ ... }}` can hold quotes and parentheses that shlex would mis-tokenize (and, unbalanced, that
# it refuses outright). The expression's VALUE is not knowable here anyway, so collapse each one to
# a single opaque word before tokenizing.
GHA_EXPRESSION = re.compile(r"\$\{\{.*?\}\}", re.DOTALL)
EXPRESSION_PLACEHOLDER = "GHA_EXPRESSION"

# The only `gh release upload` flag that consumes the argument after it.
VALUE_FLAGS = {"-R", "--repo"}


def run_scripts(document):
    """Yields (job id, step name, script) for every step in the workflow that has a `run:`."""
    for job_id, job in (document.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        for step in job.get("steps") or []:
            if not isinstance(step, dict):
                continue
            script = step.get("run")
            if isinstance(script, str):
                yield job_id, step.get("name", "<unnamed step>"), script


def upload_assets(script):
    """Yields (asset argument, whole invocation) for every `gh release upload` in one run script.

    Only the positional arguments AFTER the tag are yielded - those are the assets. Flags, the
    values they consume, and the tag itself are skipped.
    """
    # Fold `\`-continued lines so a multi-line invocation is tokenized as one command, then split
    # on the separators that end a command. `&&`/`||` are covered by splitting on `&` and `|`.
    folded = re.sub(r"\\\n", " ", GHA_EXPRESSION.sub(EXPRESSION_PLACEHOLDER, script))
    for command in re.split(r"[\n;&|]+", folded):
        if "gh" not in command or "release" not in command:
            continue
        words = shlex.split(command, comments=True)
        for start in range(len(words) - len(COMMAND) + 1):
            if tuple(words[start : start + len(COMMAND)]) != COMMAND:
                continue
            positionals = 0
            skip_next = False
            for word in words[start + len(COMMAND) :]:
                if skip_next:
                    skip_next = False
                    continue
                if word in VALUE_FLAGS:
                    skip_next = True
                    continue
                if word.startswith("-"):
                    continue
                positionals += 1
                if positionals == 1:  # the tag
                    continue
                yield word, " ".join(command.split())


def main(argv):
    root = Path(__file__).resolve().parents[2]
    workflows = [Path(a) for a in argv[1:]] or sorted((root / ".github" / "workflows").glob("*.yml"))

    if not workflows:
        print("no workflow files to check")
        return 1

    problems = []
    checked = 0

    for workflow in workflows:
        if not workflow.is_file():
            print(f"{workflow}: not found")
            return 1
        try:
            document = yaml.safe_load(workflow.read_text())
        except yaml.YAMLError as e:
            print(f"{workflow}: not parseable as YAML ({e})")
            return 1
        if not isinstance(document, dict):
            continue

        for job_id, step_name, script in run_scripts(document):
            for asset, command in upload_assets(script):
                checked += 1
                if asset == EXPRESSION_PLACEHOLDER:
                    continue
                bad = sorted({c for c in GLOB_CHARS if c in asset})
                if bad:
                    problems.append(
                        f"{workflow}: job '{job_id}', step '{step_name}' uploads the asset "
                        f"pattern '{asset}' (glob character{'s' if len(bad) > 1 else ''} "
                        f"{', '.join(bad)}):\n    {command}"
                    )

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("`gh release upload` globs each asset argument itself and EXITS NONZERO when a pattern")
        print("matches nothing, so a step that uploads one archive per matrix leg fails on every leg")
        print("that does not produce the other one. That is how run 33796693420 lost every native")
        print("26.9.1 asset after building them all successfully.")
        print("Name the assets, or compute their names in an earlier step and pass them through a")
        print("step output - see native-image.yml's 'Package artifact' step.")
        return 1

    print(f"release upload assets are explicit: {checked} asset argument(s) across {len(workflows)} workflow(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
