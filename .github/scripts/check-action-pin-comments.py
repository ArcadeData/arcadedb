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

"""Fails when the version comment beside a SHA-pinned action is not a tag at that SHA.

Every third-party action in .github/workflows is pinned as

    uses: <owner>/<repo>[/<path>]@<40-hex sha> # <version>

The `lint` job's ensure-sha-pinned-actions step enforces the first half of that rule: the reference
IS a commit SHA. Nothing enforced the second half, and it drifted (#10602): the three CodeQL steps
ran v4.39.0 commented `# v3.29.5`, carried through three Dependabot bumps, and setup-graalvm ran
v1.6.7 commented `# v1.6.1`. A SHA is not human-readable, so the comment is the only statement a
reviewer, an auditor or somebody triaging an advisory can read about which release runs here. A
wrong one is worse than none. Dependabot does not repair it either: it rewrites the comment on a
bump only when the comment still names the release the OLD SHA was, so once it is wrong it stays
wrong.

For every such pin this resolves the SHA against the action repository's tags (one
`git ls-remote --tags` per distinct repository, no token needed) and fails when the comment is not
among the tags at that SHA. A tag counts whether it points at the commit directly or through an
annotated tag object (the peeled `^{}` ref). A moving major tag such as `v4` may also sit on the
SHA; the comment may name either, but convention here is the full release.

The comment is the first whitespace-separated token after `#`, so `# v4.39.1 (security fix)` checks
`v4.39.1` while `# tag=v4.39.1` checks `tag=v4.39.1` and fails: write the bare tag first. A `uses:`
is matched one line at a time; every pin in this repository fits on one line, keep it that way.

A SHA pin with no version comment fails too, and so does a repository whose tags cannot be listed:
a check that passes because it could not look is how the comments drifted in the first place.

Usage:
  check-action-pin-comments.py [--tags-dir DIR] [workflow ...]   # defaults to .github/workflows/*.yml

  --tags-dir DIR   read `git ls-remote --tags` output from DIR/<owner>/<repo>.txt instead of the
                   network; used by the self-test so it does not depend on upstream tags

@author Roberto Franchini (r.franchini@arcadedata.com)
"""

import argparse
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# `uses: owner/repo[/path]@<sha>` optionally followed by `# <version>`. Local (`./`) and `docker://`
# references have no owner/repo@sha shape and are not matched.
PIN = re.compile(
    r"""^\s*(?:-\s+)?uses:\s*["']?(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+)(?P<path>/[^@\s"']*)?"""
    r"""@(?P<sha>[0-9a-f]{40})["']?(?:\s+#\s*(?P<comment>\S+))?""")

# Short enough that an outage fails the lint job in about a minute rather than several.
LS_REMOTE_TIMEOUT_SECONDS = 30
RETRY_DELAY_SECONDS = 5


def collect_pins(workflows):
    """Returns [(file, line number, owner, repo, action, sha, comment-or-None)]."""
    pins = []
    for workflow in workflows:
        for number, line in enumerate(workflow.read_text().splitlines(), start=1):
            match = PIN.match(line)
            if match:
                action = f"{match['owner']}/{match['repo']}{match['path'] or ''}"
                pins.append((workflow, number, match["owner"], match["repo"], action, match["sha"], match["comment"]))
    return pins


def parse_ls_remote(text):
    """Maps each SHA to the tag names at it, direct or peeled from an annotated tag object."""
    tags = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2 or not parts[1].startswith("refs/tags/"):
            continue
        name = parts[1][len("refs/tags/"):]
        if name.endswith("^{}"):
            name = name[:-3]
        tags.setdefault(parts[0], set()).add(name)
    return tags


def list_tags(owner, repo, tags_dir):
    """Returns (sha -> tags, None) or (None, error message)."""
    if tags_dir is not None:
        fixture = tags_dir / owner / f"{repo}.txt"
        if not fixture.is_file():
            return None, f"no tag listing at {fixture}"
        return parse_ls_remote(fixture.read_text()), None

    url = f"https://github.com/{owner}/{repo}"
    last_error = None
    # One retry: a single transient network error should not turn every open PR's lint red.
    for attempt in range(2):
        if attempt:
            time.sleep(RETRY_DELAY_SECONDS)
        try:
            result = subprocess.run(["git", "ls-remote", "--tags", url], capture_output=True, text=True,
                                    timeout=LS_REMOTE_TIMEOUT_SECONDS,
                                    env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
        except (OSError, subprocess.TimeoutExpired) as e:
            last_error = str(e)
            continue
        if result.returncode == 0:
            return parse_ls_remote(result.stdout), None
        last_error = result.stderr.strip() or f"git ls-remote exited {result.returncode}"
    return None, f"cannot list the tags of {url}: {last_error}"


def main(argv):
    parser = argparse.ArgumentParser(description="Checks that each SHA-pinned action's version comment is a tag at its SHA.")
    parser.add_argument("--tags-dir", type=Path, help="read `git ls-remote --tags` output from DIR/<owner>/<repo>.txt")
    parser.add_argument("workflows", nargs="*", type=Path, help="workflow files (default: .github/workflows/*.yml)")
    options = parser.parse_args(argv[1:])
    tags_dir = options.tags_dir

    root = Path(__file__).resolve().parents[2]
    workflows = options.workflows or sorted(
        p for p in (root / ".github" / "workflows").iterdir() if p.suffix in (".yml", ".yaml"))

    for workflow in workflows:
        if not workflow.is_file():
            print(f"{workflow}: not found")
            return 1

    pins = collect_pins(workflows)
    repositories = sorted({(owner, repo) for _, _, owner, repo, _, _, _ in pins})
    with ThreadPoolExecutor(max_workers=8) as pool:
        listings = dict(zip(repositories, pool.map(lambda r: list_tags(r[0], r[1], tags_dir), repositories)))

    problems = []
    for workflow, number, owner, repo, action, sha, comment in pins:
        where = f"{workflow}:{number}: {action}@{sha[:12]}"
        tags_by_sha, error = listings[(owner, repo)]
        if error is not None:
            problems.append(f"{where}: {error}")
            continue
        tags = sorted(tags_by_sha.get(sha, ()))
        if comment is None:
            problems.append(f"{where} has no version comment; append `# <tag>` naming the release at "
                            f"that SHA ({', '.join(tags) or 'no tag points at it'})")
        elif comment not in tags:
            problems.append(f"{where} is commented `# {comment}` but the tags at that SHA are "
                            f"[{', '.join(tags) or 'none'}]")

    if problems:
        print("SHA-pinned actions whose version comment does not name the release at the SHA:")
        for problem in problems:
            print(f"  {problem}")
        print()
        print("The comment is the only readable statement of which release runs; correct it to a tag")
        print("listed above (the full release, not the moving major tag), or re-pin to the SHA of the")
        print("release the comment names.")
        return 1

    print(f"{len(pins)} SHA-pinned action references across {len(repositories)} repositories name the "
          f"release at their SHA.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
