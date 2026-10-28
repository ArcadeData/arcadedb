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

"""Fails when a claude-code-action `prompt` tells the model to change something on GitHub that the
job gives it no way to change.

#10600: claude-code-review.yml told the model "add the label ... to the pr" while the job held
`pull-requests: read` and `--allowed-tools` listed only read-only `gh` commands plus `gh pr comment`.
The model had no tool to even attempt the label, so it never produced a permission error either: the
instruction was a security control on paper that could never fire, and nothing in a run log said so.
A prompt and a permissions block live in two different parts of the file, so only a check that reads
both can see the mismatch.

For every step that uses anthropics/claude-code-action with a `prompt`, each mutating instruction in
the prompt (add or remove a label, close, merge, approve, assign, push) must be backed by BOTH:

  - a `write` grant in the job's `permissions` (or the workflow's, when the job sets none) on one of
    the scopes that operation needs, and
  - a tool in `--allowed-tools` that performs it (`gh pr edit`, `gh api`, `git push`, ...).

An instruction preceded in the same sentence by a negation ("do not", "never", ...) is a
prohibition, not an instruction, and is not checked. The preferred fix is usually not to grant the
model a write tool at all, but to have it return a verdict (`--json-schema`, or a final line) and let
a later job step perform the mutation - then the prompt asks for a verdict, and this check is
satisfied by construction.

Limits: the phrases below are matched literally, so a prompt that asks for a mutation in other words
is not seen. Extend MUTATIONS when a new phrasing appears.

Usage:
  check-claude-prompt-mutations.py [workflow ...]   # defaults to .github/workflows/*.yml

@author Roberto Franchini (r.franchini@arcadedata.com)
"""

import re
import sys
from pathlib import Path

import yaml

ACTION = "anthropics/claude-code-action"

ARTICLE = r"(?:the\s+|this\s+|that\s+|a\s+|an\s+|its\s+)?"
TARGET = r"(?:pr|pull\s+request|issue)\b"

# (what, prompt phrase, scopes any one of which must be `write`, tool prefixes any one of which must be allowed)
MUTATIONS = [
    (
        "label",
        re.compile(r"\b(?:add|apply|put|set|remove)\s+" + ARTICLE + r"labels?\b"),
        ("pull-requests", "issues"),
        ("gh pr edit", "gh issue edit", "gh label", "gh api"),
    ),
    (
        "close",
        re.compile(r"\b(?:close|reopen)\s+" + ARTICLE + TARGET),
        ("pull-requests", "issues"),
        ("gh pr close", "gh issue close", "gh pr reopen", "gh issue reopen", "gh api"),
    ),
    (
        "merge",
        re.compile(r"\bmerge\s+" + ARTICLE + r"(?:pr|pull\s+request)\b"),
        ("contents",),
        ("gh pr merge", "gh api"),
    ),
    (
        "approve",
        re.compile(r"\b(?:approve|request\s+changes\s+on)\s+" + ARTICLE + r"(?:pr|pull\s+request)\b"),
        ("pull-requests",),
        ("gh pr review", "gh api"),
    ),
    (
        "assign",
        re.compile(r"\bassign\s+(?:it|them|" + ARTICLE + TARGET + r")"),
        ("pull-requests", "issues"),
        ("gh pr edit", "gh issue edit", "gh api"),
    ),
    (
        "push",
        re.compile(r"\b(?:push|commit)\s+(?:the\s+|your\s+)?(?:changes|commits?|fix(?:es)?|branch)\b"),
        ("contents",),
        ("git push",),
    ),
]

NEGATION = re.compile(r"\b(?:not|never|don't|do\s+not|no|without)\b")

ALLOWED_TOOLS = re.compile(r"--allowed-?tools(?:=|\s+)(\"[^\"]*\"|'[^']*'|\S+)", re.IGNORECASE)
BASH_TOOL = re.compile(r"^Bash(?:\((.*)\))?$")


def negated(prompt, start):
    """True when the sentence holding the match denies it ("do not add a label")."""
    sentence_start = max(prompt.rfind(".", 0, start), prompt.rfind("\n\n", 0, start), prompt.rfind("\n-", 0, start))
    return bool(NEGATION.search(prompt[sentence_start + 1 : start]))


def allowed_commands(claude_args):
    """The command prefixes the model may run through Bash; '' means any command."""
    commands = []
    for match in ALLOWED_TOOLS.finditer(claude_args or ""):
        for tool in match.group(1).strip("\"'").split(","):
            bash = BASH_TOOL.match(tool.strip())
            if not bash:
                continue
            pattern = (bash.group(1) or "").strip()
            commands.append(re.sub(r"[:\s]*\*$", "", pattern).strip())
    return commands


def tool_allowed(commands, prefixes):
    for command in commands:
        for prefix in prefixes:
            # `Bash(gh:*)` covers `gh pr edit`; `Bash(gh pr edit 5:*)` is still a `gh pr edit`.
            if prefix.startswith(command) or command.startswith(prefix):
                return True
    return False


def write_scopes(permissions):
    if permissions == "write-all":
        return None  # every scope
    if not isinstance(permissions, dict):
        return set()
    return {scope for scope, level in permissions.items() if level == "write"}


def main(argv):
    root = Path(__file__).resolve().parents[2]
    workflows = [Path(a) for a in argv[1:]] or sorted((root / ".github" / "workflows").glob("*.yml"))

    if not workflows:
        print("no workflow files to check")
        return 1

    problems = []
    prompts = 0

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

        for job_id, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            permissions = job["permissions"] if "permissions" in job else document.get("permissions")
            granted = write_scopes(permissions)
            for step in job.get("steps") or []:
                if not isinstance(step, dict) or ACTION not in str(step.get("uses", "")):
                    continue
                inputs = step.get("with") or {}
                prompt = inputs.get("prompt")
                if not isinstance(prompt, str):
                    continue
                prompts += 1
                text = prompt.lower()
                commands = allowed_commands(str(inputs.get("claude_args", "")))
                where = f"{workflow}: job '{job_id}', step '{step.get('name', '<unnamed step>')}'"
                for what, phrase, scopes, prefixes in MUTATIONS:
                    for match in phrase.finditer(text):
                        if negated(text, match.start()):
                            continue
                        missing = []
                        if granted is not None and not granted.intersection(scopes):
                            missing.append(f"a `write` permission on {' or '.join(scopes)}")
                        if not tool_allowed(commands, prefixes):
                            missing.append(f"a tool in --allowed-tools that can do it ({', '.join(prefixes)})")
                        if missing:
                            problems.append(
                                f"{where}: the prompt asks the model to {what} (\"{match.group(0)}\") but the job "
                                f"lacks {' and '.join(missing)}"
                            )
                        break

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("A prompt instruction the model has no tool or permission to carry out is silently dropped: it")
        print("never even fails, so the control it describes does not exist (#10600). Either have the model")
        print("return a verdict and let a later job step make the change, or grant both the scope and the tool.")
        return 1

    print(f"claude-code-action prompts are executable: {prompts} prompt(s) across {len(workflows)} workflow(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
