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

"""Fails when a workflow step runs Maven as anything but the repository's Maven wrapper (`./mvnw`).

`.mvn/wrapper/maven-wrapper.properties` pins the Maven version the build is reviewed against. The
runner image's `mvn` is whatever version GitHub installed that week, so a step that calls it picks up
a Maven upgrade with no change in this repository and no review. That has broken publishing twice:

- 2026-10-08 the hosted runner image moved from Maven 3.9.16 to 3.10.0 and every `Java CI - deploy`
  snapshot upload failed with HTTP 401 for a week (#10023);
- a week later the same runner Maven made central-publishing-maven-plugin put local repository
  metadata into the release bundle, and the Central Portal rejected the 26.10.2 release (#10111).

#10111 moved the release workflow to `./mvnw`; #10134 moved every other step and added this check, so
a step written later cannot reintroduce the runner's Maven either.

Flagged: a command whose program is `mvn`, `mvn.cmd` or the Maven daemon (`mvnd`), by bare name or by
path. Accepted: `mvnw` / `mvnw.cmd` by any path (`./mvnw`, `../mvnw`). Each `run:` of a workflow
step or of a composite action step is checked, and so is every shell script of this repository that a
step runs, either as a command (`./test-builder-local.sh`) or through a shell (`bash build.sh`,
`source env.sh`), resolved against the step's or the job's working directory -
package/test-builder-local.sh ran the runner's mvn that way. The script given to `bash -c` / `sh -c`
is checked as a script of its own.

How a script is read: `\\` continuations are folded, comments are dropped, every `$(...)` and
backtick command substitution is checked as a script of its own, and the rest is tokenized like a
shell (quotes respected) and split into commands on newlines, `;`, `&`, `|`, `(` and `)` - which also
makes the body of a `case` pattern (`a) mvn verify ;;`) a command. A command's program is its first
word after any `NAME=value` assignments and shell keywords such as `then`, `do`, `!` or `{`. After a
prefix that runs another program (`sudo`, `env`, `time`, `timeout`, `xargs`, ...), the program is the
first Maven executable among its remaining words, else its first word that is not an option or a
number. Words that only mention Maven (an `echo "mvn ..."`, a comment, a file name such as
`mvn-deploy.yml`) are not programs and are not flagged.

A here-doc body is read as commands like any other line, so `bash <<EOF` with `mvn` in its body is
caught, and so - failing closed - is a here-doc that only writes such a line into a file.

Limits: Maven reached through `eval`, a `$'...'` string given to `bash -c`, or a script whose path
holds a variable other than a leading `$GITHUB_WORKSPACE` is NOT seen. Every Maven call in this repository's workflows is a plain
command in a step's `run:`; keep it that way, or extend this check alongside.

Usage:
  check-maven-wrapper.py [workflow-or-action.yml ...]
      # defaults to .github/workflows/*.yml|*.yaml and .github/actions/**/action.yml|action.yaml

@author Roberto Franchini (r.franchini@arcadedata.com)
"""

import re
import shlex
import sys
from pathlib import Path

import yaml

RUNNER_MAVEN = {"mvn", "mvn.cmd", "mvnd", "mvnd.cmd", "mvnd.sh"}
WRAPPER = {"mvnw", "mvnw.cmd"}

# Shell keywords that open a compound command whose body follows: the next word is the command.
SHELL_KEYWORDS = {"if", "then", "elif", "else", "while", "until", "do", "!", "{"}

# Programs that run another program given later on their command line, possibly after options of
# their own (`sudo -E mvn`, `env -i mvn`, `timeout 10m mvn`, `xargs -n1 mvn`).
COMMAND_RUNNERS = {
    "sudo", "time", "timeout", "exec", "command", "nohup", "env", "xargs", "builtin", "nice", "stdbuf",
}

# Shells: `bash -c "..."` runs a script given inline, `bash build.sh` / `source env.sh` a script file.
SHELLS = {"bash", "sh", "zsh", "dash", "source", "."}
SHELL_OPTIONS_WITH_ARGUMENT = {"-o", "+o", "-O", "+O", "--rcfile", "--init-file"}

# `$GITHUB_WORKSPACE` is the checkout root, so a script named through it is still a repository script.
WORKSPACE_PREFIX = re.compile(r"^\$(\{GITHUB_WORKSPACE\}|GITHUB_WORKSPACE)/")

NUMBER = re.compile(r"^[0-9.]+[smhd]?$")

SEPARATOR_CHARS = set(";&|()")
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")

# `${{ ... }}` can hold quotes that would mis-tokenize the command; its value is not knowable here anyway.
GHA_EXPRESSION = re.compile(r"\$\{\{.*?\}\}", re.DOTALL)
EXPRESSION_PLACEHOLDER = "GHA_EXPRESSION"


def closing_paren(script, start):
    """Index just past the `)` that closes the `$(` whose body starts at `start`, skipping quoted text."""
    depth = 1
    i = start
    n = len(script)
    while i < n:
        c = script[i]
        if c == "\\":
            i += 2
            continue
        if c in "'\"":
            end = script.find(c, i + 1)
            i = n if end < 0 else end + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return n


def extract_substitutions(script):
    """Splits a script into (outer text, [inner scripts]) for every `$(...)` and backtick substitution.

    The substitution is replaced by a placeholder word in the outer text, so the command it sits in
    stays one command, and its body is returned to be checked as a script of its own. Comments are
    dropped here, and nothing inside single-quoted text (`'...'`, `$'...'`) is taken as a
    substitution, as a shell would: a comment or a quoted fixture that mentions `$(mvn ...)` runs
    nothing."""
    inner = []
    out = []
    i = 0
    n = len(script)
    in_double = False
    while i < n:
        c = script[i]
        if c == "\\":
            out.append(script[i : i + 2])
            i += 2
        elif not in_double and (c == "'" or script.startswith("$'", i)):
            # `$'...'` honours backslash escapes, so `\'` does not end it; plain `'...'` has none.
            ansi = c == "$"
            j = i + (2 if ansi else 1)
            while j < n and script[j] != "'":
                j += 2 if ansi and script[j] == "\\" else 1
            # `'...'` is kept for the tokenizer (it may be the script of a `bash -c`); `$'...'` becomes a
            # placeholder word, as shlex does not know that quote form (`$'it\'s'`).
            out.append("QUOTED" if ansi else script[i : j + 1])
            i = j + 1
        elif not in_double and c == "#" and (i == 0 or script[i - 1] in " \t\n;&|()"):
            end = script.find("\n", i)
            i = n if end < 0 else end
        elif c == '"':
            in_double = not in_double
            out.append(c)
            i += 1
        elif script.startswith("$(", i) and not script.startswith("$((", i):
            j = closing_paren(script, i + 2)
            inner.append(script[i + 2 : j - 1] if j <= n and script[j - 1 : j] == ")" else script[i + 2 :])
            out.append("SUBSTITUTION")
            i = j
        elif c == "`":
            j = script.find("`", i + 1)
            if j < 0:
                j = n
            inner.append(script[i + 1 : j])
            out.append("SUBSTITUTION")
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out), inner


def tokenize(script):
    """Shell-like tokens of a script whose comments are already dropped, with `;&|()` runs as separate
    tokens and a `;` at every line end."""
    text = script.replace("\n", " ;\n")
    try:
        lexer = shlex.shlex(text, posix=True, punctuation_chars=";&|()")
        lexer.whitespace_split = True
        lexer.commenters = ""
        return list(lexer)
    except ValueError:
        # Unbalanced quotes: fall back to a plain split. It keeps every word, so it can only report
        # more commands than the shell would run, never fewer.
        tokens = []
        for chunk in re.split(r"([;&|()]+)", text):
            if chunk and set(chunk) <= SEPARATOR_CHARS:
                tokens.append(chunk)
            else:
                tokens.extend(chunk.split())
        return tokens


def program_of(words):
    """The index of the word a command runs as its program, or None for a command that runs nothing."""
    start = 0
    while start < len(words) and (ASSIGNMENT.match(words[start]) or words[start] in SHELL_KEYWORDS):
        start += 1
    if start >= len(words):
        return None
    if Path(words[start]).name not in COMMAND_RUNNERS:
        return start
    # A runner's own options may take arguments (`sudo -u builder mvn`), so a Maven executable
    # anywhere after it is the program; failing that, the first word that is not an option of it.
    rest = range(start + 1, len(words))
    for index in rest:
        if Path(words[index]).name in RUNNER_MAVEN | WRAPPER:
            return index
    for index in rest:
        word = words[index]
        if not (word.startswith("-") or ASSIGNMENT.match(word) or NUMBER.match(word)):
            inner = program_of(words[index:])
            return None if inner is None else index + inner
    return None


def programs(script):
    """Yields (program, its arguments, whole command) for every command in a run script, substitutions
    included."""
    folded = re.sub(r"\\\r?\n", " ", GHA_EXPRESSION.sub(EXPRESSION_PLACEHOLDER, script))
    outer, inner = extract_substitutions(folded)
    for body in inner:
        yield from programs(body)

    command = []
    for token in tokenize(outer) + [";"]:
        if token and set(token) <= SEPARATOR_CHARS:
            words = command
            command = []
            start = program_of(words)
            if start is not None:
                yield words[start], words[start + 1 :], " ".join(words[start:])
        else:
            command.append(token)


def default_working_directory(*holders):
    """The `defaults.run.working-directory` of the innermost holder (job, then workflow) that sets one."""
    for holder in holders:
        run = ((holder or {}).get("defaults") or {}).get("run") if isinstance(holder, dict) else None
        if isinstance(run, dict) and isinstance(run.get("working-directory"), str):
            return run["working-directory"]
    return None


def run_scripts(document):
    """Yields (location, run script, working directory or None) for every step of a workflow's jobs
    or of a composite action."""
    if not isinstance(document, dict):
        return
    step_lists = []
    for job_id, job in (document.get("jobs") or {}).items():
        if isinstance(job, dict):
            step_lists.append((f"job '{job_id}'", job.get("steps"), default_working_directory(job, document)))
    runs = document.get("runs")
    if isinstance(runs, dict):
        step_lists.append(("composite action", runs.get("steps"), None))
    for where, steps, job_directory in step_lists:
        for index, step in enumerate(steps or []):
            if isinstance(step, dict) and isinstance(step.get("run"), str):
                directory = step.get("working-directory")
                yield (
                    f"{where}, step '{step.get('name', f'#{index + 1}')}'",
                    step["run"],
                    directory if isinstance(directory, str) else job_directory,
                )


def repository_script(root, directory, program):
    """The shell script file a command runs, when it is one that exists on disk, else None."""
    program = WORKSPACE_PREFIX.sub(lambda _: f"{root}/", program)
    directory = WORKSPACE_PREFIX.sub(lambda _: f"{root}/", directory) if directory else directory
    if not program.endswith(".sh") or "$" in program:
        return None
    candidate = root / (directory or ".") / program
    return candidate.resolve() if candidate.is_file() else None


def default_files(root):
    """The workflows and composite actions of the repository at `root`."""
    github = root / ".github"
    files = sorted((github / "workflows").glob("*.yml")) + sorted((github / "workflows").glob("*.yaml"))
    files += sorted((github / "actions").glob("**/action.yml")) + sorted((github / "actions").glob("**/action.yaml"))
    return files


def shell_target(arguments):
    """For a shell's arguments: ("inline", script) for `-c script`, ("file", path) for a script file,
    else None. The shell's own options come first and may take an argument (`-o pipefail`,
    `-O extglob`), which is neither the script nor its file."""
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in SHELL_OPTIONS_WITH_ARGUMENT:
            index += 2
            continue
        if argument[:1] == "-" and argument[1:2] != "-" and "c" in argument[1:]:
            return ("inline", arguments[index + 1]) if index + 1 < len(arguments) else None
        if argument[:1] in ("-", "+"):
            index += 1
            continue
        return "file", argument
    return None


def main(argv):
    """Checks the given workflow / action files, or the repository's, and returns the exit status."""
    root = Path(__file__).resolve().parents[2]
    files = [Path(a) for a in argv[1:]] or default_files(root)

    if not files:
        print("no workflow files to check")
        return 1

    problems = []
    wrapped = 0

    for path in files:
        if not path.is_file():
            print(f"{path}: not found")
            return 1
        try:
            document = yaml.safe_load(path.read_text())
        except yaml.YAMLError as e:
            print(f"{path}: not parseable as YAML ({e})")
            return 1

        for where, script, directory in run_scripts(document):
            pending = [(where, script, directory)]
            seen = set()
            while pending:
                location, text, cwd = pending.pop()
                for program, arguments, command in programs(text):
                    # `Path(".").name` is empty, and `.` is how a script is sourced.
                    name = Path(program).name or program
                    if name in WRAPPER:
                        wrapped += 1
                        continue
                    if name in RUNNER_MAVEN:
                        problems.append(f"{path}: {location} runs the runner's Maven:\n    {command}")
                        continue
                    script_path = program
                    if name in SHELLS:
                        target = shell_target(arguments)
                        if target is None:
                            continue
                        if target[0] == "inline":
                            pending.append((f"{location} -> {name} -c", target[1], cwd))
                            continue
                        script_path = target[1]
                    called = repository_script(root, cwd, script_path)
                    if called is not None and called not in seen:
                        seen.add(called)
                        # A script's own relative calls resolve against the directory the step
                        # runs it from, which is the shell's working directory, not the script's.
                        pending.append((f"{location} -> {script_path}", called.read_text(errors="replace"), cwd))

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("Invoke Maven as ./mvnw. The runner image's mvn changes version under the workflow with no")
        print("review: the move from 3.9.16 to 3.10.0 broke every snapshot deploy (#10023) and then the")
        print("26.10.2 release bundle (#10111). The wrapper pins the version in")
        print(".mvn/wrapper/maven-wrapper.properties, so a Maven upgrade arrives as a reviewable bump.")
        return 1

    print(f"every Maven invocation goes through the Maven wrapper: {wrapped} ./mvnw invocation(s) across {len(files)} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
