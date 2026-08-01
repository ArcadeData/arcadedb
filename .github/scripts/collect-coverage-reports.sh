#!/usr/bin/env bash
#
# Collects the JaCoCo reports of every test suite, and refuses to hand over a partial set.
#
# Coverage is produced by four jobs that each upload their own JaCoCo XML, and merged by a fifth
# that downloads all four and uploads the union. The merge is only meaningful if all four are
# present: a suite that is missing does not read as "unknown", it reads as "nothing in that module
# was covered". In #5701 the ha-raft reports were absent and Codecov reported 1,110 covered lines
# disappearing - as a coverage regression, on a PR that had not touched the module.
#
# The absence used to be invisible. Each download carried `continue-on-error: true`, and the file
# list came from a bare `find`, so "the suite is missing" and "the suite is there" produced the
# same green step with a different number of files. This script makes that difference loud: it
# names the suites it expects, verifies each one actually contributed a report, and fails before
# anything is uploaded rather than publishing a merge that is quietly missing a module.
#
# Failing here is the point. A partial upload is worse than no upload: it does not just mislabel
# its own commit, it becomes the base every later PR is compared against, so one incomplete run
# produces a phantom coverage drop and then a phantom coverage recovery on whoever comes next.
#
# Usage:
#   collect-coverage-reports.sh <suite>=<dir> [<suite>=<dir> ...]
#
# Writes COVERAGE_FILES (a comma-separated list) to $GITHUB_OUTPUT when set, a per-suite table to
# $GITHUB_STEP_SUMMARY when set, and the list to stdout either way.
#
# @author Luca Garulli (l.garulli@arcadedata.com)

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "usage: $(basename "$0") <suite>=<dir> [<suite>=<dir> ...]" >&2
    exit 2
fi

files=()
missing=()
summary=()

# `find` runs into a file rather than straight into the read loop: a process substitution is not
# part of the pipeline, so `pipefail` never sees its exit status and an unreadable directory would
# look exactly like an empty one. Given this script exists to tell those two apart, it checks.
scratch="$(mktemp)"
trap 'rm -f "$scratch"' EXIT

for argument in "$@"; do
    if [[ $argument != *=* ]]; then
        echo "$(basename "$0"): expected <suite>=<dir>, got '$argument'" >&2
        exit 2
    fi
    suite="${argument%%=*}"
    directory="${argument#*=}"

    # A suite whose job never ran leaves no directory at all, one whose job produced no coverage
    # leaves an empty one. Both are the same failure - the merge would silently drop that suite.
    found=()
    if [[ -d $directory ]]; then
        if ! find "$directory" -type f -name 'jacoco*.xml' >"$scratch"; then
            echo "$(basename "$0"): cannot read '$directory' for suite '$suite'" >&2
            exit 1
        fi
        while IFS= read -r report; do
            found+=("$report")
        done < <(sort "$scratch")
    fi

    if [[ ${#found[@]} -eq 0 ]]; then
        missing+=("$suite")
        summary+=("| $suite | \`$directory\` | 0 | :x: missing |")
    else
        files+=("${found[@]}")
        summary+=("| $suite | \`$directory\` | ${#found[@]} | :white_check_mark: |")
    fi
done

if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
    {
        echo "### Coverage reports collected"
        echo
        echo "| Suite | Artifact | Reports | Status |"
        echo "|---|---|---:|---|"
        printf '%s\n' "${summary[@]}"
    } >>"$GITHUB_STEP_SUMMARY"
fi

printf '%s\n' "${summary[@]}"

if [[ ${#missing[@]} -gt 0 ]]; then
    echo >&2
    echo "No coverage report from: ${missing[*]}" >&2
    echo >&2
    echo "Not uploading a partial merge. Every module those suites cover would be reported as" >&2
    echo "uncovered, which reads as a coverage regression on a diff that never touched it - and" >&2
    echo "then becomes the base the next pull request is compared against." >&2
    echo >&2
    echo "Check whether the producing job was cancelled or failed before writing its JaCoCo XML." >&2
    exit 1
fi

list="$(
    IFS=,
    echo "${files[*]}"
)"

echo "Collected ${#files[@]} JaCoCo report(s)"

if [[ -n ${GITHUB_OUTPUT:-} ]]; then
    echo "COVERAGE_FILES=$list" >>"$GITHUB_OUTPUT"
fi

echo "$list"
