#!/usr/bin/env bash
#
# Self-test for the CI scripts in .github/scripts.
#
# These scripts guard the build, so a regression in one of them is silent by construction: it does
# not break anything visible, it just stops catching what it was written to catch. The cases below
# pin both directions - what must fail, and what must not - against fixtures rather than against
# the live workflows, so they keep testing the same thing as the workflows change.
#
# The #5701 fixtures are the regression test for this issue: a `coverage-report` job that
# downloads HA coverage without waiting for it, and a merge that is missing one suite's reports.
#
# Usage:
#   test-ci-scripts.sh
#
# @author Luca Garulli (l.garulli@arcadedata.com)

set -euo pipefail

cd "$(dirname "$0")/../../.."

SCRIPTS=".github/scripts"
DEPS="$SCRIPTS/check-workflow-artifact-deps.py"
COLLECT="$SCRIPTS/collect-coverage-reports.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0
checks=0

pass() {
    checks=$((checks + 1))
    echo "  ok   - $1"
}

fail() {
    checks=$((checks + 1))
    failures=$((failures + 1))
    echo "  FAIL - $1"
    if [[ -n ${2:-} ]]; then
        echo "$2" | sed 's/^/         /'
    fi
}

# Runs a command, then asserts its exit status and, optionally, that its output mentions a string.
expect() {
    local description="$1" expected_status="$2" needle="$3"
    shift 3
    local output status=0
    output="$("$@" 2>&1)" || status=$?

    if [[ $status -ne $expected_status ]]; then
        fail "$description (expected exit $expected_status, got $status)" "$output"
    elif [[ -n $needle && $output != *"$needle"* ]]; then
        fail "$description (output does not mention '$needle')" "$output"
    else
        pass "$description"
    fi
}

echo "check-workflow-artifact-deps.py"

# The #5701 shape: coverage-report consumes an artifact from a job it does not wait for.
mkdir -p "$work/unordered"
cat >"$work/unordered/ci.yml" <<'YAML'
name: unordered
on: [ push ]
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: unit-coverage-reports
  ha-integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: ha-integration-coverage-reports
  coverage-report:
    runs-on: ubuntu-latest
    needs: [ unit-tests ]
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: unit-coverage-reports
      - uses: actions/download-artifact@v8
        with:
          name: ha-integration-coverage-reports
YAML
expect "rejects a download whose producer is not in needs" 1 "ha-integration-tests" \
    "$DEPS" "$work/unordered"

# Same workflow with the missing dependency added: the only change that should matter.
mkdir -p "$work/ordered"
sed 's/needs: \[ unit-tests \]/needs: [ unit-tests, ha-integration-tests ]/' \
    "$work/unordered/ci.yml" >"$work/ordered/ci.yml"
expect "accepts the same workflow once needs lists the producer" 0 "" \
    "$DEPS" "$work/ordered"

# needs is transitive: waiting for a job that waits for the producer is enough.
mkdir -p "$work/transitive"
cat >"$work/transitive/ci.yml" <<'YAML'
name: transitive
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: jars
  test:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
  report:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: jars
YAML
expect "accepts a producer reached transitively through needs" 0 "" \
    "$DEPS" "$work/transitive"

# A matrix uploader names its artifact with an expression; a consumer picks the family up with a
# pattern. That pair must resolve, or every matrix workflow reports a false violation.
mkdir -p "$work/matrix"
cat >"$work/matrix/ci.yml" <<'YAML'
name: matrix
on: [ push ]
jobs:
  cell:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        version: [ 1, 2 ]
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: bolt-matrix-java-${{ matrix.version }}
  merge:
    runs-on: ubuntu-latest
    needs: cell
    steps:
      - uses: actions/download-artifact@v8
        with:
          pattern: bolt-matrix-*
          merge-multiple: true
YAML
expect "resolves a matrix-named upload against a pattern download" 0 "" \
    "$DEPS" "$work/matrix"

# The same pattern download without the dependency is still a race.
mkdir -p "$work/matrix-unordered"
sed '/needs: cell/d' "$work/matrix/ci.yml" >"$work/matrix-unordered/ci.yml"
expect "rejects a pattern download that does not wait for the matrix" 1 "cell" \
    "$DEPS" "$work/matrix-unordered"

# A matrix consumer downloading its own cell's artifact resolves to the matrix producer.
mkdir -p "$work/matrix-name"
cat >"$work/matrix-name/ci.yml" <<'YAML'
name: matrix-name
on: [ push ]
jobs:
  cell:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        version: [ 1, 2 ]
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: report-${{ matrix.version }}
  consume:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        version: [ 1, 2 ]
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: report-${{ matrix.version }}
YAML
expect "rejects a matrix-named download that does not wait for the matrix" 1 "cell" \
    "$DEPS" "$work/matrix-name"

# A literal artifact that falls under a matrix family's glob is not produced by that family:
# `build-*` covers `build-logs`, but `matrix.os` only ever yields `build-ubuntu` / `build-macos`.
# Reporting the matrix job here would be a false violation in a check that gates the whole build.
mkdir -p "$work/prefix-collision"
cat >"$work/prefix-collision/ci.yml" <<'YAML'
name: prefix-collision
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ ubuntu, macos ]
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: build-${{ matrix.os }}
  logs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: build-logs
  consume:
    runs-on: ubuntu-latest
    needs: logs
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: build-logs
YAML
expect "does not demand a matrix job whose glob merely covers another artifact" 0 "" \
    "$DEPS" "$work/prefix-collision"

# The same shape, with the download actually naming one of the matrix's values.
mkdir -p "$work/matrix-value"
sed 's/name: build-logs$/name: build-macos/' "$work/prefix-collision/ci.yml" \
    >"$work/matrix-value/ci.yml"
expect "still demands the matrix job when the name is one it produces" 1 "build" \
    "$DEPS" "$work/matrix-value"

# An `include`-only matrix is the shape most of this repository's matrices use.
mkdir -p "$work/matrix-include"
cat >"$work/matrix-include/ci.yml" <<'YAML'
name: matrix-include
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        include:
          - platform: linux
            runs-on: ubuntu-latest
          - platform: darwin
            runs-on: macos-15
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: wheel-${{ matrix.platform }}
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: wheel-darwin
YAML
expect "resolves a value contributed only by a matrix include" 1 "build" \
    "$DEPS" "$work/matrix-include"

# A matrix built at run time cannot be resolved, so the glob fallback applies rather than a crash.
mkdir -p "$work/matrix-dynamic"
cat >"$work/matrix-dynamic/ci.yml" <<'YAML'
name: matrix-dynamic
on: [ push ]
jobs:
  setup:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.gen.outputs.matrix }}
    steps:
      - id: gen
        run: echo matrix=[] >> "$GITHUB_OUTPUT"
  build:
    runs-on: ubuntu-latest
    needs: setup
    strategy:
      matrix: ${{ fromJSON(needs.setup.outputs.matrix) }}
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: image-${{ matrix.arch }}
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: image-amd64
YAML
expect "falls back to the glob when the matrix is built at run time" 1 "build" \
    "$DEPS" "$work/matrix-dynamic"

# `run-id` pointing at the current run is an ordinary consumer, not a cross-run read.
mkdir -p "$work/same-run-id"
cat >"$work/same-run-id/ci.yml" <<'YAML'
name: same-run-id
on: [ push ]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: report
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: report
          run-id: ${{ github.run_id }}
YAML
expect "still checks a download pinned to the current run-id" 1 "producer" \
    "$DEPS" "$work/same-run-id"

# A selector that collapses to a bare "*" names nothing in particular: no finding can be drawn.
mkdir -p "$work/opaque"
cat >"$work/opaque/ci.yml" <<'YAML'
name: opaque
on:
  workflow_dispatch:
    inputs:
      artifact:
        type: string
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: something
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: ${{ inputs.artifact }}
YAML
expect "does not guess at a fully interpolated selector" 0 "" \
    "$DEPS" "$work/opaque"

# A download with no selector takes every artifact in the run, so it depends on every producer.
mkdir -p "$work/download-all"
cat >"$work/download-all/ci.yml" <<'YAML'
name: download-all
on: [ push ]
jobs:
  a:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: one
  b:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: two
  all:
    runs-on: ubuntu-latest
    needs: a
    steps:
      - uses: actions/download-artifact@v8
YAML
expect "rejects an unselective download that misses a producer" 1 "'b'" \
    "$DEPS" "$work/download-all"

# A cross-run download reads another run's artifact store, so this run's ordering cannot apply.
mkdir -p "$work/cross-run"
cat >"$work/cross-run/ci.yml" <<'YAML'
name: cross-run
on: [ push ]
jobs:
  fetch:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: from-another-workflow
          run-id: 12345
          github-token: ${{ secrets.GITHUB_TOKEN }}
YAML
expect "ignores a download from another workflow run" 0 "" \
    "$DEPS" "$work/cross-run"

# A job delegating to a reusable workflow has no steps to read, so its uploads are invisible. The
# check must not turn that blind spot into a hard failure on a workflow that is actually correct.
mkdir -p "$work/reusable"
cat >"$work/reusable/ci.yml" <<'YAML'
name: reusable
on: [ push ]
jobs:
  called:
    uses: ./.github/workflows/build.yml
  consume:
    runs-on: ubuntu-latest
    needs: called
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: built-by-the-called-workflow
YAML
expect "does not report an artifact a reusable workflow may upload" 0 "" \
    "$DEPS" "$work/reusable"

# Suppressed is not the same as discarded: a typo in an artifact name looks identical to one the
# called workflow uploads, so the finding still has to be visible somewhere.
expect "says which findings it suppressed and why" 0 "not reported" \
    "$DEPS" "$work/reusable"

# The blind spot must not suppress a violation that is still visible in this file.
mkdir -p "$work/reusable-unordered"
cat >"$work/reusable-unordered/ci.yml" <<'YAML'
name: reusable-unordered
on: [ push ]
jobs:
  called:
    uses: ./.github/workflows/build.yml
  producer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        with:
          name: visible
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: visible
YAML
expect "still reports a visible violation alongside a reusable workflow" 1 "producer" \
    "$DEPS" "$work/reusable-unordered"

# The invariant on the workflows this repository actually ships.
expect "the repository's own workflows are ordered" 0 "" "$DEPS"

echo
echo "collect-coverage-reports.sh"

for suite in unit slow integration ha; do
    mkdir -p "$work/coverage/$suite/module/target/site/jacoco"
    echo '<report/>' >"$work/coverage/$suite/module/target/site/jacoco/jacoco.xml"
done

expect "collects a report from every suite" 0 "$work/coverage/ha/module/target/site/jacoco/jacoco.xml" \
    "$COLLECT" \
    "unit-tests=$work/coverage/unit" \
    "slow-unit-tests=$work/coverage/slow" \
    "integration-tests=$work/coverage/integration" \
    "ha-integration-tests=$work/coverage/ha"

# #5701 itself: the HA job has not uploaded yet, so its artifact never downloaded.
expect "refuses to publish when a suite's artifact is missing" 1 "ha-integration-tests" \
    "$COLLECT" \
    "unit-tests=$work/coverage/unit" \
    "slow-unit-tests=$work/coverage/slow" \
    "integration-tests=$work/coverage/integration" \
    "ha-integration-tests=$work/coverage/absent"

# A job that ran but produced no coverage leaves the directory behind and is just as wrong.
mkdir -p "$work/coverage/empty"
expect "refuses to publish when a suite produced no report" 1 "ha-integration-tests" \
    "$COLLECT" \
    "unit-tests=$work/coverage/unit" \
    "ha-integration-tests=$work/coverage/empty"

# The collected list is what the upload steps consume, so its shape is part of the contract.
list="$("$COLLECT" "unit-tests=$work/coverage/unit" "ha-integration-tests=$work/coverage/ha" | tail -1)"
if [[ $list == *",,"* || $list == *, || $list == ,* ]]; then
    fail "the file list has no empty entries" "$list"
elif [[ $(tr ',' '\n' <<<"$list" | wc -l) -ne 2 ]]; then
    fail "the file list has one entry per report" "$list"
else
    pass "the file list is comma-separated with no empty entries"
fi

expect "rejects an argument that is not <suite>=<dir>" 2 "expected <suite>=<dir>" \
    "$COLLECT" "not-a-pair"

# An unreadable directory is not an empty one, and telling those apart is the whole job. Skipped
# for root, which can read it regardless.
if [[ $(id -u) -ne 0 ]]; then
    mkdir -p "$work/coverage/unreadable"
    chmod 000 "$work/coverage/unreadable"
    expect "fails when a suite's directory cannot be read" 1 "cannot read" \
        "$COLLECT" "ha-integration-tests=$work/coverage/unreadable"
    chmod 755 "$work/coverage/unreadable"
fi

echo
if [[ $failures -gt 0 ]]; then
    echo "$failures of $checks checks failed"
    exit 1
fi
echo "$checks checks passed"
