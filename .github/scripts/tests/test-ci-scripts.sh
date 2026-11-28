#!/usr/bin/env bash
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
RESULTS="$SCRIPTS/check-test-results.py"
GUARDS="$SCRIPTS/check-test-reporter-guards.py"
ALLOWLIST="$SCRIPTS/check-license-allowlist.py"
DBDOCSSYNC="$SCRIPTS/check-database-docs-sync.py"
CONTROLCHARS="$SCRIPTS/check-source-control-chars.py"
NEEDSOUTPUTS="$SCRIPTS/check-workflow-needs-outputs.py"
STATUSCOMPLETE="$SCRIPTS/check-ci-status-completeness.py"
GRAALVMPIN="$SCRIPTS/check-native-graalvm-pin.py"
NATIVEMARCH="$SCRIPTS/check-native-march.py"
NATIVEMARCHDEFAULT="$SCRIPTS/check-native-march-builder-default.py"
RELEASEASSETS="$SCRIPTS/check-release-upload-assets.py"
CREDSCOPE="$SCRIPTS/check-deploy-credential-scope.py"
WRAPPER="$SCRIPTS/check-maven-wrapper.py"
PROMPTMUT="$SCRIPTS/check-claude-prompt-mutations.py"
PINCOMMENTS="$SCRIPTS/check-action-pin-comments.py"
CHAOSSUMMARY="$SCRIPTS/chaos-run-summary.sh"
SBOMFIELDS="$SCRIPTS/check-sbom-fields.py"

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
echo "check-test-results.py"

# Writes what surefire/failsafe write: one file per class, the counts on the root element.
suite_report() {
    local path="$1" name="$2" tests="$3" suite_failures="$4" suite_errors="$5"
    mkdir -p "$(dirname "$path")"
    cat >"$path" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="$name" time="0.5" tests="$tests" errors="$suite_errors" skipped="0" failures="$suite_failures">
  <testcase name="aTest" classname="$name" time="0.5"/>
</testsuite>
XML
}

green="$work/results/green"
suite_report "$green/engine/target/surefire-reports/TEST-com.arcadedb.GreenTest.xml" com.arcadedb.GreenTest 7 0 0
suite_report "$green/server/target/surefire-reports/TEST-com.arcadedb.server.GreenTest.xml" com.arcadedb.server.GreenTest 3 0 0
expect "accepts a run in which every suite passed" 0 "10 test(s)" \
    "$RESULTS" surefire-reports --root "$green"

# #5763's shape: Maven ran with --fail-never and exited 0, so this report is the only evidence
# there is. A green exit here would publish a failing suite as a passing job.
failing="$work/results/failing"
suite_report "$failing/engine/target/surefire-reports/TEST-com.arcadedb.GreenTest.xml" com.arcadedb.GreenTest 7 0 0
suite_report "$failing/ha-raft/target/surefire-reports/TEST-com.arcadedb.RedTest.xml" com.arcadedb.RedTest 4 1 0
expect "rejects a run with a failed test" 1 "TEST-com.arcadedb.RedTest.xml" \
    "$RESULTS" surefire-reports --root "$failing"

# An error is a distinct attribute from a failure and is just as fatal.
erroring="$work/results/erroring"
suite_report "$erroring/engine/target/surefire-reports/TEST-com.arcadedb.BrokenTest.xml" com.arcadedb.BrokenTest 4 0 2
expect "rejects a run with an errored test" 1 "2 error(s)" \
    "$RESULTS" surefire-reports --root "$erroring"

# Reports for the other test phase are not this job's verdict: the integration jobs run with
# -DskipTests, and a stale surefire directory must not redden them.
mixed="$work/results/mixed"
suite_report "$mixed/engine/target/surefire-reports/TEST-com.arcadedb.RedTest.xml" com.arcadedb.RedTest 4 1 0
suite_report "$mixed/engine/target/failsafe-reports/TEST-com.arcadedb.GreenIT.xml" com.arcadedb.GreenIT 4 0 0
expect "reads only the reports of the phase it was asked about" 0 "4 test(s)" \
    "$RESULTS" failsafe-reports --root "$mixed"

# No report at all means the suite never ran. Answering "no failures" to that is the false green
# this check exists to prevent.
mkdir -p "$work/results/absent/engine/target"
expect "rejects a run that produced no report" 1 "no surefire-reports" \
    "$RESULTS" surefire-reports --root "$work/results/absent"

# A fork killed mid-write leaves a report that cannot be parsed. It is not evidence of success.
truncated="$work/results/truncated/engine/target/surefire-reports"
mkdir -p "$truncated"
printf '<?xml version="1.0"?>\n<testsuite name="com.arcadedb.CutTest" tests="4"' >"$truncated/TEST-com.arcadedb.CutTest.xml"
expect "rejects a report it cannot parse" 1 "unreadable" \
    "$RESULTS" surefire-reports --root "$work/results/truncated"

# A suite that only skipped tests ran and reported; nothing failed.
skipped="$work/results/skipped"
suite_report "$skipped/engine/target/surefire-reports/TEST-com.arcadedb.SkippedTest.xml" com.arcadedb.SkippedTest 0 0 0
expect "accepts a suite whose tests were all skipped" 0 "0 failure(s)" \
    "$RESULTS" surefire-reports --root "$skipped"

# Writes a report with one test case per argument: "pass", "abort" (a failed assumption, which surefire
# records as a skipped case naming the exception) or "disable" (a test disabled in the source, whose
# skipped case carries a message only). The shapes are copied from real surefire 3 reports.
case_report() {
    local path="$1" name="$2"
    shift 2
    local tests=$# skipped_count=0 kind body=""
    for kind in "$@"; do
        case "$kind" in
            pass) body+="  <testcase name=\"passes$tests\" classname=\"$name\" time=\"0.1\"/>"$'\n' ;;
            abort)
                skipped_count=$((skipped_count + 1))
                body+="  <testcase name=\"aborts$skipped_count\" classname=\"$name\" time=\"0.1\"><skipped type=\"org.opentest4j.TestAbortedException\"><![CDATA[org.opentest4j.TestAbortedException: Assumption failed: the heap is too busy here"$'\n'"	at org.junit.jupiter.api.Assumptions.throwAssumptionFailed(Assumptions.java:328)]]></skipped></testcase>"$'\n' ;;
            disable)
                skipped_count=$((skipped_count + 1))
                body+="  <testcase name=\"disabled$skipped_count\" classname=\"$name\" time=\"0\"><skipped message=\"void $name.disabled$skipped_count() is @Disabled\"/></testcase>"$'\n' ;;
        esac
    done
    mkdir -p "$(dirname "$path")"
    printf '<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="%s" time="0.5" tests="%d" errors="0" skipped="%d" failures="0">\n%s</testsuite>\n' \
        "$name" "$tests" "$skipped_count" "$body" >"$path"
}

# #11209: an assumption that aborts SOME tests of a class is reported, with its reason, and is not a
# failure - the rest of the class ran, and assumptions are often deliberate.
partly="$work/results/partly-aborted"
case_report "$partly/engine/target/surefire-reports/TEST-com.arcadedb.PartlyTest.xml" com.arcadedb.PartlyTest pass abort pass
expect "accepts a suite in which only some tests aborted" 0 "1 skipped (1 aborted by an assumption)" \
    "$RESULTS" surefire-reports --root "$partly"
expect "names the aborted test and why it aborted" 0 "aborts1: Assumption failed: the heap is too busy here" \
    "$RESULTS" surefire-reports --root "$partly"

# The false green #11209 is about: every test of the class aborted, so it counted towards the tests and
# towards nothing else, and the lane used to report clean over a class that asserted nothing.
whole="$work/results/wholly-aborted"
suite_report "$whole/engine/target/surefire-reports/TEST-com.arcadedb.GreenTest.xml" com.arcadedb.GreenTest 7 0 0
case_report "$whole/engine/target/surefire-reports/TEST-com.arcadedb.Issue1Test.xml" com.arcadedb.Issue1Test abort abort
expect "rejects a suite whose every test aborted" 1 "TEST-com.arcadedb.Issue1Test.xml: every one of its 2 test(s) aborted" \
    "$RESULTS" surefire-reports --root "$whole"

# A disabled test is a decision taken in the source, not evidence that went missing on this runner.
disabled="$work/results/disabled"
case_report "$disabled/engine/target/surefire-reports/TEST-com.arcadedb.DisabledTest.xml" com.arcadedb.DisabledTest disable disable
expect "accepts a suite whose every test is disabled" 0 "2 skipped (0 aborted by an assumption)" \
    "$RESULTS" surefire-reports --root "$disabled"

# #11275: the rule is that SOME case of the class asserted, not that some case did not abort. A class
# whose cases are part aborted and part disabled ran none of them, so it is the false green above
# reached through a disabled remainder rather than an aborted one.
mixed="$work/results/aborted-and-disabled"
case_report "$mixed/engine/target/surefire-reports/TEST-com.arcadedb.Issue1MixedTest.xml" com.arcadedb.Issue1MixedTest abort disable
expect "rejects a suite whose tests all aborted or were disabled" 1 \
    "TEST-com.arcadedb.Issue1MixedTest.xml: none of its 2 test(s) ran (1 aborted, 1 disabled)" \
    "$RESULTS" surefire-reports --root "$mixed"
expect "lists the disabled remainder beside the aborted cases" 1 "1 of 2 test(s) aborted and 1 disabled" \
    "$RESULTS" surefire-reports --root "$mixed"

# The fourth shape: a class that aborted some cases, disabled others and ran the rest asserted something.
ranSome="$work/results/aborted-disabled-and-ran"
case_report "$ranSome/engine/target/surefire-reports/TEST-com.arcadedb.RanSomeTest.xml" com.arcadedb.RanSomeTest abort disable pass
expect "accepts a suite in which one test ran beside aborted and disabled ones" 0 "2 skipped (1 aborted by an assumption)" \
    "$RESULTS" surefire-reports --root "$ranSome"

# JUnit 4's assumption, which the TinkerPop suites gate on graph features with, is an abort too.
junit4="$work/results/junit4-aborted"
case_report "$junit4/gremlin-it/target/failsafe-reports/TEST-com.arcadedb.FeatureIT.xml" com.arcadedb.FeatureIT abort
sed -i.bak 's/org.opentest4j.TestAbortedException/org.junit.AssumptionViolatedException/g' \
    "$junit4/gremlin-it/target/failsafe-reports/TEST-com.arcadedb.FeatureIT.xml"
expect "rejects a suite whose every test failed a JUnit 4 assumption" 1 "every one of its 1 test(s) aborted" \
    "$RESULTS" failsafe-reports --root "$junit4"

echo
echo "check-test-reporter-guards.py"

# The #5763 fix: the reporter is decorative, so it must not redden a job whose tests passed. It is
# only decorative while the job can still fail on its own, and these fixtures pin both directions.

# The e2e shape. `mvnw verify` exits non-zero on a failing test, so the job's verdict is the test
# step itself and exempting the reporter costs nothing.
mkdir -p "$work/reporter-e2e"
cat >"$work/reporter-e2e/ci.yml" <<'YAML'
name: e2e
on: [ push ]
jobs:
  java-e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - name: E2E Tests
        run: ./mvnw verify -Pintegration -pl e2e
      - name: E2E Tests Reporter
        uses: dorny/test-reporter@v3
        if: success() || failure()
        continue-on-error: true
YAML
expect "accepts an exempt reporter in a job whose test step is the verdict" 0 "" \
    "$GUARDS" "$work/reporter-e2e"

# The unit-tests shape: --fail-never makes the test step exit 0 regardless, and check-test-results.py
# is what turns a failing test back into a failing job. Exempting the reporter is safe here too.
mkdir -p "$work/reporter-verdict"
cat >"$work/reporter-verdict/ci.yml" <<'YAML'
name: verdict
on: [ push ]
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - name: Run Tests with Coverage
        run: ./mvnw verify --fail-never -Pcoverage
      - name: Unit Tests Reporter
        uses: dorny/test-reporter@v3
        continue-on-error: true
      - name: Check test results
        run: ./.github/scripts/check-test-results.py surefire-reports
YAML
expect "accepts an exempt reporter in a --fail-never job that checks its results" 0 "" \
    "$GUARDS" "$work/reporter-verdict"

# The false green. Same --fail-never job with the verdict step removed: the test command cannot
# fail, and now neither can the reporter, so every failing test publishes as a green job.
mkdir -p "$work/reporter-false-green"
grep -v "check-test-results.py" "$work/reporter-verdict/ci.yml" |
    grep -v "name: Check test results" >"$work/reporter-false-green/ci.yml"
expect "rejects an exempt reporter in a --fail-never job with no verdict step" 1 "--fail-never" \
    "$GUARDS" "$work/reporter-false-green"

# The nightly workflows are that same shape, and are correct precisely because their reporters are
# not exempt: there, the reporter failing is the only thing that reports a failing test.
mkdir -p "$work/reporter-nightly"
grep -v "continue-on-error: true" "$work/reporter-false-green/ci.yml" >"$work/reporter-nightly/ci.yml"
expect "accepts a --fail-never job whose reporter is not exempt" 0 "" \
    "$GUARDS" "$work/reporter-nightly"

# `continue-on-error` takes an expression, and an expression that evaluates true on some runs is
# the same hazard on those runs. It cannot be resolved statically, so it is read as exempt.
mkdir -p "$work/reporter-expression"
sed 's/continue-on-error: true/continue-on-error: ${{ github.event_name == '"'"'push'"'"' }}/' \
    "$work/reporter-false-green/ci.yml" >"$work/reporter-expression/ci.yml"
expect "rejects a reporter exempted by an unresolvable expression" 1 "supplies a verdict" \
    "$GUARDS" "$work/reporter-expression"

# A `|| true` on a step that is not the tests must not be read as a swallowed test failure: it
# would report every job that guards an optional cleanup, in a check that gates the whole build.
mkdir -p "$work/reporter-unrelated-guard"
cat >"$work/reporter-unrelated-guard/ci.yml" <<'YAML'
name: guarded
on: [ push ]
jobs:
  js-e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - name: E2E Node.js Tests
        run: |
          npm ci
          npm test
      - name: JS E2E Tests Reporter
        uses: dorny/test-reporter@v3
        continue-on-error: true
YAML
expect "accepts a job whose test step exits on failure" 0 "" \
    "$GUARDS" "$work/reporter-unrelated-guard"

# The live workflows are the point of the check, so they are asserted directly as well: mvn-test.yml
# must stay clean with the e2e reporters exempt, and the nightly workflows with theirs not.
expect "accepts the workflows as they stand" 0 "no unguarded reporter exemptions" \
    "$GUARDS"

echo "check-license-allowlist.py"

# The #5651 shape: the npm side of dependency-review-config.yml has an allow-list
# (license-checker --onlyAllow), the Maven side only ever had a deny-list. A license
# that is neither denied nor explicitly allowed must fail loudly instead of passing
# both gates by omission. "Mozilla Public License 2.0" (no comma, no "Version") is
# deliberately not the exact clause ALLOWED_CLAUSES recognizes for MPL-2.0, so this
# also pins that matching is exact-clause, not fuzzy.
mkdir -p "$work/license-mpl"
cat >"$work/license-mpl/THIRD-PARTY.txt" <<'EOF'
List of third-party dependencies grouped by their license type.

    (Apache License 2.0) Apache Commons Lang (org.apache.commons:commons-lang3:3.14.0 - https://commons.apache.org/proper/commons-lang/)
    (Mozilla Public License 2.0) Some MPL Lib (org.example:mpl-lib:1.0.0 - https://example.org)
EOF
expect "rejects a license outside the allow-list" 1 "Mozilla Public License 2.0" \
    "$ALLOWLIST" "$work/license-mpl/THIRD-PARTY.txt"

# A bare GPL (no classpath exception) must still be rejected, even though this script
# allows a specific classpath-exception GPL clause elsewhere - the exception text has
# to actually be present, "GPL" as a substring is not enough either way.
mkdir -p "$work/license-bare-gpl"
cat >"$work/license-bare-gpl/THIRD-PARTY.txt" <<'EOF'
List of third-party dependencies grouped by their license type.

    (Apache License 2.0) Apache Commons Lang (org.apache.commons:commons-lang3:3.14.0 - https://commons.apache.org/proper/commons-lang/)
    (GNU General Public License v3.0) Some GPL Lib (org.example:gpl-lib:1.0.0 - https://example.org)
EOF
expect "rejects a bare GPL with no classpath exception" 1 "GNU General Public License v3.0" \
    "$ALLOWLIST" "$work/license-bare-gpl/THIRD-PARTY.txt"

# Every family named in CLAUDE.md's ALLOWED list must pass, plus the license clauses
# this project already ships that motivated adding ISC / MPL-2.0 / CDDL / GPL-2.0-with-
# classpath-exception to that list alongside this script (see CLAUDE.md and the
# ALLOWED_CLAUSES docstring for the reasoning and the real dependency each one covers).
# The JMH and "New/Revised" BSD lines also pin that a clause with its own nested
# parentheses is parsed as one clause, not split apart.
mkdir -p "$work/license-clean"
cat >"$work/license-clean/THIRD-PARTY.txt" <<'EOF'
List of third-party dependencies grouped by their license type.

    (Apache License 2.0) Apache Commons Lang (org.apache.commons:commons-lang3:3.14.0 - https://commons.apache.org/proper/commons-lang/)
    (MIT License) SLF4J API Module (org.slf4j:slf4j-api:2.0.16 - http://www.slf4j.org)
    (BSD 2-Clause) Some BSD2 Lib (org.example:bsd2-lib:1.0.0 - https://example.org)
    (BSD 3-Clause) ANTLR 4 Runtime (org.antlr:antlr4-runtime:4.13.2 - https://www.antlr.org)
    (BSD 3-Clause "New" or "Revised" License (BSD-3-Clause)) abego TreeLayout Core (org.abego.treelayout:org.abego.treelayout.core:1.0.3 - http://treelayout.sourceforge.net)
    (EPL 1.0) logback-core (ch.qos.logback:logback-core:1.5.27 - https://logback.qos.ch/)
    (EPL 2.0) JUnit Jupiter API (org.junit.jupiter:junit-jupiter-api:6.0.2 - https://junit.org/junit5/)
    (UPL 1.0) Some UPL Lib (org.example:upl-lib:1.0.0 - https://example.org)
    (EDL 1.0) Jakarta Activation API (jakarta.activation:jakarta.activation-api:2.1.3 - https://github.com/eclipse-ee4j/jaf-api)
    (LGPL 2.1) logback-classic (ch.qos.logback:logback-classic:1.5.27 - https://logback.qos.ch/)
    (CC0 1.0 Universal) Some Public Domain Lib (org.example:cc0-lib:1.0.0 - https://example.org)
    (ISC) jBCrypt (org.mindrot:jbcrypt:0.4 - https://github.com/djmdjm/jBCrypt)
    (Mozilla Public License, Version 2.0) rhino (org.mozilla:rhino:1.7.15.1 - https://mozilla.github.io/rhino/)
    (CDDL + GPLv2 with classpath exception) javax.annotation API (javax.annotation:javax.annotation-api:1.3.2 - http://jcp.org/en/jsr/detail?id=250)
    (GNU General Public License (GPL), version 2, with the Classpath exception) JMH Core (org.openjdk.jmh:jmh-core:1.37 - http://openjdk.java.net/projects/code-tools/jmh/jmh-core/)
    (EPL 2.0) (GPL2 w/ CPE) Jakarta Annotations API (jakarta.annotation:jakarta.annotation-api:1.3.5 - https://projects.eclipse.org/projects/ee4j.ca)
EOF
expect "accepts every license family in CLAUDE.md's allow-list" 0 "" \
    "$ALLOWLIST" "$work/license-clean/THIRD-PARTY.txt"

# A dual-license dependency passes if ANY declared option is allowed, matching how a
# recipient may actually choose a license - even when the other option alone would not
# be (a bare GPL2, here, only made safe by the classpath exception in the other clause).
mkdir -p "$work/license-dual"
cat >"$work/license-dual/THIRD-PARTY.txt" <<'EOF'
List of third-party dependencies grouped by their license type.

    (Apache License 2.0) (GNU Lesser General Public License) javaparser-core (com.github.javaparser:javaparser-core:3.26.3 - https://github.com/javaparser/javaparser-core)
EOF
expect "accepts a dual-licensed dependency if one option is allowed" 0 "" \
    "$ALLOWLIST" "$work/license-dual/THIRD-PARTY.txt"

# A report with no "(<license>)" heading lines means the wrong goal ran (e.g.
# add-third-party against the near-empty root aggregator pom instead of
# aggregate-add-third-party against the whole reactor) - fail loudly rather than
# silently reporting zero violations.
mkdir -p "$work/license-empty"
printf 'List of third-party dependencies grouped by their license type.\n' \
    >"$work/license-empty/THIRD-PARTY.txt"
expect "rejects a report with no dependency lines" 2 "nothing to check" \
    "$ALLOWLIST" "$work/license-empty/THIRD-PARTY.txt"

expect "rejects a missing report file" 2 "not found" \
    "$ALLOWLIST" "$work/license-mpl/does-not-exist.txt"

# The live report is the point of the check: it must stay clean against the actual
# reactor, generated locally with:
#   mvn org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party -DskipTests
# A committed fixture (tests/fixtures/THIRD-PARTY-reactor-2026-08-11.txt), snapshotted
# from a real run against the full engine dependency closure (422 dependencies), stands
# in for that live report here so this self-test does not need network access or a
# multi-gigabyte local Maven repository to run.
expect "accepts a real reactor-wide report (422 dependencies, snapshotted 2026-08-11)" 0 "" \
    "$ALLOWLIST" "$SCRIPTS/tests/fixtures/THIRD-PARTY-reactor-2026-08-11.txt"

echo
echo "filter-meterian-sarif.py"

FIXTURE_SARIF="$SCRIPTS/tests/fixtures/meterian-main-2026-08-13.sarif"

# Counts a SARIF's rules and results, as "<rules> <results>", summed across every run.
sarif_counts() {
    python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
rules = sum(len(r.get("tool", {}).get("driver", {}).get("rules", [])) for r in doc.get("runs", []))
results = sum(len(r.get("results", [])) for r in doc.get("runs", []))
print(rules, results)
' "$1"
}

# The snapshot is the point of the check: 65 real findings from the 2026-08-13 scan of main,
# 46 of them "[stability] ... is outdated" and 19 tagged security. Filtering must leave exactly
# the 19 security findings and the 18 rules they reference (safe-buffer carries two results).
cp "$FIXTURE_SARIF" "$work/real.sarif"
expect "filters the real 2026-08-13 scan of main" 0 "46 stability" \
    "$SCRIPTS/filter-meterian-sarif.py" "$work/real.sarif"

counts="$(sarif_counts "$work/real.sarif")"
if [[ $counts == "18 19" ]]; then
    pass "leaves 18 rules and 19 results"
else
    fail "leaves 18 rules and 19 results" "got: $counts"
fi

# Every survivor must be a security finding - the filter must never strip one.
survivors="$(python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
bad = [r["id"] for run in doc["runs"] for r in run["tool"]["driver"]["rules"]
       if "security" not in r.get("properties", {}).get("tags", [])]
print(",".join(bad) if bad else "clean")
' "$work/real.sarif")"
if [[ $survivors == "clean" ]]; then
    pass "every surviving rule is tagged security"
else
    fail "every surviving rule is tagged security" "non-security survivors: $survivors"
fi

# The trap this script exists to avoid: results reference their rule by array position as well as
# by id, so dropping rules without remapping result.rule.index silently reattributes findings to
# whatever rule slid into that slot. Pin the linkage by id, not by trusting the index.
mismatch="$(python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
bad = []
for run in doc["runs"]:
    rules = run["tool"]["driver"]["rules"]
    for res in run["results"]:
        idx = res.get("rule", {}).get("index")
        if idx is None:
            continue
        if not (0 <= idx < len(rules)) or rules[idx]["id"] != res["ruleId"]:
            bad.append(res["ruleId"])
print(",".join(bad) if bad else "consistent")
' "$work/real.sarif")"
if [[ $mismatch == "consistent" ]]; then
    pass "remaps result.rule.index to the surviving rule array"
else
    fail "remaps result.rule.index to the surviving rule array" "misattributed: $mismatch"
fi

# Filtering an already-filtered report must be a no-op, so a re-run cannot compound.
cp "$work/real.sarif" "$work/idempotent.sarif"
"$SCRIPTS/filter-meterian-sarif.py" "$work/idempotent.sarif" >/dev/null
if cmp -s "$work/real.sarif" "$work/idempotent.sarif"; then
    pass "is idempotent"
else
    fail "is idempotent" "second pass changed the report"
fi

# Meterian's own upload has no result.rule.index at all (GitHub adds it when it normalises the
# report). The filter must handle both shapes, so cover the bare one explicitly.
cat >"$work/no-index.sarif" <<'JSON'
{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Meterian","rules":[
  {"id":"S1","properties":{"tags":["stability"]}},
  {"id":"V1","properties":{"tags":["CWE-1104","security","vulnerability"]}}
]}},"results":[
  {"ruleId":"S1","message":{"text":"outdated"}},
  {"ruleId":"V1","message":{"text":"vulnerable"}}
]}]}
JSON
expect "handles a report with no result.rule.index" 0 "1 stability" \
    "$SCRIPTS/filter-meterian-sarif.py" "$work/no-index.sarif"

counts="$(sarif_counts "$work/no-index.sarif")"
if [[ $counts == "1 1" ]]; then
    pass "keeps the security finding when there is no rule index"
else
    fail "keeps the security finding when there is no rule index" "got: $counts"
fi

# Defensive: a rule carrying both tags is a security finding first. Dropping it because it also
# says "stability" would lose a real vulnerability, so the filter keeps it.
cat >"$work/both-tags.sarif" <<'JSON'
{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Meterian","rules":[
  {"id":"B1","properties":{"tags":["stability","security"]}}
]}},"results":[{"ruleId":"B1","message":{"text":"both"}}]}]}
JSON
"$SCRIPTS/filter-meterian-sarif.py" "$work/both-tags.sarif" >/dev/null
counts="$(sarif_counts "$work/both-tags.sarif")"
if [[ $counts == "1 1" ]]; then
    pass "keeps a rule tagged both stability and security"
else
    fail "keeps a rule tagged both stability and security" "got: $counts"
fi

# A multi-run report must be filtered in every run, not just the first.
cat >"$work/multi-run.sarif" <<'JSON'
{"version":"2.1.0","runs":[
 {"tool":{"driver":{"name":"Meterian","rules":[{"id":"S1","properties":{"tags":["stability"]}}]}},
  "results":[{"ruleId":"S1","message":{"text":"outdated"}}]},
 {"tool":{"driver":{"name":"Meterian","rules":[
   {"id":"S2","properties":{"tags":["stability"]}},
   {"id":"V2","properties":{"tags":["security"]}}]}},
  "results":[{"ruleId":"S2","message":{"text":"outdated"}},{"ruleId":"V2","message":{"text":"vuln"}}]}
]}
JSON
"$SCRIPTS/filter-meterian-sarif.py" "$work/multi-run.sarif" >/dev/null
counts="$(sarif_counts "$work/multi-run.sarif")"
if [[ $counts == "1 1" ]]; then
    pass "filters every run, not just the first"
else
    fail "filters every run, not just the first" "got: $counts"
fi

# An explicit "properties": null is valid JSON and reads back as null rather than as a missing
# key, so the tag lookup has to cope with it. Getting this wrong throws, and because the workflow
# step is continue-on-error that would quietly ship the whole unfiltered report.
cat >"$work/null-properties.sarif" <<'JSON'
{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Meterian","rules":[
  {"id":"N1","properties":null},
  {"id":"S1","properties":{"tags":["stability"]}},
  {"id":"V1","properties":{"tags":["security"]}}
]}},"results":[
  {"ruleId":"N1","message":{"text":"no metadata"}},
  {"ruleId":"S1","message":{"text":"outdated"}},
  {"ruleId":"V1","message":{"text":"vuln"}}
]}]}
JSON
expect "survives a rule with an explicit null properties" 0 "1 stability" \
    "$SCRIPTS/filter-meterian-sarif.py" "$work/null-properties.sarif"

counts="$(sarif_counts "$work/null-properties.sarif")"
if [[ $counts == "2 2" ]]; then
    pass "keeps a rule whose tags cannot be read"
else
    fail "keeps a rule whose tags cannot be read" "got: $counts"
fi

# SARIF spells the rule position two ways: nested under result.rule, and as a top-level
# result.ruleIndex. A report using the second form must be remapped too - a stale index there is
# the same misattribution the nested case is guarded against.
cat >"$work/top-level-index.sarif" <<'JSON'
{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Meterian","rules":[
  {"id":"S1","properties":{"tags":["stability"]}},
  {"id":"S2","properties":{"tags":["stability"]}},
  {"id":"V1","properties":{"tags":["security"]}}
]}},"results":[
  {"ruleId":"S1","ruleIndex":0,"message":{"text":"outdated"}},
  {"ruleId":"S2","ruleIndex":1,"message":{"text":"outdated"}},
  {"ruleId":"V1","ruleIndex":2,"message":{"text":"vuln"}}
]}]}
JSON
"$SCRIPTS/filter-meterian-sarif.py" "$work/top-level-index.sarif" >/dev/null
# The index is bounds-checked before it is dereferenced: a stale one usually points past the end
# of the shortened array, and an IndexError here would abort the whole harness instead of failing
# this one check.
remapped="$(python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
run = doc["runs"][0]
rules, results = run["tool"]["driver"]["rules"], run["results"]
bad = []
for r in results:
    if "ruleIndex" not in r:
        continue
    idx = r["ruleIndex"]
    if not (0 <= idx < len(rules)) or rules[idx]["id"] != r["ruleId"]:
        bad.append(r["ruleId"])
print(",".join(bad) if bad else "consistent")
' "$work/top-level-index.sarif")"
if [[ $remapped == "consistent" ]]; then
    pass "remaps a top-level result.ruleIndex"
else
    fail "remaps a top-level result.ruleIndex" "misattributed: $remapped"
fi

# A scan that produced no report must not turn the upload step red: the workflow already treats
# the upload as best-effort, and the filter has to keep that contract.
expect "treats a missing report as nothing to do" 0 "nothing to filter" \
    "$SCRIPTS/filter-meterian-sarif.py" "$work/does-not-exist.sarif"

# Malformed input is a real error - failing loudly beats uploading a mangled report.
printf 'not json at all\n' >"$work/broken.sarif"
expect "rejects a malformed report" 2 "not valid JSON" \
    "$SCRIPTS/filter-meterian-sarif.py" "$work/broken.sarif"

# Well-formed JSON that is not an object gets the same treatment, rather than reaching a .get()
# on a list and reporting a stack trace where the line above reports a reason.
printf '[]\n' >"$work/array.sarif"
expect "rejects well-formed JSON that is not an object" 2 "not valid JSON" \
    "$SCRIPTS/filter-meterian-sarif.py" "$work/array.sarif"

echo "check-database-docs-sync.py"

# The #6406 shape: the grammar's checkDatabaseStatement rule and the docs' [[sql-check-database]]
# syntax block, taken from a real snapshot of both at the time this test was written, agree.
mkdir -p "$work/dbdocs-clean"
cat >"$work/dbdocs-clean/SQLParser.g4" <<'EOF'
checkDatabaseStatement
    : CHECK DATABASE
      (TYPE identifier (COMMA identifier)*)?
      (BUCKET (identifier | INTEGER_LITERAL) (COMMA (identifier | INTEGER_LITERAL))*)?
      (RECORD rid (COMMA rid)*)?
      (FIX)?
      (DELETE ORPHANS)?
      (RECLAIM UNREFERENCED FILES)?
      (DEEP)?
      (COMPRESS)?
    ;
EOF
cat >"$work/dbdocs-clean/sql-database-admin.adoc" <<'EOF'
[[sql-check-database]]
===== SQL - `CHECK DATABASE`

[source,sql]
----
CHECK DATABASE [ TYPE <type-name>[,]* ] [ BUCKET <bucket-name>[,]* ] [ RECORD <rid>[,]* ]
              [ FIX ] [ DELETE ORPHANS ] [ RECLAIM UNREFERENCED FILES ] [ DEEP ] [ COMPRESS ]
----
EOF
expect "accepts a grammar and docs block that agree" 0 "agree on 8 clause" \
    "$DBDOCSSYNC" "$work/dbdocs-clean/SQLParser.g4" "$work/dbdocs-clean/sql-database-admin.adoc"

# The #5680/#6090/#6189 shape itself: a clause lands in the grammar and the docs are never
# touched. Modeled here as VALIDATE SIGNATURES, a clause neither side has ever had, so this pins
# detection rather than a coincidence with real history.
mkdir -p "$work/dbdocs-grammar-ahead"
cp "$work/dbdocs-clean/sql-database-admin.adoc" "$work/dbdocs-grammar-ahead/"
sed 's/(COMPRESS)?/(COMPRESS)?\n      (VALIDATE SIGNATURES)?/' \
    "$work/dbdocs-clean/SQLParser.g4" >"$work/dbdocs-grammar-ahead/SQLParser.g4"
expect "rejects a grammar clause the docs never mention" 1 "VALIDATE SIGNATURES" \
    "$DBDOCSSYNC" "$work/dbdocs-grammar-ahead/SQLParser.g4" "$work/dbdocs-grammar-ahead/sql-database-admin.adoc"

# The other direction: the docs still advertise a clause a later grammar change removed. Just as
# stale, and just as easy to miss without something that reads both sides.
mkdir -p "$work/dbdocs-docs-ahead"
cp "$work/dbdocs-clean/sql-database-admin.adoc" "$work/dbdocs-docs-ahead/"
sed '/(RECLAIM UNREFERENCED FILES)?/d' \
    "$work/dbdocs-clean/SQLParser.g4" >"$work/dbdocs-docs-ahead/SQLParser.g4"
expect "rejects a docs clause the grammar no longer has" 1 "RECLAIM UNREFERENCED FILES" \
    "$DBDOCSSYNC" "$work/dbdocs-docs-ahead/SQLParser.g4" "$work/dbdocs-docs-ahead/sql-database-admin.adoc"

# A multi-keyword clause (DELETE ORPHANS) has to match as ONE phrase, not as two single-keyword
# clauses that happen to sit next to each other - otherwise a docs block that separately mentions
# a bare `[ DELETE ]` and a bare `[ ORPHANS ]` would be accepted as if it spelled out the clause
# the grammar actually has.
mkdir -p "$work/dbdocs-split-phrase"
cp "$work/dbdocs-clean/SQLParser.g4" "$work/dbdocs-split-phrase/"
sed 's/\[ DELETE ORPHANS \]/[ DELETE ] [ ORPHANS ]/' \
    "$work/dbdocs-clean/sql-database-admin.adoc" >"$work/dbdocs-split-phrase/sql-database-admin.adoc"
expect "treats a multi-keyword clause as one phrase, not separate keywords" 1 "DELETE ORPHANS" \
    "$DBDOCSSYNC" "$work/dbdocs-split-phrase/SQLParser.g4" "$work/dbdocs-split-phrase/sql-database-admin.adoc"

# A missing anchor or a missing rule is a script/input problem, not a clause mismatch - it must
# fail loudly with which one is missing rather than reporting an empty, vacuously-agreeing diff.
mkdir -p "$work/dbdocs-no-anchor"
cp "$work/dbdocs-clean/SQLParser.g4" "$work/dbdocs-no-anchor/"
printf 'No CHECK DATABASE section here.\n' >"$work/dbdocs-no-anchor/sql-database-admin.adoc"
expect "fails loudly when the docs anchor is missing" 2 "anchor" \
    "$DBDOCSSYNC" "$work/dbdocs-no-anchor/SQLParser.g4" "$work/dbdocs-no-anchor/sql-database-admin.adoc"

# A clause whose placeholder repetition sits directly against the clause's OWN last keyword, no
# space in between (RECLAIM UNREFERENCED FILES[,]*) - code review on #6406 flagged that a
# non-greedy `\[([^\]]+?)\]` extracting the docs' bracket groups stops at the FIRST `]` it meets,
# which here is the placeholder's own inner one, truncating the captured text to "...FILES[," and
# silently dropping FILES from the recovered phrase. That used to be a false-positive drift alarm
# (grammar and docs agreeing in reality, reported as disagreeing) rather than a crash, which is
# exactly the failure mode a docs-sync guard must not have: it would tell a PR author to "fix" a
# doc line that was already correct. Depth-tracked bracket matching plus splitting `[`/`]` off as
# their own tokens in leading_keywords (matching how `(`/`)` were already split) fixes it.
mkdir -p "$work/dbdocs-adjacent-bracket"
cat >"$work/dbdocs-adjacent-bracket/SQLParser.g4" <<'EOF'
checkDatabaseStatement
    : CHECK DATABASE
      (TYPE identifier (COMMA identifier)*)?
      (RECLAIM UNREFERENCED FILES)?
    ;
EOF
cat >"$work/dbdocs-adjacent-bracket/sql-database-admin.adoc" <<'EOF'
[[sql-check-database]]
===== SQL - `CHECK DATABASE`

[source,sql]
----
CHECK DATABASE [ TYPE <type-name>[,]* ] [ RECLAIM UNREFERENCED FILES[,]* ]
----
EOF
expect "recovers a full keyword phrase when a placeholder touches the last keyword" 0 "agree on 2 clause" \
    "$DBDOCSSYNC" "$work/dbdocs-adjacent-bracket/SQLParser.g4" "$work/dbdocs-adjacent-bracket/sql-database-admin.adoc"

# The raw NUL that reached ArcadeDBServer.java (issue #6426) compiled and behaved correctly, so the
# only thing that noticed was grep - which reported the whole 1351-line file as binary and returned
# nothing for every search over it. The guard has to catch the byte wherever it sits, and must not
# trip on the tab/newline/carriage-return that every source file is made of.
mkdir -p "$work/controlchars-clean"
printf 'class A {\n\tvoid a() {\n\t\tb("x\\0y");\r\n\t}\n}\n' >"$work/controlchars-clean/A.java"
expect "accepts tabs, CRLF and an escaped NUL" 0 "" \
    "$CONTROLCHARS" "$work/controlchars-clean"

mkdir -p "$work/controlchars-nul"
printf 'class A {\n  int i = "x".indexOf('"'"'\000'"'"');\n}\n' >"$work/controlchars-nul/A.java"
expect "rejects a raw NUL byte and points at its line" 1 "A.java:2:" \
    "$CONTROLCHARS" "$work/controlchars-nul"

# ESC is the one that reads as harmless - it is how a terminal colour code is written - and it makes
# the file just as invisible to grep as the NUL does.
mkdir -p "$work/controlchars-esc"
printf 'RED = "\033[31m"\n' >"$work/controlchars-esc/colors.py"
expect "rejects a raw ESC byte" 1 "ESC" \
    "$CONTROLCHARS" "$work/controlchars-esc"

# A control byte in build output or in a vendored bundle is not ours to fix, and a binary suffix is
# not a source file at all - neither may fail the build.
mkdir -p "$work/controlchars-skipped/target" "$work/controlchars-skipped/node_modules"
printf 'class B {\000}\n' >"$work/controlchars-skipped/target/B.java"
printf 'var x = "\000";\n' >"$work/controlchars-skipped/node_modules/bundle.js"
printf '\000\001\002' >"$work/controlchars-skipped/logo.png"
expect "ignores build output, vendored bundles and non-source files" 0 "" \
    "$CONTROLCHARS" "$work/controlchars-skipped"

echo
echo "check-workflow-needs-outputs.py"

# The shape this script exists for: seven jobs read an output the producer never declares. GitHub
# resolves it to "" instead of failing, so the workflow looks correct and quietly passes nothing.
mkdir -p "$work/needs-undeclared"
cat >"$work/needs-undeclared/ci.yml" <<'YAML'
name: undeclared
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo building
  e2e:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
        env:
          IMAGE: ${{ needs.build.outputs.image-tag }}
YAML
expect "rejects a reference to an output the producer never declares" 1 "image-tag" \
    "$NEEDSOUTPUTS" "$work/needs-undeclared"

# The same workflow with the output declared: the only change that should matter. Deriving it from
# the rejected fixture rather than writing a second one is deliberate - it is what makes the pair
# evidence, since the declaration is then the only difference between a pass and a failure.
mkdir -p "$work/needs-declared"
awk '/^  build:/{print; print "    outputs:"; print "      image-tag: repo\/image:latest"; next} {print}' \
    "$work/needs-undeclared/ci.yml" >"$work/needs-declared/ci.yml"
expect "accepts the same workflow once the output is declared" 0 "" \
    "$NEEDSOUTPUTS" "$work/needs-declared"

# A job that declares SOME outputs but not the one referenced is the easier mistake to miss: the
# `outputs:` block exists, so a reader skims past it.
mkdir -p "$work/needs-wrong-name"
cat >"$work/needs-wrong-name/ci.yml" <<'YAML'
name: wrong-name
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.v.outputs.version }}
    steps:
      - id: v
        run: echo "version=1" >> $GITHUB_OUTPUT
  e2e:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
        env:
          IMAGE: ${{ needs.build.outputs.image-tag }}
YAML
expect "rejects a reference to a name the producer does not declare" 1 "declares: version" \
    "$NEEDSOUTPUTS" "$work/needs-wrong-name"

# A `needs` reference to a job that does not exist is the other half of the same typo.
mkdir -p "$work/needs-missing-job"
cat >"$work/needs-missing-job/ci.yml" <<'YAML'
name: missing-job
on: [ push ]
jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - run: echo test
        env:
          IMAGE: ${{ needs.buidl.outputs.image-tag }}
YAML
expect "rejects a reference to a job that does not exist" 1 "no job 'buidl' exists" \
    "$NEEDSOUTPUTS" "$work/needs-missing-job"

# A reference to a job the consumer does not depend on resolves to "" just as surely as an
# undeclared output does, even when the producer declares the output correctly.
mkdir -p "$work/needs-not-declared-dep"
cat >"$work/needs-not-declared-dep/ci.yml" <<'YAML'
name: not-a-dependency
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      image-tag: repo/image:latest
    steps:
      - run: echo building
  e2e:
    runs-on: ubuntu-latest
    steps:
      - run: echo test
        env:
          IMAGE: ${{ needs.build.outputs.image-tag }}
YAML
expect "rejects a reference to a job the consumer does not depend on" 1 "does not list 'build'" \
    "$NEEDSOUTPUTS" "$work/needs-not-declared-dep"

# The needs CONTEXT is not transitive, unlike an artifact: `report` waits for `test` which waits for
# `build`, so the artifact-deps script would accept the equivalent download, but `needs.build` is
# still empty here. Getting this wrong in the permissive direction would make the check accept the
# exact reference it exists to reject.
mkdir -p "$work/needs-transitive"
cat >"$work/needs-transitive/ci.yml" <<'YAML'
name: transitive-outputs
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      image-tag: repo/image:latest
    steps:
      - run: echo building
  test:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
  report:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - run: echo report
        env:
          IMAGE: ${{ needs.build.outputs.image-tag }}
YAML
expect "rejects a transitively-reached producer, because the needs context is direct-only" 1 "DIRECT" \
    "$NEEDSOUTPUTS" "$work/needs-transitive"

# Text that merely mentions the expression is not a reference. Without a ${{ }} gate an echo or an
# error message would be reported as a live violation.
mkdir -p "$work/needs-plain-text"
cat >"$work/needs-plain-text/ci.yml" <<'YAML'
name: plain-text
on: [ push ]
jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - run: echo "set needs.build.outputs.image-tag to use a custom image"
YAML
expect "ignores the expression text outside a \${{ }} block" 0 "" \
    "$NEEDSOUTPUTS" "$work/needs-plain-text"

# A reusable-workflow job declares its outputs in the called workflow, which this script does not
# read. Reporting it would be a false violation in a check that gates the whole build.
mkdir -p "$work/needs-reusable"
cat >"$work/needs-reusable/ci.yml" <<'YAML'
name: reusable
on: [ push ]
jobs:
  build:
    uses: ./.github/workflows/called.yml
  e2e:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
        env:
          IMAGE: ${{ needs.build.outputs.image-tag }}
YAML
expect "ignores a reference into a reusable-workflow job" 0 "" \
    "$NEEDSOUTPUTS" "$work/needs-reusable"

echo "check-ci-status-completeness.py"

# #6569's shape, minimal: an aggregator job that forgot to wait for one of its siblings, so that
# sibling can fail or be skipped without the aggregator ever noticing.
mkdir -p "$work/status-incomplete"
cat >"$work/status-incomplete/ci.yml" <<'YAML'
name: incomplete
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo building
  unit-tests:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
  new-lane:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
  ci-status:
    runs-on: ubuntu-latest
    if: always()
    needs: [ build, unit-tests ]
    steps:
      - run: echo status
YAML
expect "rejects an aggregator whose needs omit a sibling job" 1 "['new-lane']" \
    "$STATUSCOMPLETE" "$work/status-incomplete"

# The same workflow with `new-lane` added to the aggregator's `needs`: the only change that should
# matter, same pairing technique as the needs-outputs fixtures above.
mkdir -p "$work/status-complete"
awk '/^    needs: \[ build, unit-tests \]$/{print "    needs: [ build, unit-tests, new-lane ]"; next} {print}' \
    "$work/status-incomplete/ci.yml" >"$work/status-complete/ci.yml"
expect "accepts the same workflow once the aggregator needs every job" 0 "" \
    "$STATUSCOMPLETE" "$work/status-complete"

# A job reached only transitively still counts: `ci-status` needing `unit-tests`, which itself needs
# `build`, covers `build` without naming it directly - same closure semantics as the sibling
# artifact-deps script, deliberately unlike check-workflow-needs-outputs.py's needs CONTEXT (which is
# direct-only for an unrelated reason: see that script's own transitive-outputs fixture).
mkdir -p "$work/status-transitive"
cat >"$work/status-transitive/ci.yml" <<'YAML'
name: transitive
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo building
  unit-tests:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
  ci-status:
    runs-on: ubuntu-latest
    if: always()
    needs: unit-tests
    steps:
      - run: echo status
YAML
expect "accepts a job reached only transitively through the aggregator's needs" 0 "" \
    "$STATUSCOMPLETE" "$work/status-transitive"

# A workflow with no job named 'ci-status' or ending in '-status' opts out entirely: not every
# workflow needs (or should be forced to have) an aggregator.
mkdir -p "$work/status-no-aggregator"
cat >"$work/status-no-aggregator/ci.yml" <<'YAML'
name: no-aggregator
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo building
  unit-tests:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
YAML
expect "ignores a workflow with no status-aggregator job" 0 "" \
    "$STATUSCOMPLETE" "$work/status-no-aggregator"

# `is_aggregator()` matches by naming convention, not just the literal 'ci-status' id: anything
# ending in '-status' is held to the same bar.
mkdir -p "$work/status-suffix-match"
cat >"$work/status-suffix-match/ci.yml" <<'YAML'
name: suffix-match
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo building
  unit-tests:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - run: echo test
  gate-status:
    runs-on: ubuntu-latest
    if: always()
    needs: build
    steps:
      - run: echo status
YAML
expect "rejects an aggregator matched only by its '-status' suffix" 1 "gate-status" \
    "$STATUSCOMPLETE" "$work/status-suffix-match"

# A workflow this script cannot even parse is reported, not silently skipped - the same posture as
# the sibling needs-outputs script's own YAML-error fixture.
mkdir -p "$work/status-unparseable"
cat >"$work/status-unparseable/ci.yml" <<'YAML'
name: broken
on: [push
jobs:
  ci-status:
    runs-on: ubuntu-latest
YAML
expect "reports a workflow it cannot parse as YAML" 1 "not parseable as YAML" \
    "$STATUSCOMPLETE" "$work/status-unparseable"

echo "check-native-graalvm-pin.py"

# A tiny helper: these cases only ever vary the pinned property and the installed java-version, so
# writing two whole files per case would bury the one number that matters in boilerplate.
graalvm_pom() {
    cat >"$1" <<XML
<project>
    <properties>
        <native.graalvm.version>$2</native.graalvm.version>
    </properties>
</project>
XML
}

graalvm_workflow() {
    local path="$1" java_version="$2" step_name="${3:-Set up GraalVM}"
    cat >"$path" <<YAML
name: Native Image
on: [ workflow_dispatch ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: $step_name
        uses: graalvm/setup-graalvm@5298d94fb55a4f185c602eeac5de1b553882abe2 # v1.6.1
        with:
          java-version: "$java_version"
          distribution: "graalvm-community"
YAML
}

mkdir -p "$work/graalvm"

# The happy path: a mainline three-component pin the builder can actually be set to.
graalvm_pom "$work/graalvm/ok-pom.xml" 25.0.2
graalvm_workflow "$work/graalvm/ok.yml" 25.0.2 "Set up GraalVM (Community, JDK 25.0.2)"
expect "accepts a pin equal to the builder java-version" 0 "consistent" \
    "$GRAALVMPIN" "$work/graalvm/ok-pom.xml" "$work/graalvm/ok.yml"

# The regression this script exists for: run 33608446679, where dependabot's b53c48c1e2 left the
# pom at 25.3.4.1 while the workflow still installed 25.0.2. All three native legs died in
# TruffleBaseFeature.afterRegistration.
graalvm_pom "$work/graalvm/skew-pom.xml" 25.3.4.1
graalvm_workflow "$work/graalvm/skew.yml" 25.0.2
expect "rejects the b53c48c1e2 skew (pom 25.3.4.1, builder 25.0.2)" 1 "getLoopNodeFactory" \
    "$GRAALVMPIN" "$work/graalvm/skew-pom.xml" "$work/graalvm/skew.yml"

# Making the two sides "agree" on 25.3.4.1 is not a fix. The tag graal-25.3.4.1 exists, but
# node-semver rejects four numeric components, so findLatestGraalVMCEVersion skips it as an
# "unexpected GraalVM CE release", finds no jdk-25.3.4.1 either, and throws - this would just fail
# one step earlier, in the setup step.
graalvm_pom "$work/graalvm/unreachable-pom.xml" 25.3.4.1
graalvm_workflow "$work/graalvm/unreachable.yml" 25.3.4.1
expect "rejects a version semver.valid() cannot parse, even when both sides agree" 1 "semver.valid" \
    "$GRAALVMPIN" "$work/graalvm/unreachable-pom.xml" "$work/graalvm/unreachable.yml"

# The corollary, and the thing this check must NOT get backwards: an intermediate release is not
# itself unusable. graal-25.2.4 is one, its tag has three components, and `version: "25.2.4"`
# installs it through setUpGraalVMJDKCE - it is only unreachable through `java-version`, which can
# address jdk-* tags alone. So a pom pinned at 25.2.4 against a `version`-pinned step is fine.
graalvm_pom "$work/graalvm/version-input-pom.xml" 25.2.4
cat >"$work/graalvm/version-input.yml" <<'YAML'
name: Native Image
on: [ workflow_dispatch ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Set up GraalVM
        uses: graalvm/setup-graalvm@5298d94fb55a4f185c602eeac5de1b553882abe2 # v1.6.1
        with:
          version: "25.2.4"
          distribution: "graalvm-community"
YAML
expect "accepts an intermediate release pinned through the version input" 0 "consistent" \
    "$GRAALVMPIN" "$work/graalvm/version-input-pom.xml" "$work/graalvm/version-input.yml"

# `version` wins over `java-version` when a step sets both, the same precedence setup-graalvm's
# main.ts applies - so the pom is compared against `version`, and matching only the java-version
# is a failure.
graalvm_pom "$work/graalvm/both-inputs-pom.xml" 25.0.2
cat >"$work/graalvm/both-inputs.yml" <<'YAML'
name: Native Image
on: [ workflow_dispatch ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Set up GraalVM
        uses: graalvm/setup-graalvm@5298d94fb55a4f185c602eeac5de1b553882abe2 # v1.6.1
        with:
          version: "25.2.4"
          java-version: "25.0.2"
          distribution: "graalvm-community"
YAML
expect "compares against version, not java-version, when a step sets both" 1 "installs version 25.2.4" \
    "$GRAALVMPIN" "$work/graalvm/both-inputs-pom.xml" "$work/graalvm/both-inputs.yml"

# The step name is the first thing a reader sees, so it may not name a version other than the one
# it installs.
graalvm_pom "$work/graalvm/stale-name-pom.xml" 25.0.2
graalvm_workflow "$work/graalvm/stale-name.yml" 25.0.2 "Set up GraalVM (Community, JDK 24.0.2)"
expect "rejects a step name that contradicts the version it installs" 1 "names a version other than" \
    "$GRAALVMPIN" "$work/graalvm/stale-name-pom.xml" "$work/graalvm/stale-name.yml"

# A version-free step name is not a contradiction, so it passes.
graalvm_pom "$work/graalvm/plain-name-pom.xml" 25.0.2
graalvm_workflow "$work/graalvm/plain-name.yml" 25.0.2 "Set up GraalVM"
expect "accepts a step name that names no version at all" 0 "consistent" \
    "$GRAALVMPIN" "$work/graalvm/plain-name-pom.xml" "$work/graalvm/plain-name.yml"

# Fail closed: a workflow that no longer sets up GraalVM leaves nothing to compare, and passing on
# nothing is exactly how the pin drifted unnoticed.
graalvm_pom "$work/graalvm/no-setup-pom.xml" 25.0.2
cat >"$work/graalvm/no-setup.yml" <<'YAML'
name: Native Image
on: [ workflow_dispatch ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo building
YAML
expect "reports a workflow with no setup-graalvm step" 1 "no graalvm/setup-graalvm step" \
    "$GRAALVMPIN" "$work/graalvm/no-setup-pom.xml" "$work/graalvm/no-setup.yml"

# Same posture on the other side: a renamed or deleted property is reported, not skipped.
cat >"$work/graalvm/no-property-pom.xml" <<'XML'
<project>
    <properties>
        <graalvm.version>25.0.2</graalvm.version>
    </properties>
</project>
XML
graalvm_workflow "$work/graalvm/no-property.yml" 25.0.2
expect "reports a pom with no native.graalvm.version property" 1 "no <native.graalvm.version>" \
    "$GRAALVMPIN" "$work/graalvm/no-property-pom.xml" "$work/graalvm/no-property.yml"

# An unpinned builder is a skew waiting to happen, so it is a failure rather than a pass.
graalvm_pom "$work/graalvm/unpinned-pom.xml" 25.0.2
cat >"$work/graalvm/unpinned.yml" <<'YAML'
name: Native Image
on: [ workflow_dispatch ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Set up GraalVM
        uses: graalvm/setup-graalvm@5298d94fb55a4f185c602eeac5de1b553882abe2 # v1.6.1
        with:
          distribution: "graalvm-community"
YAML
expect "reports a setup-graalvm step that pins no version at all" 1 "neither \`version\` nor" \
    "$GRAALVMPIN" "$work/graalvm/unpinned-pom.xml" "$work/graalvm/unpinned.yml"

# Same as the sibling scripts: a workflow that will not parse is reported, never silently skipped.
graalvm_pom "$work/graalvm/broken-pom.xml" 25.0.2
cat >"$work/graalvm/broken.yml" <<'YAML'
name: broken
on: [workflow_dispatch
jobs:
  build:
YAML
expect "reports a workflow it cannot parse as YAML" 1 "not parseable as YAML" \
    "$GRAALVMPIN" "$work/graalvm/broken-pom.xml" "$work/graalvm/broken.yml"

# The repository's own two files must agree - this is the case that would have caught b53c48c1e2.
expect "the repository's own native pin matches its own workflow" 0 "consistent" \
    "$GRAALVMPIN"

echo
echo "check-native-march.py"

# Writes a native pom whose base native-maven-plugin passes $2 as its -march build arg (none when
# empty), with $3 as the native.march default and the per-arch profiles of the real pom unless $4
# is "no-profiles". Extra profile XML can be passed as $5. The recorded expected baselines
# (native.march.expected.<arch>, #10715) come from $MARCH_EXPECTED_* and default to the real pom's;
# MARCH_EXPECTED_NONE=1 leaves them out.
march_pom() {
    local path="$1" arg="$2" default="$3" profiles="${4:-}" extra="${5:-}"
    local arg_xml="" profile_xml="" expected_xml=""
    if [[ -z ${MARCH_EXPECTED_NONE:-} ]]; then
        expected_xml="
        <native.march.expected.amd64>${MARCH_EXPECTED_AMD64:-x86-64-v3}</native.march.expected.amd64>
        <native.march.expected.x86_64>${MARCH_EXPECTED_X86_64:-x86-64-v3}</native.march.expected.x86_64>
        <native.march.expected.aarch64>${MARCH_EXPECTED_AARCH64:-armv8-a}</native.march.expected.aarch64>"
    fi
    if [[ -z ${MARCH_PINS_NONE:-} ]]; then
        expected_xml="$expected_xml
        <native.march.pins-builder-default.amd64>${MARCH_PINS_AMD64:-true}</native.march.pins-builder-default.amd64>
        <native.march.pins-builder-default.x86_64>${MARCH_PINS_X86_64:-true}</native.march.pins-builder-default.x86_64>
        <native.march.pins-builder-default.aarch64>${MARCH_PINS_AARCH64:-false}</native.march.pins-builder-default.aarch64>"
    fi
    [[ -n $arg ]] && arg_xml="<buildArg>$arg</buildArg>"
    if [[ $profiles != no-profiles ]]; then
        profile_xml='
        <profile><id>native-march-amd64</id><activation><os><arch>amd64</arch></os></activation>
            <properties><native.march>x86-64-v3</native.march></properties></profile>
        <profile><id>native-march-x86_64</id><activation><os><arch>x86_64</arch></os></activation>
            <properties><native.march>x86-64-v3</native.march></properties></profile>
        <profile><id>native-march-aarch64</id><activation><os><arch>aarch64</arch></os></activation>
            <properties><native.march>armv8-a</native.march></properties></profile>'
    fi
    cat >"$path" <<XML
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <properties>
        <native.march>$default</native.march>$expected_xml
    </properties>
    <build>
        <plugins>
            <plugin>
                <groupId>org.graalvm.buildtools</groupId>
                <artifactId>native-maven-plugin</artifactId>
                <configuration>
                    <buildArgs>
                        <buildArg>--no-fallback</buildArg>
                        $arg_xml
                    </buildArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
    <profiles>$profile_xml
        $extra
    </profiles>
</project>
XML
}

mkdir -p "$work/march"
printf '## CPU requirements\n\n| Arch | -march |\n|---|---|\n| amd64 | `x86-64-v3` |\n| arm64 | `armv8-a` |\n\nOverride: `-Dnative.march=x86-64-v2`.\n\n## Next\n\n| `x86-64-v4` |\n' >"$work/march/docs.md"
printf 'jobs:\n  build:\n    steps:\n      - run: ./mvnw -Pnative -Dnative.static=true package\n' >"$work/march/clean.yml"

march_pom "$work/march/ok-pom.xml" '-march=${native.march}' compatibility
expect "accepts a per-arch -march that the docs name" 0 "amd64=x86-64-v3, x86_64=x86-64-v3, aarch64=armv8-a" \
    "$NATIVEMARCH" "$work/march/ok-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# The #10581 shape: no -march at all, so the binary silently inherits GraalVM's x86-64-v3 default.
march_pom "$work/march/none-pom.xml" "" compatibility
expect "rejects a native pom that passes no -march (#10581)" 1 "passes no -march build arg" \
    "$NATIVEMARCH" "$work/march/none-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# -march=native ties a CI-built binary to the runner's CPU, whichever way it gets in.
march_pom "$work/march/native-pom.xml" '-march=${native.march}' native no-profiles
expect "rejects a native.march that resolves to native" 1 "targets the CPU of the machine" \
    "$NATIVEMARCH" "$work/march/native-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"
printf 'jobs:\n  build:\n    steps:\n      - run: ./mvnw -Pnative -Dnative.march=native package\n' >"$work/march/native.yml"
expect "rejects a workflow that builds with -Dnative.march=native" 1 "native.yml:4" \
    "$NATIVEMARCH" "$work/march/ok-pom.xml" "$work/march/docs.md" "$work/march/native.yml"
printf 'jobs:\n  build:\n    steps:\n      - run: ./mvnw -Pnative -Dnative.march=compatibility package\n' >"$work/march/compat.yml"
expect "accepts a workflow that builds the amd64 compatibility variant with -Dnative.march=compatibility (#11253)" 0 "" \
    "$NATIVEMARCH" "$work/march/ok-pom.xml" "$work/march/docs.md" "$work/march/compat.yml"

# A profile's buildArgs are appended after the base ones, so a second -march there silently wins.
march_pom "$work/march/profile-arg-pom.xml" '-march=${native.march}' compatibility "" \
    '<profile><id>sneaky</id><build><plugins><plugin><artifactId>native-maven-plugin</artifactId>
     <configuration><buildArgs combine.children="append"><buildArg>-march=x86-64-v4</buildArg></buildArgs>
     </configuration></plugin></plugins></build></profile>'
expect "rejects a -march passed from a profile" 1 "profile 'sneaky' passes -march=x86-64-v4" \
    "$NATIVEMARCH" "$work/march/profile-arg-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# An x86 baseline on aarch64 cannot build, and means the profiles were mixed up.
march_pom "$work/march/wrong-family-pom.xml" '-march=${native.march}' x86-64-v3 no-profiles
expect "rejects a baseline from the wrong architecture" 1 "not a baseline for that architecture" \
    "$NATIVEMARCH" "$work/march/wrong-family-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# Raising the baseline without telling users is the drift this guards: docs must follow the pom.
march_pom "$work/march/v4-pom.xml" '-march=${native.march}' compatibility no-profiles \
    '<profile><id>a</id><activation><os><arch>amd64</arch></os></activation><properties><native.march>x86-64-v4</native.march></properties></profile>'
expect "rejects a baseline the docs do not name" 1 'does not name `x86-64-v4`' \
    "$NATIVEMARCH" "$work/march/v4-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# A baseline the section names only in an override example, or a table under another heading,
# states no requirement: lowering the pom to x86-64-v2 must not pass on the strength of an example.
march_pom "$work/march/v2-pom.xml" '-march=${native.march}' compatibility no-profiles \
    '<profile><id>a</id><activation><os><arch>amd64</arch></os></activation><properties><native.march>x86-64-v2</native.march></properties></profile>'
expect "rejects a baseline the docs name only outside the CPU requirements table" 1 "table does not name \`x86-64-v2\`" \
    "$NATIVEMARCH" "$work/march/v2-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"
expect "rejects a baseline only a table under another heading names" 1 "table does not name \`x86-64-v4\`" \
    "$NATIVEMARCH" "$work/march/v4-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"
printf '# Native\n\nno table here\n' >"$work/march/no-section.md"
expect "reports docs without a CPU requirements table" 1 "has no table under a 'CPU requirements' heading" \
    "$NATIVEMARCH" "$work/march/ok-pom.xml" "$work/march/no-section.md" "$work/march/clean.yml"

# The #10715 shape: the arm64 profile raised to armv8.1-a (drops every ARMv8.0 core) while the docs
# table was edited in the same change, so the docs check is satisfied by the change it should question.
# Only the recorded expected baseline catches it.
printf '## CPU requirements\n\n| Arch | -march |\n|---|---|\n| amd64 | `x86-64-v3` |\n| arm64 | `armv8.1-a` |\n' >"$work/march/docs-raised.md"
march_pom "$work/march/raised-pom.xml" '-march=${native.march}' compatibility "" \
    '<profile><id>a</id><activation><os><arch>aarch64</arch></os></activation><properties><native.march>armv8.1-a</native.march></properties></profile>'
expect "rejects a baseline raised above the recorded one even when the docs were edited to match (#10715)" 1 \
    "on aarch64 -march resolves to 'armv8.1-a' but the recorded baseline" \
    "$NATIVEMARCH" "$work/march/raised-pom.xml" "$work/march/docs-raised.md" "$work/march/clean.yml"

# Raising it deliberately is a two-line change: the profile and the recorded value, in one reviewed diff.
MARCH_EXPECTED_AARCH64=armv8.1-a march_pom "$work/march/raised-ok-pom.xml" '-march=${native.march}' compatibility "" \
    '<profile><id>a</id><activation><os><arch>aarch64</arch></os></activation><properties><native.march>armv8.1-a</native.march></properties></profile>'
expect "accepts a raised baseline once the recorded value is raised with it" 0 "aarch64=armv8.1-a" \
    "$NATIVEMARCH" "$work/march/raised-ok-pom.xml" "$work/march/docs-raised.md" "$work/march/clean.yml"

# The record is read from the top-level properties only: a profile that sets it too cannot launder a raise.
march_pom "$work/march/laundered-pom.xml" '-march=${native.march}' compatibility "" \
    '<profile><id>a</id><activation><os><arch>aarch64</arch></os></activation><properties><native.march>armv8.1-a</native.march><native.march.expected.aarch64>armv8.1-a</native.march.expected.aarch64></properties></profile>'
expect "does not let a profile override the recorded baseline" 1 "but the recorded baseline" \
    "$NATIVEMARCH" "$work/march/laundered-pom.xml" "$work/march/docs-raised.md" "$work/march/clean.yml"

# Fail closed: a pom that records no expected baseline leaves nothing to compare against.
MARCH_EXPECTED_NONE=1 march_pom "$work/march/no-expected-pom.xml" '-march=${native.march}' compatibility
expect "reports a pom that records no expected baseline" 1 "records no expected baseline for aarch64" \
    "$NATIVEMARCH" "$work/march/no-expected-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# The record of whether a baseline claims to be the builder's default is required, so the native job has a claim to check.
MARCH_PINS_NONE=1 march_pom "$work/march/no-pins-pom.xml" '-march=${native.march}' compatibility
expect "reports a pom that does not say whether its baseline is the builder's default (#11254)" 1 \
    "<native.march.pins-builder-default.aarch64> must be true or false" \
    "$NATIVEMARCH" "$work/march/no-pins-pom.xml" "$work/march/docs.md" "$work/march/clean.yml"

# The repository's own pom, docs, workflow and scripts must pass.
expect "the repository's own native CPU baseline is explicit and documented" 0 "explicit and documented" \
    "$NATIVEMARCH"

echo
echo "check-native-march-builder-default.py"

# A stand-in builder: prints what `native-image --expert-options-all` prints about -H:MicroArchitecture
# (wrapped over lines, as the real one is) with $2 as the AMD64 default and $3 as the AArch64 one.
fake_builder() {
    local path="$1" amd64="$2" aarch64="$3"
    cat >"$path" <<SH
#!/bin/sh
echo "  -H:MicroArchitecture=...     Generate instructions for a specific machine type. Defaults to '$amd64' on AMD64"
echo "                               and '$aarch64' on AArch64. Use -march=compatibility for best compatibility."
SH
    chmod +x "$path"
}

mkdir -p "$work/marchdefault"
D="$work/marchdefault"
march_pom "$D/pom.xml" '-march=${native.march}' compatibility
fake_builder "$D/builder" x86-64-v3 armv8.1-a

expect "accepts amd64 pinning the builder's default" 0 "builder default 'x86-64-v3', pom baseline 'x86-64-v3'" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder" amd64
expect "accepts a macOS Intel build (x86_64) the same way" 0 "builder default 'x86-64-v3'" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder" x86_64
expect "accepts aarch64 recorded as a deliberate departure from the builder's default" 0 \
    "builder default 'armv8.1-a', pom baseline 'armv8-a'" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder" arm64

# The #10715 shape: the pom claims it pins the builder's default, and the builder's default is another value.
MARCH_PINS_AARCH64=true march_pom "$D/claims-pom.xml" '-march=${native.march}' compatibility
expect "rejects a pom that claims armv8-a is the builder's default when it is armv8.1-a (#10715, #11254)" 1 \
    "this builder defaults to 'armv8.1-a'" \
    "$NATIVEMARCHDEFAULT" "$D/claims-pom.xml" "$D/builder" aarch64

# The builder moved its default: a baseline recorded as deliberate that now equals it is a stale record.
fake_builder "$D/builder-moved" x86-64-v3 armv8-a
expect "rejects a deliberate-departure record the builder's default has caught up with" 1 \
    "departs from the builder's default, but this builder defaults to 'armv8-a' too" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder-moved" aarch64
fake_builder "$D/builder-v4" x86-64-v4 armv8.1-a
expect "rejects amd64 claiming the default when the builder's default is another level" 1 \
    "this builder defaults to 'x86-64-v4'" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder-v4" amd64

# Fail closed: an unreadable default leaves the claim unverified, which is what this check exists to end.
printf '#!/bin/sh\necho "no such sentence"\n' >"$D/builder-silent"
chmod +x "$D/builder-silent"
expect "fails when the builder's help no longer states its default" 1 "claim about it is unverified" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder-silent" amd64
expect "fails when there is no native-image to ask" 1 "claim about it is unverified" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/no-such-builder" amd64
MARCH_PINS_NONE=1 march_pom "$D/no-pins-pom.xml" '-march=${native.march}' compatibility
expect "fails when the pom records no claim" 1 "is '', not true or false" \
    "$NATIVEMARCHDEFAULT" "$D/no-pins-pom.xml" "$D/builder" amd64
expect "skips an architecture the pom records no baseline for" 0 "nothing to verify" \
    "$NATIVEMARCHDEFAULT" "$D/pom.xml" "$D/builder" riscv64

echo
echo "check-release-upload-assets.py"

# These cases vary only the asset arguments of a `gh release upload` step, so the workflow around
# them is boilerplate.
release_workflow() {
    local path="$1"
    shift
    cat >"$path" <<YAML
name: Release
on:
  release:
    types: [ published ]
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - name: Attach to release
        env:
          GH_TOKEN: \${{ secrets.GITHUB_TOKEN }}
        run: |
          gh release upload "\${{ github.event.release.tag_name }}" \\
            $* \\
            --clobber
YAML
}

mkdir -p "$work/releaseassets"

# The regression this check exists for: run 33796693420. Each matrix leg packages exactly one
# archive, so the .zip pattern matched nothing on the three Unix legs and the .tar.gz pattern
# matched nothing on Windows - `gh release upload` exits nonzero on a zero-match pattern, so all
# four legs failed AFTER building and packaging their binary successfully.
release_workflow "$work/releaseassets/glob.yml" \
    'native/target/arcadedb-*.tar.gz native/target/arcadedb-*.zip native/target/*.sha256'
expect "rejects the run 33796693420 asset globs" 1 "arcadedb-*.zip" \
    "$RELEASEASSETS" "$work/releaseassets/glob.yml"

# The fix: names computed by an earlier step and passed through a step output.
release_workflow "$work/releaseassets/explicit.yml" \
    '"native/target/$ARCHIVE" "native/target/$ARCHIVE.sha256"'
expect "accepts assets named through a shell variable" 0 "explicit" \
    "$RELEASEASSETS" "$work/releaseassets/explicit.yml"

# A single glob is no safer than three - it still cannot report which leg produced nothing.
release_workflow "$work/releaseassets/single-glob.yml" 'dist/arcadedb-?.tar.gz'
expect "rejects a '?' glob as well as '*'" 1 "arcadedb-?.tar.gz" \
    "$RELEASEASSETS" "$work/releaseassets/single-glob.yml"

# The tag is a positional too, and it is not an asset - a tag holding a glob character must not be
# reported, or the check would fire on workflows that upload nothing patterned.
cat >"$work/releaseassets/tag-only.yml" <<'YAML'
name: Release
on: [ workflow_dispatch ]
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - name: Attach to release
        run: gh release upload "v1.*" dist/arcadedb.tar.gz --clobber
YAML
expect "does not treat the tag argument as an asset" 0 "explicit" \
    "$RELEASEASSETS" "$work/releaseassets/tag-only.yml"

# A workflow with no `gh release upload` at all passes rather than erroring.
cat >"$work/releaseassets/none.yml" <<'YAML'
name: Build
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: ls dist/*.tar.gz
YAML
expect "ignores workflows that never upload a release asset" 0 "explicit" \
    "$RELEASEASSETS" "$work/releaseassets/none.yml"

# Fail closed on a workflow it cannot read, exactly as the GraalVM pin check does.
cat >"$work/releaseassets/broken.yml" <<'YAML'
name: broken
on: [workflow_dispatch
jobs:
  build:
YAML
expect "reports a workflow it cannot parse as YAML" 1 "not parseable as YAML" \
    "$RELEASEASSETS" "$work/releaseassets/broken.yml"

# The repository's own workflows must pass - this is the case that would have caught run 33796693420.
expect "the repository's own workflows name their release assets" 0 "explicit" \
    "$RELEASEASSETS"

echo
echo "check-deploy-credential-scope.py"

# These cases vary only the deploy command, so the job around it - setup-java wiring the 'central'
# server's credentials, then one Maven step - is boilerplate.
deploy_workflow() {
    local path="$1" command="$2"
    cat >"$path" <<YAML
name: Deploy
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-java@0000000000000000000000000000000000000000 # v6
        with:
          distribution: temurin
          java-version: 21
          server-id: central
          server-username: MAVEN_USERNAME
          server-password: MAVEN_PASSWORD
      - name: Publish
        run: |
          $command
YAML
}

mkdir -p "$work/credscope"

# The regression this check exists for: #10023. Under Maven 3.10 the credentials of server 'central'
# only reach Maven Central's own origin, so the snapshot upload to central.sonatype.com carried none
# and every `Java CI - deploy` run on main failed with HTTP 401 from 2026-10-08 on.
deploy_workflow "$work/credscope/bare.yml" \
    'mvn deploy -Pdeploy -Pdocker -pl !e2e --batch-mode --file pom.xml -DskipTests'
expect "rejects the #10023 deploy command without the credential scope" 1 "mvn deploy -Pdeploy" \
    "$CREDSCOPE" "$work/credscope/bare.yml"

# The fix, in both positions the repository's workflows use it.
deploy_workflow "$work/credscope/scoped.yml" \
    'mvn deploy -Pdeploy -pl !e2e --batch-mode -DskipTests -Dmaven.repository.credentialScope=id'
expect "accepts a deploy that restores id-scoped credentials" 0 "credential scope" \
    "$CREDSCOPE" "$work/credscope/scoped.yml"
deploy_workflow "$work/credscope/goal-last.yml" \
    'mvn -Pdeploy,deploy-central --batch-mode -Dmaven.repository.credentialScope=id deploy'
expect "accepts the flag ahead of a trailing deploy goal" 0 "credential scope" \
    "$CREDSCOPE" "$work/credscope/goal-last.yml"

# The goal-last shape without the flag is the release workflow's, and must be caught as well.
deploy_workflow "$work/credscope/goal-last-bare.yml" \
    'mvn -Pdeploy,deploy-central --batch-mode -DskipTests=true -pl !e2e deploy'
expect "rejects a trailing deploy goal without the flag" 1 "deploy-central" \
    "$CREDSCOPE" "$work/credscope/goal-last-bare.yml"

# The wrapper is the same Maven.
deploy_workflow "$work/credscope/wrapper.yml" './mvnw --batch-mode deploy:deploy'
expect "rejects ./mvnw deploy:deploy without the flag" 1 "mvnw" \
    "$CREDSCOPE" "$work/credscope/wrapper.yml"

# MAVEN_ARGS is read by Maven itself (3.9+), so a step env that sets the flag there is as good.
cat >"$work/credscope/maven-args.yml" <<'YAML'
name: Deploy
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-java@0000000000000000000000000000000000000000 # v6
        with:
          server-id: central
      - name: Publish
        env:
          MAVEN_ARGS: -Dmaven.repository.credentialScope=id
        run: mvn deploy
YAML
expect "accepts the flag passed through MAVEN_ARGS" 0 "credential scope" \
    "$CREDSCOPE" "$work/credscope/maven-args.yml"

# MAVEN_ARGS is honoured at job and workflow level as well, as GitHub merges those into the step.
for level in workflow job; do
    if [[ $level == workflow ]]; then
        workflow_env=$'env:\n  MAVEN_ARGS: -Dmaven.repository.credentialScope=id'
        job_env=""
    else
        workflow_env=""
        job_env=$'    env:\n      MAVEN_ARGS: -Dmaven.repository.credentialScope=id'
    fi
    cat >"$work/credscope/maven-args-$level.yml" <<YAML
name: Deploy
on: [ push ]
$workflow_env
jobs:
  build:
    runs-on: ubuntu-latest
$job_env
    steps:
      - uses: actions/setup-java@0000000000000000000000000000000000000000 # v6
        with:
          server-id: central
      - run: mvn deploy
YAML
    expect "accepts the flag passed through $level-level MAVEN_ARGS" 0 "credential scope" \
        "$CREDSCOPE" "$work/credscope/maven-args-$level.yml"
done

# A command continued over several lines is still one invocation: the flag on the last line counts,
# and a continued deploy without it is still caught.
deploy_workflow "$work/credscope/continued.yml" $'mvn deploy -Pdeploy \\\n            --batch-mode \\\n            -Dmaven.repository.credentialScope=id'
expect "accepts the flag on a continuation line" 0 "credential scope" \
    "$CREDSCOPE" "$work/credscope/continued.yml"
deploy_workflow "$work/credscope/continued-bare.yml" $'mvn deploy -Pdeploy \\\n            --batch-mode -DskipTests'
expect "rejects a continued deploy without the flag" 1 "mvn deploy -Pdeploy --batch-mode -DskipTests" \
    "$CREDSCOPE" "$work/credscope/continued-bare.yml"

# `-Pdeploy` is a profile, not the deploy goal: a `verify` with that profile uploads nothing.
deploy_workflow "$work/credscope/profile-only.yml" 'mvn verify -Pdeploy -pl e2e'
expect "does not treat the -Pdeploy profile as the deploy goal" 0 "credential scope" \
    "$CREDSCOPE" "$work/credscope/profile-only.yml"

# A job that wires no server credentials has nothing to scope - its deploy fails for another reason.
cat >"$work/credscope/no-server.yml" <<'YAML'
name: Deploy
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-java@0000000000000000000000000000000000000000 # v6
        with:
          java-version: 21
      - run: mvn deploy
YAML
expect "ignores a job that declares no server credentials" 0 "credential scope" \
    "$CREDSCOPE" "$work/credscope/no-server.yml"

# Fail closed on a workflow it cannot read, as the other workflow checks do.
cat >"$work/credscope/broken.yml" <<'YAML'
name: broken
on: [workflow_dispatch
jobs:
  build:
YAML
expect "reports a workflow it cannot parse as YAML" 1 "not parseable as YAML" \
    "$CREDSCOPE" "$work/credscope/broken.yml"

# The repository's own workflows must pass - this is the case that would have caught #10023.
expect "the repository's own deploy workflows scope their credentials" 0 "credential scope" \
    "$CREDSCOPE"

echo
echo "check-gitignore-src-paths.sh"

GITIGNORESRC="$PWD/$SCRIPTS/check-gitignore-src-paths.sh"

# A repository of its own, so the probes see only the fixture's patterns. A module source tree, one tracked file in it
gitignore_repo() {
    local repo="$work/gitignore-$1"
    mkdir -p "$repo/mod/src/main/java" "$repo/py/src/pkg"
    echo "class A {}" >"$repo/mod/src/main/java/A.java"
    echo "" >"$repo/py/src/pkg/__init__.py"
    git -C "$repo" init -q
    printf '%s\n' "${@:2}" >"$repo/.gitignore"
    git -C "$repo" add -A
    echo "$repo"
}

# The #10162 shape: an unanchored runtime-output name hides a package of every source tree
repo="$(gitignore_repo unanchored 'out' '**/replication/' '*.log')"
expect "reports an unanchored directory name that hides a source package" 1 "replication" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

# The fix: one negation re-includes every directory of the source trees, the file patterns still apply
repo="$(gitignore_repo negated 'out' '**/replication/' '*.log' '!**/src/**/')"
expect "accepts the names once the source trees are re-included" 0 "No ignore pattern hides" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

# A pattern written AFTER the negation wins over it, which is how a later edit reopens the hole
repo="$(gitignore_repo after-negation '!**/src/**/' 'logs')"
expect "reports a name added below the negation" 1 "logs" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

# A path pattern that points into a source tree, written after the negation, hides that package too
repo="$(gitignore_repo src-path '!**/src/**/' 'mod/src/main/java/newpackage/')"
expect "reports a path pattern pointing into a source tree" 1 "mod/src/main/java/newpackage" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

# ... unless it names output that is deliberately written there
repo="$(gitignore_repo src-path-deliberate '!**/src/**/' '/grpc/src/generated/' '**/src/**/__pycache__/')"
expect "accepts the deliberate output paths inside a source tree" 0 "No ignore pattern hides" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

# A module's own .gitignore governs its own source trees, and nested files win over the root negation
repo="$(gitignore_repo nested '!**/src/**/')"
printf '%s\n' 'build/' >"$repo/py/.gitignore"
git -C "$repo" add -A
expect "reports a module .gitignore hiding its own source package" 1 "py/src/main/probe/build" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"
printf '%s\n' 'build/' '!src/**/' >"$repo/py/.gitignore"
expect "accepts the module .gitignore once it re-includes its source trees" 0 "No ignore pattern hides" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

# Anchored patterns, wildcards and the deliberate names are not probed
repo="$(gitignore_repo not-probed '/out' 'mod/target/' '*.tmp' '__pycache__/')"
expect "does not probe anchored, wildcard or deliberate patterns" 0 "No ignore pattern hides" \
    bash -c "cd '$repo' && '$GITIGNORESRC'"

expect "the repository's own ignore files hide no source directory" 0 "No ignore pattern hides" \
    "$GITIGNORESRC"

echo
echo "check-maven-wrapper.py"

mkdir -p "$work/wrapper"

# Writes a one-step workflow whose `run:` is the given script.
maven_workflow() {
    local path="$1" script="$2"
    {
        cat <<YAML
name: Build
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Build
        run: |
YAML
        printf '%s\n' "$script" | sed 's/^/          /'
    } >"$path"
}

# The regression this check exists for: #10134. mvn-deploy.yml published every snapshot with the
# runner image's Maven, which is how a runner Maven bump broke it with no change in the repository
# (#10023), and how the same bump broke the release bundle a week later (#10111).
maven_workflow "$work/wrapper/bare.yml" 'mvn deploy -Pdeploy --batch-mode -DskipTests'
expect "rejects the runner's mvn" 1 "mvn deploy -Pdeploy" \
    "$WRAPPER" "$work/wrapper/bare.yml"

maven_workflow "$work/wrapper/wrapper.yml" './mvnw deploy -Pdeploy --batch-mode -DskipTests'
expect "accepts ./mvnw" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/wrapper.yml"

# The shapes the census in #10134 missed or that a single-line grep would: a command substitution
# (mvn-test.yml's `Get project version`), a continued command, a later command in a multi-line
# script (license-compliance.yml), an `&&` chain, a pipe, and an absolute path to the runner's Maven.
maven_workflow "$work/wrapper/substitution.yml" 'echo "version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)" >> $GITHUB_OUTPUT'
expect "rejects mvn inside \$(...)" 1 "mvn help:evaluate" \
    "$WRAPPER" "$work/wrapper/substitution.yml"
maven_workflow "$work/wrapper/backtick.yml" 'echo "version=`mvn help:evaluate -q -DforceStdout`"'
expect "rejects mvn inside backticks" 1 "mvn help:evaluate" \
    "$WRAPPER" "$work/wrapper/backtick.yml"
maven_workflow "$work/wrapper/continued.yml" $'mvn -B -ntp \\\n  -Pnative package'
expect "rejects a continued mvn command" 1 "mvn -B -ntp -Pnative package" \
    "$WRAPPER" "$work/wrapper/continued.yml"
maven_workflow "$work/wrapper/later-line.yml" $'echo "Generating the report..."\nmvn org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party -DskipTests'
expect "rejects mvn on a later line of the script" 1 "aggregate-add-third-party" \
    "$WRAPPER" "$work/wrapper/later-line.yml"
maven_workflow "$work/wrapper/chained.yml" './mvnw -q install -pl e2e -am && mvn verify -pl e2e'
expect "rejects mvn after && even when ./mvnw precedes it" 1 "mvn verify -pl e2e" \
    "$WRAPPER" "$work/wrapper/chained.yml"
maven_workflow "$work/wrapper/piped.yml" 'mvn dependency:tree | tee deps.txt'
expect "rejects mvn piped into another command" 1 "mvn dependency:tree" \
    "$WRAPPER" "$work/wrapper/piped.yml"
maven_workflow "$work/wrapper/absolute.yml" '/usr/share/apache-maven/bin/mvn verify'
expect "rejects an absolute path to the runner's mvn" 1 "/usr/share/apache-maven/bin/mvn verify" \
    "$WRAPPER" "$work/wrapper/absolute.yml"
maven_workflow "$work/wrapper/prefixed.yml" 'MAVEN_OPTS=-Xmx2g time mvn verify'
expect "rejects mvn behind an env assignment and a command prefix" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/prefixed.yml"
maven_workflow "$work/wrapper/if-then.yml" $'if [ -f pom.xml ]; then\n  mvn verify\nfi'
expect "rejects mvn inside an if block" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/if-then.yml"
# A runner prefix with options of its own still runs Maven (review of #10175): the program is not
# the option that follows the prefix.
for prefixed in 'sudo -E mvn verify' 'env -i PATH=/usr/bin mvn verify' 'time -p mvn verify' \
    'timeout 10m mvn verify' 'xargs -n1 mvn verify' 'sudo -u builder mvn verify' 'nice -n 5 mvn verify'; do
    maven_workflow "$work/wrapper/runner-prefix.yml" "$prefixed"
    expect "rejects mvn behind '${prefixed% mvn verify}'" 1 "mvn verify" \
        "$WRAPPER" "$work/wrapper/runner-prefix.yml"
done
maven_workflow "$work/wrapper/runner-wrapped.yml" $'sudo -E ./mvnw verify\nsudo apt-get install -y maven'
expect "accepts a runner prefix in front of the wrapper or of another program" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/runner-wrapped.yml"

# The body of a `case` pattern is a command.
maven_workflow "$work/wrapper/case.yml" $'case "$MODE" in\n  full) mvn verify ;;\n  *) ./mvnw test ;;\nesac'
expect "rejects mvn in a case pattern body" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/case.yml"

# A script given inline to a shell is a script too, in either quote form.
maven_workflow "$work/wrapper/bash-c.yml" 'bash -c "mvn verify"'
expect "rejects mvn inside bash -c \"...\"" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/bash-c.yml"
maven_workflow "$work/wrapper/sh-ec.yml" "sh -ec 'cd e2e && mvn verify'"
expect "rejects mvn inside sh -ec '...'" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/sh-ec.yml"
maven_workflow "$work/wrapper/bash-c-wrapped.yml" "bash -c './mvnw verify'"
expect "accepts the wrapper inside bash -c" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/bash-c-wrapped.yml"

# A shell option that takes an argument is not the script file (second review of #10175).
for inline in "bash -o pipefail -c 'mvn verify'" "bash -O extglob -c 'mvn verify'" \
    "bash --noprofile -c 'mvn verify'" "bash +o posix -ec 'mvn verify'"; do
    maven_workflow "$work/wrapper/shell-options.yml" "$inline"
    expect "rejects mvn in '$inline'" 1 "mvn verify" \
        "$WRAPPER" "$work/wrapper/shell-options.yml"
done

# A here-doc body is read as commands: Maven piped into a shell through one is caught, and a
# here-doc that only writes such a line is reported too, failing closed.
maven_workflow "$work/wrapper/heredoc-shell.yml" $'bash <<EOF\nmvn verify\nEOF'
expect "rejects mvn in a here-doc fed to a shell" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/heredoc-shell.yml"
maven_workflow "$work/wrapper/heredoc-file.yml" $'cat > build.sh <<\'EOF\'\nmvn verify\nEOF'
expect "reports mvn in a here-doc that writes a file (fails closed)" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/heredoc-file.yml"

# Unbalanced quotes fall back to a plain split, which must still find the command.
maven_workflow "$work/wrapper/unbalanced.yml" $'echo "it is #1; mvn verify'
expect "rejects mvn in a script with unbalanced quotes" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/unbalanced.yml"

maven_workflow "$work/wrapper/mvnd.yml" 'mvnd verify'
expect "rejects the Maven daemon, which is no more pinned than mvn" 1 "mvnd verify" \
    "$WRAPPER" "$work/wrapper/mvnd.yml"

# What must NOT be flagged: words that merely mention mvn, a comment, the wrapper's Windows script,
# a wrapper invoked from a subdirectory, and a ./mvnw step on a later line after an echo.
maven_workflow "$work/wrapper/mentions.yml" $'# mvn is not used here (mvn verify)\necho "mvn verify would use the runner Maven"\nls mvn-deploy.yml\n./mvnw verify'
expect "ignores comments, strings and file names that mention mvn" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/mentions.yml"
# Nor is a substitution that is not one: inside a comment (check-test-database-paths.sh says
# "`mvn clean` cannot remove it") or inside single quotes (this file's own fixtures). A substitution
# inside double quotes IS one, and is still caught (the `$(...)` case above).
maven_workflow "$work/wrapper/inert.yml" $'# `mvn clean` cannot remove it, nor can $(mvn clean)\nprintf \'%s\\n\' \'version=$(mvn help:evaluate)\'\necho $\'it\\\'s `mvn verify`\'\n./mvnw verify'
expect "ignores substitutions inside comments and single quotes" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/inert.yml"
maven_workflow "$work/wrapper/subdir.yml" $'cd package\n../mvnw verify\n./mvnw.cmd verify'
expect "accepts ../mvnw and mvnw.cmd" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/subdir.yml"

# A GitHub expression can hold quotes that would otherwise mis-tokenize the command.
maven_workflow "$work/wrapper/expression.yml" './mvnw versions:set "-DnewVersion=${{ github.event.inputs.releaseversion }}"'
expect "accepts a wrapper call carrying a GitHub expression" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/expression.yml"

# Composite actions run steps too, and are checked the same way.
mkdir -p "$work/wrapper/action"
cat >"$work/wrapper/action/action.yml" <<'YAML'
name: Build
runs:
  using: composite
  steps:
    - shell: bash
      run: mvn verify
YAML
expect "rejects mvn in a composite action's step" 1 "mvn verify" \
    "$WRAPPER" "$work/wrapper/action/action.yml"

# A shell script of the repository that a step runs is checked too, resolved against the step's
# working directory: package/test-builder-local.sh ran the runner's mvn from mvn-test.yml that way.
mkdir -p "$work/wrapper/scripts"
printf '#!/bin/bash\nVERSION=$(mvn -f ../pom.xml help:evaluate -q -DforceStdout)\n' >"$work/wrapper/scripts/bare.sh"
printf '#!/bin/bash\nVERSION=$(../mvnw -f ../pom.xml help:evaluate -q -DforceStdout)\n' >"$work/wrapper/scripts/wrapped.sh"
for script in bare wrapped; do
    cat >"$work/wrapper/runs-$script.yml" <<YAML
name: Build
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Run script
        working-directory: $work/wrapper/scripts
        run: ./$script.sh
YAML
done
expect "rejects mvn inside a script the step runs" 1 "./bare.sh runs the runner's Maven" \
    "$WRAPPER" "$work/wrapper/runs-bare.yml"
expect "accepts a script the step runs that uses the wrapper" 0 "Maven wrapper" \
    "$WRAPPER" "$work/wrapper/runs-wrapped.yml"
# A script handed to a shell, or sourced, is followed just like one run as a command.
for call in 'bash ./bare.sh' 'sh -e bare.sh' 'source ./bare.sh' '. ./bare.sh'; do
    cat >"$work/wrapper/runs-via-shell.yml" <<YAML
name: Build
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - working-directory: $work/wrapper/scripts
        run: $call
YAML
    expect "rejects mvn inside a script run as '$call'" 1 "bare.sh runs the runner's Maven" \
        "$WRAPPER" "$work/wrapper/runs-via-shell.yml"
done
# A script named through $GITHUB_WORKSPACE is the repository's own; the real repository's
# test-builder-local.sh is used, as $GITHUB_WORKSPACE always resolves to the repository root.
for call in 'bash "$GITHUB_WORKSPACE/package/test-builder-local.sh"' '${GITHUB_WORKSPACE}/package/test-builder-local.sh'; do
    cat >"$work/wrapper/runs-workspace.yml" <<YAML
name: Build
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: $call
YAML
    expect "follows a script named through \$GITHUB_WORKSPACE ('$call')" 0 "1 ./mvnw invocation" \
        "$WRAPPER" "$work/wrapper/runs-workspace.yml"
done
cat >"$work/wrapper/runs-bare-job-default.yml" <<YAML
name: Build
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: $work/wrapper/scripts
    steps:
      - run: ./bare.sh
YAML
expect "resolves a script against the job's default working directory" 1 "./bare.sh runs the runner's Maven" \
    "$WRAPPER" "$work/wrapper/runs-bare-job-default.yml"

# Fail closed on what it cannot read, as the other workflow checks do.
cat >"$work/wrapper/broken.yml" <<'YAML'
name: broken
on: [workflow_dispatch
jobs:
  build:
YAML
expect "reports a workflow it cannot parse as YAML" 1 "not parseable as YAML" \
    "$WRAPPER" "$work/wrapper/broken.yml"
expect "reports a workflow it cannot find" 1 "not found" \
    "$WRAPPER" "$work/wrapper/missing.yml"

# The repository's own workflows must pass - this is the case that would have caught #10134.
expect "the repository's own workflows invoke Maven through the wrapper" 0 "Maven wrapper" \
    "$WRAPPER"

echo "check-action-pin-comments.py"

# Tag listings stand in for `git ls-remote --tags`, so these cases do not move with upstream tags.
# The #10602 shape: codeql-action's SHA is v4.39.0 (and the moving `v4`), commented `# v3.29.5`.
mkdir -p "$work/pins/tags/github" "$work/pins/tags/actions" "$work/pins/tags/graalvm"
cat >"$work/pins/tags/github/codeql-action.txt" <<'TXT'
1111111111111111111111111111111111111111	refs/tags/v3.29.5
6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee	refs/tags/v4
6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee	refs/tags/v4.39.0
TXT
# An annotated tag: the plain ref names the tag object, the peeled `^{}` ref names the commit.
cat >"$work/pins/tags/actions/checkout.txt" <<'TXT'
2222222222222222222222222222222222222222	refs/tags/v7.0.1
3d3c42e5aac5ba805825da76410c181273ba90b1	refs/tags/v7.0.1^{}
TXT
cat >"$work/pins/tags/graalvm/setup-graalvm.txt" <<'TXT'
27b360dbb857524eaf0b4c4cef364d38e5b4f173	refs/tags/v1.6.7
TXT
pin_workflow() {
    cat >"$work/pins/$1.yml" <<YAML
name: pins
on: [ push ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - name: Analyze
        uses: $2
      - uses: ./.github/actions/local
      - uses: docker://alpine:3
YAML
}
pin_workflow drifted 'github/codeql-action/analyze@6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee          # v3.29.5'
expect "rejects a comment naming a release that is not at the SHA" 1 "commented \`# v3.29.5\` but the tags at that SHA are [v4, v4.39.0]" \
    "$PINCOMMENTS" --tags-dir "$work/pins/tags" "$work/pins/drifted.yml"
pin_workflow graalvm 'graalvm/setup-graalvm@27b360dbb857524eaf0b4c4cef364d38e5b4f173 # v1.6.1'
expect "rejects a stale patch release in the comment" 1 "graalvm/setup-graalvm@27b360dbb857 is commented \`# v1.6.1\`" \
    "$PINCOMMENTS" --tags-dir "$work/pins/tags" "$work/pins/graalvm.yml"
pin_workflow correct 'github/codeql-action/analyze@6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee # v4.39.0'
expect "accepts a comment naming a tag at the SHA, direct or peeled, and skips local and docker refs" 0 "2 SHA-pinned action references across 2 repositories" \
    "$PINCOMMENTS" --tags-dir "$work/pins/tags" "$work/pins/correct.yml"
pin_workflow uncommented 'github/codeql-action/analyze@6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee'
expect "rejects a SHA pin with no version comment" 1 "has no version comment; append \`# <tag>\` naming the release at that SHA (v4, v4.39.0)" \
    "$PINCOMMENTS" --tags-dir "$work/pins/tags" "$work/pins/uncommented.yml"
pin_workflow unlisted 'nobody/missing@6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee # v1.0.0'
expect "fails closed when the tags of an action cannot be listed" 1 "no tag listing at" \
    "$PINCOMMENTS" --tags-dir "$work/pins/tags" "$work/pins/unlisted.yml"
expect "reports a workflow it cannot find" 1 "not found" \
    "$PINCOMMENTS" --tags-dir "$work/pins/tags" "$work/pins/missing.yml"
expect "accepts --tags-dir after the workflow arguments" 1 "commented \`# v3.29.5\`" \
    "$PINCOMMENTS" "$work/pins/drifted.yml" --tags-dir "$work/pins/tags"

echo
echo "chaos-run-summary.sh (#10603)"

# The run page of the HA chaos workflow is rendered from summary.json. It used to print seven
# hand-picked keys, so the reformat counters, the late commits and the whole ledger never reached
# it. The renderer now walks every leaf, so a key reaches the page by existing in the report.
mkdir -p "$work/chaos/run1"
cat >"$work/chaos/run1/summary.json" <<'JSON'
{
  "seed": 42,
  "nodes": 3,
  "duration": "PT1H",
  "writers": 4,
  "facetWriters": 1,
  "faults": "kill:3,pause",
  "result": "PASS",
  "message": "Completed 12 steps\nsecond line",
  "steps": 12,
  "ledger": { "acked": 100, "ackedLate": 1, "failed": 2, "unknown": 0, "lostUnknown": 0, "inFlight": 0 },
  "lateCommits": 5,
  "reformats": 7,
  "stepsWithUnreadReformats": 1,
  "facetWarnings": [],
  "facetWarningsDropped": 0,
  "facets": { "checks": 3 },
  "replay": "./mvnw verify -Dchaos.seed=42",
  "addedLater": [],
  "nothing": null
}
JSON
chaos_out="$("$CHAOSSUMMARY" "$work/chaos/run1/summary.json" 2>&1)" || true

if grep -qxF '### HA chaos: PASS' <<<"$chaos_out"; then
    pass "renders the result as the heading"
else
    fail "renders the result as the heading" "$chaos_out"
fi
for leaf in seed nodes duration writers faults message steps ledger.acked ledger.ackedLate ledger.failed ledger.unknown \
    ledger.lostUnknown ledger.inFlight lateCommits reformats stepsWithUnreadReformats facetWriters facetWarnings \
    facetWarningsDropped facets.checks replay addedLater nothing; do
    if grep -qE "^- ${leaf//./\\.}: " <<<"$chaos_out"; then
        pass "prints the '$leaf' field"
    else
        fail "prints the '$leaf' field" "$chaos_out"
    fi
done
if grep -qxF -- '- reformats: `` 7 ``' <<<"$chaos_out" && grep -qxF -- '- ledger.acked: `` 100 ``' <<<"$chaos_out"; then
    pass "prints the value next to its key"
else
    fail "prints the value next to its key" "$chaos_out"
fi
if grep -qxF -- '- message: `` Completed 12 steps second line ``' <<<"$chaos_out"; then
    pass "keeps a multi-line message on its own bullet"
else
    fail "keeps a multi-line message on its own bullet" "$chaos_out"
fi
if grep -qE '^- result: ' <<<"$chaos_out"; then
    fail "does not repeat the result below the heading" "$chaos_out"
else
    pass "does not repeat the result below the heading"
fi

# The fixture above must keep covering every key the harness writes, or the cases above stop
# proving that the real summary reaches the page.
chaos_keys="$(sed -n '/public .*void summary(/,/Files.writeString/p' \
    e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosReport.java | grep -oE '\.put\("[a-zA-Z]+"' | sed -E 's/\.put\("(.*)"/\1/' || true)"
if [[ -z $chaos_keys ]]; then
    fail "reads the keys ChaosReport.summary writes" "no .put(\"...\") found in ChaosReport.summary"
else
    missing_keys=""
    for key in $chaos_keys; do
        jq -e --arg k "$key" '[paths | map(tostring) | last] | index($k) != null' "$work/chaos/run1/summary.json" >/dev/null \
            || missing_keys="$missing_keys $key"
    done
    if [[ -z $missing_keys ]]; then
        pass "the fixture covers every key ChaosReport.summary writes"
    else
        fail "the fixture covers every key ChaosReport.summary writes" "missing:$missing_keys"
    fi
fi

expect "says so when no summary was written" 0 "no summary written" \
    "$CHAOSSUMMARY" "$work/chaos/absent/summary.json"
if "$CHAOSSUMMARY" "$work/chaos/run1/summary.json" "$work/chaos/run1/summary.json" | grep -c '^### HA chaos: PASS$' | grep -qx 2; then
    pass "renders one section per summary file"
else
    fail "renders one section per summary file"
fi

printf '{ "result": "PASS", "message": "a``b`" }' >"$work/chaos/ticks.json"
chaos_out="$("$CHAOSSUMMARY" "$work/chaos/ticks.json" 2>&1)" || true
if grep -qxF -- '- message: ``` a``b` ```' <<<"$chaos_out"; then
    pass "fences a value longer than any backtick run inside it"
else
    fail "fences a value longer than any backtick run inside it" "$chaos_out"
fi

printf '{ "result": ' >"$work/chaos/broken.json"
chaos_out="$("$CHAOSSUMMARY" "$work/chaos/broken.json" "$work/chaos/run1/summary.json" 2>&1)" && chaos_status=0 || chaos_status=$?
if [[ $chaos_status -eq 0 ]] && grep -qF 'unreadable summary' <<<"$chaos_out" && grep -qxF '### HA chaos: PASS' <<<"$chaos_out"; then
    pass "an unreadable summary does not hide the ones after it"
else
    fail "an unreadable summary does not hide the ones after it" "$chaos_out"
fi

# The workflow must render through the script, not through a hand-written key list again.
if grep -q 'chaos-run-summary.sh' .github/workflows/ha-chaos-tests.yml && ! grep -qF '\(.seed)' .github/workflows/ha-chaos-tests.yml; then
    pass "the HA chaos workflow renders its run summary through chaos-run-summary.sh"
else
    fail "the HA chaos workflow renders its run summary through chaos-run-summary.sh"
fi

echo
echo "check-claude-prompt-mutations.py"

mkdir -p "$work/promptmut"
# Writes a claude-code-action workflow: $1 file, $2 job permissions block, $3 allowed tools, $4 prompt line.
promptmut_workflow() {
    cat >"$work/promptmut/$1" <<YAML
name: Review
on: [ pull_request ]
jobs:
  review:
    runs-on: ubuntu-latest
$2
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          prompt: |
            Please review this pull request.
            $4
          claude_args: '--model m --allowed-tools "$3"'
YAML
}
READ_PERMS=$'    permissions:\n      contents: read\n      pull-requests: read'
WRITE_PERMS=$'    permissions:\n      contents: read\n      pull-requests: write'
READ_TOOLS='Bash(gh pr comment:*),Bash(gh pr diff:*),Bash(gh pr view:*)'
LABEL_TOOLS='Bash(gh pr comment:*),Bash(gh pr edit:*)'

# The #10600 shape: read scope, read-only allowlist, and an instruction to label the PR.
promptmut_workflow issue10600.yml "$READ_PERMS" "$READ_TOOLS" '- If a security hazard is added then add the label "do to not merge" to he pr'
expect "rejects a label instruction with neither the scope nor the tool (#10600)" 1 "asks the model to label" \
    "$PROMPTMUT" "$work/promptmut/issue10600.yml"
promptmut_workflow scope-only.yml "$WRITE_PERMS" "$READ_TOOLS" '- If a security hazard is added then add the label "do not merge".'
expect "rejects a label instruction whose allowlist has no labelling tool" 1 "a tool in --allowed-tools" \
    "$PROMPTMUT" "$work/promptmut/scope-only.yml"
promptmut_workflow tool-only.yml "$READ_PERMS" "$LABEL_TOOLS" '- If a security hazard is added then add the label "do not merge".'
expect "rejects a label instruction whose job holds only read scope" 1 "a \`write\` permission on pull-requests or issues" \
    "$PROMPTMUT" "$work/promptmut/tool-only.yml"
promptmut_workflow both.yml "$WRITE_PERMS" "$LABEL_TOOLS" '- If a security hazard is added then add the label "do not merge".'
expect "accepts a label instruction backed by both the scope and the tool" 0 "1 prompt(s)" \
    "$PROMPTMUT" "$work/promptmut/both.yml"
promptmut_workflow broad-tool.yml "$WRITE_PERMS" 'Bash(gh:*)' '- Then add a label to the pull request.'
expect "accepts a broader Bash pattern that covers the labelling command" 0 "" \
    "$PROMPTMUT" "$work/promptmut/broad-tool.yml"
promptmut_workflow negated.yml "$READ_PERMS" "$READ_TOOLS" '- Do not try to add a label yourself: a later step does that from your verdict.'
expect "does not read a prohibition as an instruction" 0 "" \
    "$PROMPTMUT" "$work/promptmut/negated.yml"
promptmut_workflow merge.yml "$WRITE_PERMS" "$READ_TOOLS" '- When everything is green, merge the PR.'
expect "rejects a merge instruction with no merge tool and no contents scope" 1 "asks the model to merge" \
    "$PROMPTMUT" "$work/promptmut/merge.yml"
promptmut_workflow close.yml "$READ_PERMS" "$READ_TOOLS" '- If it is spam, close this issue.'
expect "rejects a close instruction on a read-only job" 1 "asks the model to close" \
    "$PROMPTMUT" "$work/promptmut/close.yml"

# A job with no permissions block inherits the workflow's.
cat >"$work/promptmut/inherited.yml" <<'YAML'
name: Review
on: [ pull_request ]
permissions:
  pull-requests: write
jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          prompt: |
            Add the label "reviewed" to the PR.
          claude_args: '--allowed-tools "Bash(gh pr edit:*)"'
YAML
expect "falls back to the workflow's permissions when the job sets none" 0 "" \
    "$PROMPTMUT" "$work/promptmut/inherited.yml"

cat >"$work/promptmut/broken.yml" <<'YAML'
name: broken
on: [workflow_dispatch
jobs:
  build:
YAML
expect "reports a workflow it cannot parse as YAML" 1 "not parseable as YAML" \
    "$PROMPTMUT" "$work/promptmut/broken.yml"
expect "reports a workflow it cannot find" 1 "not found" \
    "$PROMPTMUT" "$work/promptmut/missing.yml"

# The repository's own workflows must pass - this is the case that would have caught #10600.
expect "the repository's own claude-code-action prompts are executable" 0 "prompts are executable" \
    "$PROMPTMUT"

# classify-pr.yml runs a model over an untrusted PR under pull_request_target, where secrets are
# present. Its containment is the job split: the `classify` job that runs the model holds no write
# scope and no write tool, reads files only (each tool pinned to a path, since a bare `Read` reaches
# /proc/self/environ and the runner's files), denies Bash, Edit, Write, NotebookEdit, WebFetch and
# WebSearch outright, and nothing in the workflow checks out the PR head.
# The `do not merge` flag of the deleted claude-code-review.yml now lives in pr-classify-apply.js
# and is pinned by test-pr-workflow-scripts.js.
echo
echo "classify-pr.yml containment"

# Prints one line per broken containment rule of a classify-pr workflow; nothing when it holds.
classify_pr_problems() {
    python3 -c '
import re, sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
jobs = doc.get("jobs") or {}
problems = []
classify = jobs.get("classify")
if not isinstance(classify, dict):
    problems.append("no classify job")
    classify = {}
perms = classify.get("permissions")
if not isinstance(perms, dict):
    problems.append("classify job sets no explicit permissions map")
else:
    for scope, level in perms.items():
        if level == "write":
            problems.append(f"classify job holds {scope}: write")
tools, denied = [], []
for step in classify.get("steps") or []:
    if "anthropics/claude-code-action" in str(step.get("uses", "")):
        args = str((step.get("with") or {}).get("claude_args", ""))
        for m in re.finditer(r"--allowed-?tools(?:=|\s+)(\"[^\"]*\"|\x27[^\x27]*\x27|\S+)", args):
            tools += [t.strip() for t in m.group(1).strip("\"\x27").split(",") if t.strip()]
        for m in re.finditer(r"--disallowed-?tools(?:=|\s+)(\"[^\"]*\"|\x27[^\x27]*\x27|\S+)", args):
            denied += [t.strip() for t in m.group(1).strip("\"\x27").split(",") if t.strip()]
# Deny rules beat allow rules from any settings source, so the allow list alone is not enough.
for name in ("Bash", "Edit", "Write", "NotebookEdit", "WebFetch", "WebSearch"):
    if name not in denied:
        problems.append(f"classify job does not deny {name} in --disallowed-tools")
for tool in denied:
    if tool.startswith("StructuredOutput"):
        problems.append(f"classify job denies the structured-output tool: {tool}")
if not tools:
    problems.append("classify job has no --allowed-tools list")
for tool in tools:
    if tool.startswith("Bash"):
        problems.append(f"classify job allows a shell command: {tool}")
    for banned in ("gh pr comment", "gh api", "gh pr edit", "git push"):
        if banned in tool:
            problems.append(f"classify job allows a write tool: {tool}")
    if tool in ("Read", "Grep", "Glob") or re.fullmatch(r"(Read|Grep|Glob)\(\s*\)", tool):
        problems.append(f"classify job allows {tool} on any path")
for job_id, job in jobs.items():
    for step in (job or {}).get("steps") or []:
        if "actions/checkout" not in str(step.get("uses", "")):
            continue
        w = step.get("with") or {}
        ref = str(w.get("ref", ""))
        if re.search(r"head\.sha|head\.ref|refs/pull", ref):
            problems.append(f"job {job_id} checks out the PR head ({ref})")
        if w.get("persist-credentials") is not False:
            problems.append(f"job {job_id} leaves the token in .git/config (persist-credentials)")
print("\n".join(problems))
' "$1"
}

CLASSIFYPR=".github/workflows/classify-pr.yml"
if [[ -f $CLASSIFYPR ]]; then
    pass "classify-pr.yml exists"
    problems="$(classify_pr_problems "$CLASSIFYPR")"
    if [[ -z $problems ]]; then
        pass "classify-pr.yml: the model job is read-only, reads pinned paths only, and no checkout takes the PR head"
    else
        fail "classify-pr.yml: the model job is read-only, reads pinned paths only, and no checkout takes the PR head" "$problems"
    fi
else
    fail "classify-pr.yml exists"
fi
if [[ ! -e .github/workflows/claude-code-review.yml ]]; then
    pass "claude-code-review.yml is gone (one automatic review per push)"
else
    fail "claude-code-review.yml is gone (one automatic review per push)"
fi

# The other direction: a classify job that broke every rule must be reported on each one.
cat >"$work/classify-pr-bad.yml" <<'YAML'
name: bad
on: [ pull_request_target ]
jobs:
  classify:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write
    steps:
      - uses: actions/checkout@v7
        with:
          ref: ${{ github.event.pull_request.head.sha }}
      - uses: anthropics/claude-code-action@v1
        with:
          prompt: Review.
          claude_args: '--allowed-tools "Read,Grep(./**),Bash(gh pr comment 5:*),Bash(gh api:*)" --disallowed-tools "Edit,Write,StructuredOutput"'
YAML
problems="$(classify_pr_problems "$work/classify-pr-bad.yml")"
for needle in "pull-requests: write" "Read on any path" "Bash(gh pr comment 5:*)" "write tool: Bash(gh api:*)" \
    "checks out the PR head" "persist-credentials" "does not deny Bash in" "does not deny NotebookEdit" \
    "does not deny WebFetch" "does not deny WebSearch" "denies the structured-output tool: StructuredOutput"; do
    if [[ $problems == *"$needle"* ]]; then
        pass "the classify-pr containment check reports: $needle"
    else
        fail "the classify-pr containment check reports: $needle" "$problems"
    fi
done
for needle in "does not deny Edit" "does not deny Write"; do
    if [[ $problems != *"$needle"* ]]; then
        pass "the classify-pr containment check accepts a listed deny: $needle"
    else
        fail "the classify-pr containment check accepts a listed deny: $needle" "$problems"
    fi
done

# #12492: the runner drops a job output whose value contains a string it has masked, and the text of
# a review can. The decision must travel from `classify` to `apply` as an artifact (the runner does
# not filter files or step outputs), never as a job output, and `apply` must still run when the
# artifact is missing so the PR gets a `verdict=none` marker that says why.
echo
echo "classify-pr.yml decision hand-off"

# Prints one line per broken hand-off rule of a classify-pr workflow; nothing when it holds.
classify_pr_handoff_problems() {
    python3 -c '
import re, sys, yaml
text = open(sys.argv[1]).read()
doc = yaml.safe_load(text)
jobs = doc.get("jobs") or {}
problems = []
classify = jobs.get("classify") or {}
apply_job = jobs.get("apply") or {}
# Named by the head the decision is for, so apply can never read the decision of another head:
# not one left by an earlier attempt, and not a rerun of apply alone, which a per-attempt name
# would send looking for an artifact its classify job never uploaded.
NAME = "classify-pr-decision-${{ needs.gate.outputs.head_sha }}"
FILE = "${{ runner.temp }}/classify-decision/decision.json"

def steps(job, action):
    return [s for s in (job.get("steps") or []) if action in str(s.get("uses", ""))]

def always(step):
    return "always()" in str(step.get("if", ""))

# No job output may carry the model answer, and nothing may read one from `needs`.
for name, value in (classify.get("outputs") or {}).items():
    if "structured_output" in str(value):
        problems.append(f"classify job exposes the decision as the job output {name}")
if re.search(r"needs\.classify\.outputs\.structured_output", text):
    problems.append("a job reads needs.classify.outputs.structured_output")
if "decision_saved" not in (classify.get("outputs") or {}):
    problems.append("classify job has no decision_saved output")

uploads = [s for s in steps(classify, "actions/upload-artifact") if (s.get("with") or {}).get("name") == NAME]
if not uploads:
    problems.append(f"classify job does not upload the {NAME} artifact")
for up in uploads:
    w = up.get("with") or {}
    if not always(up):
        problems.append("the decision upload does not run with if: always()")
    if str(w.get("path", "")) != FILE:
        problems.append(f"the decision upload path is not the single file {FILE}")
    days = w.get("retention-days")
    if not isinstance(days, int) or not 1 <= days <= 7:
        problems.append("the decision upload has no short retention-days (1 to 7)")
    if w.get("overwrite") is not True:
        problems.append("the decision upload does not set overwrite: true (a re-run would fail on the name)")

saves = [s for s in (classify.get("steps") or []) if s.get("id") == "save"]
if not saves:
    problems.append("classify job has no save step writing the decision file")
for save in saves:
    if not always(save):
        problems.append("the save step does not run with if: always()")
    if set((save.get("env") or {}).keys()) != {"DECISION"}:
        problems.append("the save step takes more than the decision in its environment")
    if "steps.classify.outputs.structured_output" not in str((save.get("env") or {}).get("DECISION", "")):
        problems.append("the save step does not read the model step output")

# The save step reads the model step output, so it must come after the model step.
ids = [s.get("id") for s in (classify.get("steps") or [])]
if "classify" in ids and "save" in ids and ids.index("save") < ids.index("classify"):
    problems.append("the save step runs before the model step")

if "classify" not in (apply_job.get("needs") or []):
    problems.append("apply does not need classify")
downloads = [s for s in steps(apply_job, "actions/download-artifact") if (s.get("with") or {}).get("name") == NAME]
if not downloads:
    problems.append(f"apply job does not download the {NAME} artifact")
for down in downloads:
    if down.get("continue-on-error") is not True:
        problems.append("the decision download does not set continue-on-error: true")
    if str((down.get("with") or {}).get("path", "")) != "${{ runner.temp }}/classify-decision":
        problems.append("the decision download path is not the directory of the uploaded file")

script_envs = [s.get("env") or {} for s in (apply_job.get("steps") or []) if "actions/github-script" in str(s.get("uses", ""))]
if not any(e.get("DECISION_FILE") == FILE for e in script_envs):
    problems.append(f"apply does not pass DECISION_FILE: {FILE}")
if not any("needs.classify.result" in str(e.get("CLASSIFY_RESULT", "")) for e in script_envs):
    problems.append("apply does not pass CLASSIFY_RESULT from needs.classify.result")
if not any("needs.classify.outputs.decision_saved" in str(e.get("DECISION_SAVED", "")) for e in script_envs):
    problems.append("apply does not pass DECISION_SAVED from needs.classify.outputs.decision_saved")
if any("STRUCTURED_OUTPUT" in e for e in script_envs):
    problems.append("apply still reads STRUCTURED_OUTPUT")

# apply must still run, and post a marker, when classify failed.
cond = str(apply_job.get("if", ""))
if "always()" not in cond or "!cancelled()" not in cond:
    problems.append("apply does not run when classify failed (if: always() && !cancelled())")
print("\n".join(problems))
' "$1"
}

if [[ -f $CLASSIFYPR ]]; then
    problems="$(classify_pr_handoff_problems "$CLASSIFYPR")"
    if [[ -z $problems ]]; then
        pass "classify-pr.yml: the decision goes from classify to apply as an artifact, not as a job output"
    else
        fail "classify-pr.yml: the decision goes from classify to apply as an artifact, not as a job output" "$problems"
    fi
fi

# The old shape: the decision as a job output, read through needs.
cat >"$work/classify-pr-joboutput.yml" <<'YAML'
name: joboutput
on: [ pull_request_target ]
jobs:
  classify:
    runs-on: ubuntu-latest
    outputs:
      structured_output: ${{ steps.classify.outputs.structured_output }}
    steps:
      - id: classify
        uses: anthropics/claude-code-action@v1
  apply:
    needs: [classify]
    if: always() && !cancelled()
    runs-on: ubuntu-latest
    steps:
      - uses: actions/github-script@v9
        env:
          STRUCTURED_OUTPUT: ${{ needs.classify.outputs.structured_output }}
YAML
problems="$(classify_pr_handoff_problems "$work/classify-pr-joboutput.yml")"
for needle in "exposes the decision as the job output structured_output" \
    "reads needs.classify.outputs.structured_output" "no decision_saved output" \
    "does not upload the classify-pr-decision-\${{ needs.gate.outputs.head_sha }} artifact" "has no save step" \
    "does not download the classify-pr-decision-\${{ needs.gate.outputs.head_sha }} artifact" "does not pass DECISION_FILE" \
    "does not pass CLASSIFY_RESULT" "does not pass DECISION_SAVED" "still reads STRUCTURED_OUTPUT"; do
    if [[ $problems == *"$needle"* ]]; then
        pass "the classify-pr hand-off check reports: $needle"
    else
        fail "the classify-pr hand-off check reports: $needle" "$problems"
    fi
done

# An artifact hand-off with every knob wrong: uploaded only on success, wide path, long retention,
# no overwrite, a save step that sees more than the decision, and an apply that stops on a failed
# classify or on a missing artifact.
cat >"$work/classify-pr-weak.yml" <<'YAML'
name: weak
on: [ pull_request_target ]
jobs:
  classify:
    runs-on: ubuntu-latest
    outputs:
      decision_saved: ${{ steps.save.outputs.saved }}
    steps:
      - id: save
        env:
          DECISION: ${{ steps.classify.outputs.structured_output }}
          TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: echo saved=true >> "$GITHUB_OUTPUT"
      - id: classify
        uses: anthropics/claude-code-action@v1
      - uses: actions/upload-artifact@v7
        with:
          name: classify-pr-decision-${{ needs.gate.outputs.head_sha }}
          path: ${{ github.workspace }}
          retention-days: 90
  apply:
    needs: [classify]
    if: success()
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: classify-pr-decision-${{ needs.gate.outputs.head_sha }}
          path: ${{ runner.temp }}/elsewhere
      - uses: actions/github-script@v9
        env:
          DECISION_FILE: ${{ runner.temp }}/classify-decision/decision.json
          CLASSIFY_RESULT: ${{ needs.classify.result }}
          DECISION_SAVED: ${{ needs.classify.outputs.decision_saved }}
YAML
problems="$(classify_pr_handoff_problems "$work/classify-pr-weak.yml")"
for needle in "the decision upload does not run with if: always()" \
    "the decision upload path is not the single file" "no short retention-days" \
    "does not set overwrite: true" "the save step does not run with if: always()" \
    "the save step takes more than the decision in its environment" "the save step runs before the model step" \
    "does not set continue-on-error: true" "download path is not the directory of the uploaded file" \
    "apply does not run when classify failed"; do
    if [[ $problems == *"$needle"* ]]; then
        pass "the classify-pr hand-off check reports: $needle"
    else
        fail "the classify-pr hand-off check reports: $needle" "$problems"
    fi
done

# An artifact name shared by every head and attempt of the run: a decision left by an earlier
# attempt, for another head, could be downloaded and posted under the current head's marker; and
# a per-attempt name would make a rerun of apply alone look for an artifact classify never uploaded.
for fixed in "classify-pr-decision" 'classify-pr-decision-${{ github.run_attempt }}'; do
    cat >"$work/classify-pr-fixed-name.yml" <<YAML
name: fixed
on: [ pull_request_target ]
jobs:
  classify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v7
        if: always()
        with:
          name: $fixed
          path: \${{ runner.temp }}/classify-decision/decision.json
  apply:
    needs: [classify]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: $fixed
YAML
    problems="$(classify_pr_handoff_problems "$work/classify-pr-fixed-name.yml")"
    for needle in "does not upload the classify-pr-decision-\${{ needs.gate.outputs.head_sha }} artifact" \
        "does not download the classify-pr-decision-\${{ needs.gate.outputs.head_sha }} artifact"; do
        if [[ $problems == *"$needle"* ]]; then
            pass "the classify-pr hand-off check reports a name of '$fixed': $needle"
        else
            fail "the classify-pr hand-off check reports a name of '$fixed': $needle" "$problems"
        fi
    done
done

# The linux/amd64 -march=compatibility leg (#11253) shares os_name and arch with the default
# linux/amd64 leg, so it must carry a `suffix` that keeps its workflow artifact and its release
# archive apart: without it both upload `arcadedb-native-linux-amd64` and the `docker` job would
# take whichever finished last, publishing the slow compat binary as the default image.
NATIVEWF=".github/workflows/native-image.yml"
if grep -qF '"suffix":"-compat","march":"compatibility"' "$NATIVEWF" \
    && grep -qF 'name: arcadedb-native-${{ matrix.os_name }}-${{ matrix.arch }}${{ matrix.suffix }}' "$NATIVEWF" \
    && grep -qF 'ARCHIVE="$BIN${{ matrix.suffix }}.tar.gz"' "$NATIVEWF"; then
    pass "the native amd64 compatibility leg names its artifact and archive apart from the default leg"
else
    fail "the native amd64 compatibility leg names its artifact and archive apart from the default leg"
fi

# #11001: a `*Test` class outside the engine tagged slow/vector ran in no lane. The engine-only tag lanes never
# build it and the unit-tests shards exclude the tag, so the tag must have a lane for the module or must not be there.
echo
echo "tag lanes cover every module"
# Captured first: `git grep | grep -q` under pipefail reports SIGPIPE as "no match".
vector_outside=$(git grep -lE '@Tag\("vector"\)' -- ':!engine' '*.java' || true)
slow_outside=$(git grep -lE '@Tag\("slow"\)' -- ':!engine' '*Test.java' || true)
if [[ -n $vector_outside ]]; then
    fail "no test outside the engine carries @Tag(\"vector\") (the vector lane builds only the engine)" "$vector_outside"
else
    pass "no test outside the engine carries @Tag(\"vector\") (the vector lane builds only the engine)"
fi
if [[ -z $slow_outside ]]; then
    pass "no slow *Test class outside the engine needs a lane"
elif grep -E 'mvnw .*-pl .!engine[^ ]*. .*-Dgroups=slow' .github/workflows/mvn-test.yml | grep -qF -- '-Dsurefire.includes=**/*Test.java'; then
    pass "slow *Test classes outside the engine have a lane in mvn-test.yml"
else
    fail "slow *Test classes outside the engine have a lane in mvn-test.yml" "$slow_outside"
fi

# #9850: two PRs that are each green against a base lacking the other can merge into a red main.
# The in-repository half of the fix is a `merge_group` trigger, so a GitHub merge queue tests the
# merge result; without it the queue's checks never start and the queue waits forever. The
# concurrency group must also keep merge-queue entries apart (keyed by SHA, never cancelled).
if grep -qE '^  merge_group:' .github/workflows/mvn-test.yml; then
    pass "mvn-test.yml runs on merge_group so a merge queue tests the merge result (#9850)"
else
    fail "mvn-test.yml runs on merge_group so a merge queue tests the merge result (#9850)" "no merge_group trigger"
fi
if grep -qF "cancel-in-progress: \${{ github.event_name == 'pull_request' }}" .github/workflows/mvn-test.yml; then
    pass "merge_group runs are never cancelled in progress (#9850)"
else
    fail "merge_group runs are never cancelled in progress (#9850)" "cancel-in-progress is not limited to pull_request"
fi

# The release SBOMs must carry the fields BSI TR-03183-2 and the NTIA minimum elements ask for. A
# generator upgrade or a pom change that silently drops hashes, licenses or the supplier would
# otherwise ship an SBOM that consumers reject; each fixture breaks exactly one field.
echo
echo "check-sbom-fields.py"

SBOMFIX="$SCRIPTS/tests/fixtures/sbom"
expect "a complete CycloneDX 1.6 SBOM passes" 0 "" \
    "$SBOMFIELDS" "$SBOMFIX/good.cdx.json"
expect "a component without hashes fails" 1 "missing hashes" \
    "$SBOMFIELDS" "$SBOMFIX/missing-hash.cdx.json"
expect "a SBOM without a supplier fails" 1 "missing supplier" \
    "$SBOMFIELDS" "$SBOMFIX/missing-supplier.cdx.json"
expect "a component without licenses fails" 1 "missing licenses" \
    "$SBOMFIELDS" "$SBOMFIX/missing-license.cdx.json"
expect "one bad file among good ones fails the run" 1 "missing-hash.cdx.json" \
    "$SBOMFIELDS" "$SBOMFIX/good.cdx.json" "$SBOMFIX/missing-hash.cdx.json"

# Variants of the good fixture, generated so each breaks one thing.
python3 - "$SBOMFIX/good.cdx.json" "$work" <<'PY'
import copy, json, sys
good = json.load(open(sys.argv[1]))
out = sys.argv[2]
def write(name, mutate):
    bom = copy.deepcopy(good)
    mutate(bom)
    json.dump(bom, open(f"{out}/sbom-{name}.json", "w"))
write("spec14", lambda b: b.update(specVersion="1.4"))
write("spec110", lambda b: b.update(specVersion="1.10"))
write("no-timestamp", lambda b: b["metadata"].pop("timestamp"))
write("no-deps", lambda b: b.update(dependencies=[]))
write("no-tool", lambda b: b["metadata"].pop("tools"))
write("empty-license", lambda b: b["components"][1].update(licenses=[{}]))
write("nested-no-hash", lambda b: b["components"][0].update(components=[{"type": "library", "name": "x", "version": "1", "purl": "pkg:maven/x/x@1", "hashes": [], "licenses": [{"expression": "MIT"}]}]))
write("supplier-string", lambda b: b["metadata"].update(supplier="Arcade Data Ltd"))
write("metadata-list", lambda b: b.update(metadata=[]))
write("hashes-string", lambda b: b["components"][0].update(hashes="abc"))
write("components-dict", lambda b: b.update(components={}))
write("component-string", lambda b: b.update(components=["x"]))
def only_publisher(name):
    def m(b):
        b["metadata"].pop("supplier", None)
        b["metadata"].pop("manufacturer", None)
        b["metadata"].setdefault("component", {})["publisher"] = name
    return m
write("publisher-only", only_publisher("Arcade Data Ltd"))
write("publisher-other", only_publisher("Someone Else"))
write("tools-components-int", lambda b: b["metadata"].update(tools={"components": 1}))
write("tools-int", lambda b: b["metadata"].update(tools=1))
write("deps-null", lambda b: b.update(dependencies=[None]))
write("deps-no-ref", lambda b: b.update(dependencies=[{"dependsOn": []}]))
write("deps-bad-dependson", lambda b: b.update(dependencies=[{"ref": "x", "dependsOn": [1]}]))
write("deps-leaf", lambda b: b.update(dependencies=[{"ref": "x"}]))
write("hash-short", lambda b: [c.update(hashes=[{"alg": "SHA-512", "content": "a"}]) for c in b["components"]])
write("hash-256-as-512", lambda b: [c.update(hashes=[{"alg": "SHA-512", "content": "a" * 64}]) for c in b["components"]])
write("hash-256", lambda b: [c.update(hashes=[{"alg": "SHA-256", "content": "ab" * 32}]) for c in b["components"]])
def other_supplier_arcade_manufacturer(b):
    b["metadata"]["supplier"] = {"name": "Third Party"}
    b["metadata"]["manufacturer"] = {"name": "Arcade Data Ltd"}
    b["metadata"].setdefault("component", {}).pop("supplier", None)
    b["metadata"]["component"].pop("publisher", None)
write("supplier-other-manufacturer-arcade", other_supplier_arcade_manufacturer)
PY
echo '{ not json' >"$work/sbom-broken.json"
expect "specVersion 1.4 fails" 1 "missing specVersion" "$SBOMFIELDS" "$work/sbom-spec14.json"
expect "specVersion 1.10 passes (numeric, not string, comparison)" 0 "" "$SBOMFIELDS" "$work/sbom-spec110.json"
expect "a missing timestamp fails" 1 "missing timestamp" "$SBOMFIELDS" "$work/sbom-no-timestamp.json"
expect "empty dependencies fail" 1 "missing dependencies" "$SBOMFIELDS" "$work/sbom-no-deps.json"
expect "no author and no tool fails" 1 "missing author/tool" "$SBOMFIELDS" "$work/sbom-no-tool.json"
expect "malformed JSON fails" 1 "unreadable" "$SBOMFIELDS" "$work/sbom-broken.json"
expect "licenses [{}] fails" 1 "missing licenses" "$SBOMFIELDS" "$work/sbom-empty-license.json"
expect "a nested component without a hash fails" 1 "missing hashes" "$SBOMFIELDS" "$work/sbom-nested-no-hash.json"
expect "supplier as a string is malformed, not a traceback" 1 "malformed metadata.supplier" "$SBOMFIELDS" "$work/sbom-supplier-string.json"
expect "metadata as a list is malformed" 1 "malformed metadata" "$SBOMFIELDS" "$work/sbom-metadata-list.json"
expect "hashes as a string is malformed" 1 "malformed components[0].hashes" "$SBOMFIELDS" "$work/sbom-hashes-string.json"
expect "metadata.component.publisher alone satisfies the supplier" 0 "" "$SBOMFIELDS" "$work/sbom-publisher-only.json"
expect "a publisher that is not Arcade Data Ltd fails" 1 "missing supplier Arcade Data Ltd" "$SBOMFIELDS" "$work/sbom-publisher-other.json"
expect "--module allows a missing timestamp" 0 "" "$SBOMFIELDS" --module "$work/sbom-no-timestamp.json"
expect "--module still checks the other fields" 1 "missing dependencies" "$SBOMFIELDS" --module "$work/sbom-no-deps.json"
expect "components as a dict is malformed" 1 "malformed components" "$SBOMFIELDS" "$work/sbom-components-dict.json"
expect "a component that is not an object is malformed" 1 "malformed components[0]" "$SBOMFIELDS" "$work/sbom-component-string.json"
expect "no argument exits 2" 2 "Usage" "$SBOMFIELDS"
expect "metadata.tools.components that is not an array is malformed, not a traceback" 1 "malformed metadata.tools.components" \
    "$SBOMFIELDS" "$work/sbom-tools-components-int.json"
expect "a malformed tool collection does not stop the BOMs after it" 1 "$work/sbom-publisher-other.json: metadata: missing supplier" \
    "$SBOMFIELDS" "$work/sbom-tools-components-int.json" "$work/sbom-publisher-other.json"
expect "metadata.tools that is neither an array nor an object is malformed" 1 "malformed metadata.tools" "$SBOMFIELDS" "$work/sbom-tools-int.json"
expect "a dependency record that is not an object is malformed" 1 "malformed dependencies[0]" "$SBOMFIELDS" "$work/sbom-deps-null.json"
expect "a dependency record without a ref is malformed" 1 "malformed dependencies[0].ref" "$SBOMFIELDS" "$work/sbom-deps-no-ref.json"
expect "a dependsOn that is not an array of strings is malformed" 1 "malformed dependencies[0].dependsOn" "$SBOMFIELDS" "$work/sbom-deps-bad-dependson.json"
expect "a leaf dependency record without dependsOn passes" 0 "" "$SBOMFIELDS" "$work/sbom-deps-leaf.json"
expect "a hash whose content is not a digest of its algorithm fails" 1 "missing hashes" "$SBOMFIELDS" "$work/sbom-hash-short.json"
expect "a SHA-256-long digest declared as SHA-512 fails" 1 "missing hashes" "$SBOMFIELDS" "$work/sbom-hash-256-as-512.json"
expect "a 64-hex-digit SHA-256 passes" 0 "" "$SBOMFIELDS" "$work/sbom-hash-256.json"
expect "a matching manufacturer satisfies the supplier although metadata.supplier names someone else" 0 "" \
    "$SBOMFIELDS" "$work/sbom-supplier-other-manufacturer-arcade.json"

# The distribution SBOMs are built from the package module's BOM: filtered to the jars one assembly
# puts under lib/, with Studio's npm tree nested under an `arcadedb-studio` application component,
# and stamped with the timestamp and supplier a release-level SBOM needs. A merge that drops the npm
# tree, lists a jar the archive does not ship or misses one it does would publish an SBOM that
# describes some other distribution.
echo
echo "merge-sbom.py"

MERGE="$SCRIPTS/merge-sbom.py"
MERGEJVM="$SBOMFIX/merge-jvm.cdx.json"
MERGESTUDIO="$SBOMFIX/merge-studio.cdx.json"
RELEASE_ARGS=(--supplier "Arcade Data Ltd" --supplier-url https://arcadedb.com --timestamp)

# Asserts a property of a merged SBOM with a Python expression over `bom` (or a block that sets `ok`);
# prints the BOM when false.
bom_expect() {
    local description="$1" file="$2" expression="$3"
    local output status=0
    output="$(python3 -c '
import json, sys
bom = json.load(open(sys.argv[1]))
def comps(cs):
    for c in cs or []:
        yield c
        yield from comps(c.get("components"))
refs = [c.get("bom-ref") for c in comps(bom.get("components"))]
top = {c["name"]: c for c in bom.get("components") or [] if c.get("type") == "library"}
apps = [c for c in bom.get("components") or [] if c.get("type") == "application"]
deps = {d["ref"]: d.get("dependsOn") or [] for d in bom.get("dependencies") or []}
try:
    ok = eval(compile(sys.argv[2], "<assertion>", "eval"))
except SyntaxError:  # a statement block that sets ok
    scope = dict(globals())
    exec(sys.argv[2], scope)
    ok = scope["ok"]
sys.exit(0 if ok else 1)
' "$file" "$expression" 2>&1)" || status=$?
    if [[ $status -eq 0 ]]; then
        pass "$description"
    else
        fail "$description" "$output$(echo; head -c 4000 "$file")"
    fi
}

merged="$work/merge-full.cdx.json"
expect "merges the Studio npm tree into the JVM BOM" 0 "" \
    "$MERGE" --root "$MERGEJVM" --nested "$MERGESTUDIO" --name arcadedb-studio "${RELEASE_ARGS[@]}" --out "$merged"
bom_expect "Studio's components are children of an arcadedb-studio application" "$merged" \
    '[a["name"] for a in apps] == ["arcadedb-studio"] and sorted(c["name"] for c in apps[0]["components"]) == ["bootstrap", "jquery"]'
bom_expect "the Studio application takes its version from the root BOM" "$merged" \
    '[a["version"] for a in apps] == ["26.12.1"]'
bom_expect "no bom-ref appears twice" "$merged" 'len(refs) == len(set(refs))'
bom_expect "Studio's dependency graph hangs off the application component" "$merged" \
    'sorted(deps.get("pkg:npm/arcadedb-studio@26.12.1", [])) == ["bootstrap@5.3.9", "jquery@4.1.0"]'
bom_expect "the Studio jar depends on the application it bundles" "$merged" \
    '"pkg:npm/arcadedb-studio@26.12.1" in deps["pkg:maven/com.arcadedb/arcadedb-studio@26.12.1?type=jar"]'
bom_expect "the root metadata component is kept" "$merged" 'bom["metadata"]["component"]["name"] == "arcadedb-package"'
bom_expect "the supplier is Arcade Data Ltd with its URL" "$merged" \
    'bom["metadata"]["supplier"] == {"name": "Arcade Data Ltd", "url": ["https://arcadedb.com"]}'
bom_expect "a derived BOM gets its own serial number" "$merged" \
    'bom["serialNumber"].startswith("urn:uuid:") and bom["serialNumber"] != "urn:uuid:11111111-1111-4111-8111-111111111111"'
expect "the merged BOM passes the strict release check" 0 "" "$SBOMFIELDS" "$merged"
bom_expect "the npm BOM's tools join the root's" "$merged" \
    'sorted(t["name"] for t in bom["metadata"]["tools"]["components"]) == ["cyclonedx-maven-plugin", "cyclonedx-npm"]'

SOURCE_DATE_EPOCH=1764151200 "$MERGE" --root "$MERGEJVM" --nested "$MERGESTUDIO" --name arcadedb-studio \
    "${RELEASE_ARGS[@]}" --out "$work/merge-epoch.cdx.json" || true
bom_expect "--timestamp honours SOURCE_DATE_EPOCH" "$work/merge-epoch.cdx.json" \
    'bom["metadata"]["timestamp"] == "2025-11-26T10:00:00Z"'
SOURCE_DATE_EPOCH=1764151200 "$MERGE" --root "$MERGEJVM" --nested "$MERGESTUDIO" --name arcadedb-studio \
    "${RELEASE_ARGS[@]}" --out "$work/merge-epoch2.cdx.json" || true
if cmp -s "$work/merge-epoch.cdx.json" "$work/merge-epoch2.cdx.json"; then
    pass "a merge under SOURCE_DATE_EPOCH is reproducible"
else
    fail "a merge under SOURCE_DATE_EPOCH is reproducible" "$(diff "$work/merge-epoch.cdx.json" "$work/merge-epoch2.cdx.json")"
fi

"$MERGE" --root "$MERGEJVM" --nested "$MERGESTUDIO" --name arcadedb-studio --out "$work/merge-plain.cdx.json" || true
expect "without --timestamp the root's missing timestamp stays missing" 1 "missing timestamp" \
    "$SBOMFIELDS" "$work/merge-plain.cdx.json"

# The studio BOM repeating a bom-ref the root already has must not produce a second component.
python3 - "$MERGESTUDIO" "$work/merge-studio-dup.cdx.json" <<'PY'
import json, sys
bom = json.load(open(sys.argv[1]))
bom["components"].append(dict(bom["components"][0]))
json.dump(bom, open(sys.argv[2], "w"))
PY
"$MERGE" --root "$MERGEJVM" --nested "$work/merge-studio-dup.cdx.json" --name arcadedb-studio "${RELEASE_ARGS[@]}" \
    --out "$work/merge-dedup.cdx.json" || true
bom_expect "a repeated bom-ref is de-duplicated" "$work/merge-dedup.cdx.json" 'len(refs) == len(set(refs))'

# --lib-dir: the BOM lists exactly the jars an assembly ships, matched by file name and hash.
mkdir -p "$work/lib-full" "$work/lib-base"
printf 'engine-jar\n' >"$work/lib-full/arcadedb-engine-26.12.1.jar"
printf 'studio-jar\n' >"$work/lib-full/arcadedb-studio-26.12.1.jar"
printf 'gremlin-jar\n' >"$work/lib-full/arcadedb-gremlin-26.12.1-shaded.jar"
printf 'engine-jar\n' >"$work/lib-base/arcadedb-engine-26.12.1.jar"
expect "filters to the full assembly's lib/" 0 "" \
    "$MERGE" --root "$MERGEJVM" --nested "$MERGESTUDIO" --name arcadedb-studio --lib-dir "$work/lib-full" \
    "${RELEASE_ARGS[@]}" --out "$work/merge-lib-full.cdx.json"
bom_expect "the full BOM keeps every shipped jar and nests Studio" "$work/merge-lib-full.cdx.json" \
    'sorted(top) == ["arcadedb-engine", "arcadedb-gremlin", "arcadedb-studio"] and [a["name"] for a in apps] == ["arcadedb-studio"]'
expect "filters to the base assembly's lib/" 0 "" \
    "$MERGE" --root "$MERGEJVM" --nested "$MERGESTUDIO" --name arcadedb-studio --lib-dir "$work/lib-base" \
    "${RELEASE_ARGS[@]}" --out "$work/merge-lib-base.cdx.json"
bom_expect "the base BOM drops unshipped jars and does not nest Studio" "$work/merge-lib-base.cdx.json" \
    '[c["name"] for c in bom["components"]] == ["arcadedb-engine"]'
bom_expect "the base BOM's dependencies name only kept components" "$work/merge-lib-base.cdx.json" \
    'set(deps) | {r for v in deps.values() for r in v} <= set(refs) | {bom["metadata"]["component"]["bom-ref"]}'
expect "the filtered base BOM passes the strict release check" 0 "" "$SBOMFIELDS" "$work/merge-lib-base.cdx.json"

mkdir -p "$work/lib-extra"
cp "$work/lib-base/"*.jar "$work/lib-extra/"
printf 'x\n' >"$work/lib-extra/stranger-1.0.jar"
expect "a shipped jar missing from the BOM fails" 1 "stranger-1.0.jar" \
    "$MERGE" --root "$MERGEJVM" --lib-dir "$work/lib-extra" --out "$work/merge-extra.cdx.json"
mkdir -p "$work/lib-stale"
printf 'not the engine\n' >"$work/lib-stale/arcadedb-engine-26.12.1.jar"
expect "a shipped jar whose hash differs from the BOM fails" 1 "hash" \
    "$MERGE" --root "$MERGEJVM" --lib-dir "$work/lib-stale" --out "$work/merge-stale.cdx.json"
mkdir -p "$work/lib-empty"
expect "an empty lib dir fails rather than writing an empty BOM" 1 "lib-empty: no jar" \
    "$MERGE" --root "$MERGEJVM" --lib-dir "$work/lib-empty" --out "$work/merge-empty.cdx.json"
expect "a missing lib dir fails too" 1 "no jar" \
    "$MERGE" --root "$MERGEJVM" --lib-dir "$work/lib-empty-missing" --out "$work/merge-empty.cdx.json"

# Orphans are found by reachability from the root, not by "has no incoming edge": with root -> studio -> engine ->
# gremlin -> engine and the studio jar not shipped, engine and gremlin both keep an incoming edge yet neither is
# reachable. The first of them in document order gets the root edge, and the other stays reachable through it.
python3 - "$MERGEJVM" "$work/merge-jvm-cycle.json" <<'PY'
import json, sys
bom = json.load(open(sys.argv[1]))
ref = {c["name"]: c["bom-ref"] for c in bom["components"]}
root = bom["metadata"]["component"]["bom-ref"]
bom["dependencies"] = [
    {"ref": root, "dependsOn": [ref["arcadedb-studio"]]},
    {"ref": ref["arcadedb-studio"], "dependsOn": [ref["arcadedb-engine"]]},
    {"ref": ref["arcadedb-engine"], "dependsOn": [ref["arcadedb-gremlin"]]},
    {"ref": ref["arcadedb-gremlin"], "dependsOn": [ref["arcadedb-engine"]]},
]
json.dump(bom, open(sys.argv[2], "w"))
PY
mkdir -p "$work/lib-cycle"
cp "$work/lib-full/arcadedb-engine-26.12.1.jar" "$work/lib-full/arcadedb-gremlin-26.12.1-shaded.jar" "$work/lib-cycle/"
"$MERGE" --root "$work/merge-jvm-cycle.json" --lib-dir "$work/lib-cycle" "${RELEASE_ARGS[@]}" \
    --out "$work/merge-cycle.cdx.json" || true
bom_expect "a retained cycle cut off from the root by a filtered jar is re-attached to the root" "$work/merge-cycle.cdx.json" '
root = bom["metadata"]["component"]["bom-ref"]
seen, todo = set(), [root]
while todo:
    r = todo.pop()
    if r not in seen:
        seen.add(r)
        todo.extend(deps.get(r, []))
ok = set(refs) <= seen and deps[root] == [top["arcadedb-engine"]["bom-ref"]]'

# The hash check must not fail open: a component with no usable hash cannot vouch for the jar shipped
# under its name, and a SHA-512-only component (what a newer generator may write) must still be checked.
python3 - "$MERGEJVM" "$work" <<'PY'
import hashlib, json, sys
bom = json.load(open(sys.argv[1]))
engine = bom["components"][0]
assert engine["name"] == "arcadedb-engine"
engine["hashes"] = []
json.dump(bom, open(f"{sys.argv[2]}/merge-jvm-hashless.json", "w"))
engine["hashes"] = [{"alg": "SHA-512", "content": hashlib.sha512(b"engine-jar\n").hexdigest()}]
json.dump(bom, open(f"{sys.argv[2]}/merge-jvm-sha512.json", "w"))
engine["hashes"] = [{"alg": "SHA-512", "content": hashlib.sha512(b"something else\n").hexdigest()},
                    {"alg": "SHA-256", "content": hashlib.sha256(b"engine-jar\n").hexdigest()}]
json.dump(bom, open(f"{sys.argv[2]}/merge-jvm-sha512-wrong.json", "w"))
PY
expect "a shipped jar whose component has no SHA-256/SHA-512 fails" 1 "no SHA-256 or SHA-512" \
    "$MERGE" --root "$work/merge-jvm-hashless.json" --lib-dir "$work/lib-base" --out "$work/merge-hashless.cdx.json"
expect "a SHA-512-only component is checked against the jar and passes when it matches" 0 "" \
    "$MERGE" --root "$work/merge-jvm-sha512.json" --lib-dir "$work/lib-base" --out "$work/merge-sha512.cdx.json"
expect "SHA-512 is preferred: a wrong SHA-512 fails even next to a matching SHA-256" 1 "hash" \
    "$MERGE" --root "$work/merge-jvm-sha512-wrong.json" --lib-dir "$work/lib-base" --out "$work/merge-sha512-wrong.cdx.json"
expect "merge-sbom.py without --root exits 2" 2 "usage" "$MERGE" --out "$work/x.json"

# A shaded jar is one file in lib/ but carries third-party code (TinkerPop, Groovy, Netty...). Unless
# that code is nested under the jar's component, a scanner sees arcadedb-gremlin-shaded.jar and none of
# the libraries a CVE would name. The source is the module's own SBOM, minus what the module's
# shade artifactSet leaves out.
mkdir -p "$work/modules/gremlin/target"
cp "$SBOMFIX/shade-gremlin.cdx.json" "$work/modules/gremlin/target/arcadedb-gremlin-26.12.1-cyclonedx.json"
cp "$SBOMFIX/shade-gremlin-pom.xml" "$work/modules/gremlin/pom.xml"
expect "nests a shaded jar's contents from its module SBOM" 0 "" \
    "$MERGE" --root "$MERGEJVM" --module-boms "$work/modules/*/target/*-cyclonedx.json" "${RELEASE_ARGS[@]}" \
    --out "$work/merge-shaded.cdx.json"
bom_expect "the shaded jar carries what its module shades in, minus the artifactSet excludes" "$work/merge-shaded.cdx.json" \
    'sorted(c["name"] for c in top["arcadedb-gremlin"].get("components", [])) == ["gremlin-core", "groovy"]'
bom_expect "jars that are not shaded get no nested components" "$work/merge-shaded.cdx.json" \
    'not top["arcadedb-engine"].get("components") and not top["arcadedb-studio"].get("components")'
bom_expect "nested shaded components keep their purl and get a bom-ref of their own" "$work/merge-shaded.cdx.json" \
    'len(refs) == len(set(refs)) and [c["purl"] for c in top["arcadedb-gremlin"]["components"] if c["name"] == "groovy"] == ["pkg:maven/org.apache.groovy/groovy@4.0.27?type=jar"]'
bom_expect "the shaded jar depends on its direct shaded-in dependency, which keeps its own edges" "$work/merge-shaded.cdx.json" \
    'any("gremlin-core" in r for r in deps[top["arcadedb-gremlin"]["bom-ref"]]) and any("groovy" in r for r in deps[next(c["bom-ref"] for c in top["arcadedb-gremlin"]["components"] if c["name"] == "gremlin-core")])'
expect "the shaded-nested BOM passes the strict release check" 0 "" "$SBOMFIELDS" "$work/merge-shaded.cdx.json"
expect "a shaded jar whose module SBOM is missing fails" 1 "arcadedb-gremlin" \
    "$MERGE" --root "$MERGEJVM" --module-boms "$work/modules/*/target/nothing-*.json" --out "$work/merge-noshade.cdx.json"

# maven-shade reads a 3-token pattern as groupId:artifactId:classifier (any type), not ...:type, so
# `org.apache.groovy:groovy:indy` must exclude the indy-classified groovy jar. And a shaded-in library
# reached only through an excluded artifact (jline via the engine here) must stay attached to the jar
# rather than float in the graph with no incoming edge.
mkdir -p "$work/modules3/gremlin/target"
python3 - "$SBOMFIX/shade-gremlin.cdx.json" "$SBOMFIX/shade-gremlin-pom.xml" "$work/modules3/gremlin" <<'PY'
import json, sys
bom = json.load(open(sys.argv[1]))
groovy = next(c for c in bom["components"] if c["name"] == "groovy")
indy = "pkg:maven/org.apache.groovy/groovy@4.0.27?classifier=indy&type=jar"
old = groovy["bom-ref"]
groovy["bom-ref"] = groovy["purl"] = indy
engine = next(c for c in bom["components"] if c["name"] == "arcadedb-engine")
jline = dict(engine, **{"group": "org.jline", "name": "jline", "version": "3.30.0",
                         "bom-ref": "pkg:maven/org.jline/jline@3.30.0?type=jar",
                         "purl": "pkg:maven/org.jline/jline@3.30.0?type=jar"})
bom["components"].append(jline)
for d in bom["dependencies"]:
    d["dependsOn"] = [indy if r == old else r for r in d["dependsOn"]]
    if d["ref"] == engine["bom-ref"]:
        d["dependsOn"].append(jline["bom-ref"])
bom["dependencies"].append({"ref": jline["bom-ref"], "dependsOn": []})
json.dump(bom, open(f"{sys.argv[3]}/target/arcadedb-gremlin-26.12.1-cyclonedx.json", "w"))
pom = open(sys.argv[2]).read().replace("<exclude>org.apache.lucene:*</exclude>",
                                       "<exclude>org.apache.groovy:groovy:indy</exclude>")
open(f"{sys.argv[3]}/pom.xml", "w").write(pom)
PY
"$MERGE" --root "$MERGEJVM" --module-boms "$work/modules3/*/target/*-cyclonedx.json" --out "$work/merge-shade3.cdx.json" || true
bom_expect "a 3-token shade exclude is groupId:artifactId:classifier" "$work/merge-shade3.cdx.json" \
    'sorted(c["name"] for c in top["arcadedb-gremlin"].get("components", [])) == ["gremlin-core", "jline"]'
bom_expect "a shaded-in library orphaned by an excluded artifact hangs off the shaded jar" "$work/merge-shade3.cdx.json" \
    'any(r.endswith("org.jline/jline@3.30.0?type=jar") for r in deps[top["arcadedb-gremlin"]["bom-ref"]])'

# A jar can carry other artifacts no module SBOM names: ratis-thirdparty inside the ha-raft uber-jar
# embeds Netty, and a Critical there was visible to an image scan but not to a scan of our SBOM. Every
# META-INF/maven/<g>/<a>/pom.properties inside a shipped jar that is not the jar itself and not already
# in the document becomes a nested component of that jar, licensed from the local Maven repository.
mkdir -p "$work/lib-emb" "$work/m2"
python3 - "$MERGEJVM" "$work" <<'PY'
import hashlib, json, sys, zipfile
from pathlib import Path
work = Path(sys.argv[2])
def jar(name, coordinates):
    path = work / "lib-emb" / name
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("org/example/Some.class", b"\xca\xfe\xba\xbe")
        for g, a, v in coordinates:
            z.writestr(f"META-INF/maven/{g}/{a}/pom.properties",
                       f"#Generated by Maven\nartifactId={a}\ngroupId={g}\nversion={v}\n")
    return hashlib.sha512(path.read_bytes()).hexdigest()
digests = {
    "arcadedb-engine": jar("arcadedb-engine-26.12.1.jar", [("com.arcadedb", "arcadedb-engine", "26.12.1")]),
    "arcadedb-gremlin": jar("arcadedb-gremlin-26.12.1-shaded.jar", [
        ("com.arcadedb", "arcadedb-gremlin", "26.12.1"),          # the jar itself
        ("com.arcadedb", "arcadedb-engine", "26.12.1"),           # already a top-level component
        ("org.apache.tinkerpop", "gremlin-core", "3.7.4"),        # already nested from the module SBOM
        ("io.netty", "netty-handler", "4.1.136.Final"),           # new, no pom in the repo
        ("org.example", "licensed", "1.0"),                       # new, license through its parent pom
        ("org.example", "custom", "1.0"),                         # license name with no SPDX id
        ("org.example", "byurl", "1.0"),                          # unknown name, MIT url
        ("org.example", "conflict", "1.0")]),                     # MIT name, Apache url: ambiguous
    "arcadedb-studio": jar("arcadedb-studio-26.12.1.jar", [
        ("com.arcadedb", "arcadedb-studio", "26.12.1"),
        ("io.netty", "netty-handler", "4.1.136.Final")]),         # embedded twice: listed once
}
bom = json.load(open(sys.argv[1]))
for c in bom["components"]:
    c["hashes"] = [{"alg": "SHA-512", "content": digests[c["name"]]}]
json.dump(bom, open(work / "merge-jvm-emb.json", "w"))
repo = work / "m2"
def pom(g, a, v, body):
    d = repo / g.replace(".", "/") / a / v
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{a}-{v}.pom").write_text(f"""<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
{body}<artifactId>{a}</artifactId><version>{v}</version></project>
""")
pom("org.example", "licensed", "1.0",
    "<parent><groupId>org.example</groupId><artifactId>mid</artifactId><version>2</version></parent>")
pom("org.example", "mid", "2",
    "<parent><groupId>org.example</groupId><artifactId>top</artifactId><version>3</version></parent><groupId>org.example</groupId>")
def licensed(a, name, url):
    pom("org.example", a, "1.0", f"<groupId>org.example</groupId><licenses><license><name>{name}</name>"
        f"<url>{url}</url></license></licenses>")
licensed("custom", "Some Custom License", "https://example.org/custom")
licensed("byurl", "The License", "https://opensource.org/licenses/MIT")
licensed("conflict", "MIT License", "https://www.apache.org/licenses/LICENSE-2.0")
pom("org.example", "top", "3",
    "<groupId>org.example</groupId><licenses><license><name>The Apache Software License, Version 2.0</name>"
    "<url>https://www.apache.org/licenses/LICENSE-2.0.txt</url></license></licenses>")
PY
expect "adds the artifacts embedded in shipped jars" 0 "" \
    "$MERGE" --root "$work/merge-jvm-emb.json" --lib-dir "$work/lib-emb" \
    --module-boms "$work/modules/*/target/*-cyclonedx.json" --maven-repo "$work/m2" "${RELEASE_ARGS[@]}" \
    --out "$work/merge-emb.cdx.json"
EMB_PY='
def all_comps():
    return list(comps(bom["components"]))
def base(c):
    return (c.get("purl") or "").partition("?")[0]
def count(purl_base):
    return sum(1 for c in all_comps() if base(c) == purl_base)
def nested(jar, name):
    return [c for c in top[jar].get("components", []) if c["name"] == name]
def prop(c, key):
    return [p["value"] for p in c.get("properties", []) if p["name"] == key]
'
bom_expect "an embedded coordinate is added once, under the first jar that carries it" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
netty = nested("arcadedb-gremlin", "netty-handler")
ok = (count("pkg:maven/io.netty/netty-handler@4.1.136.Final") == 1 and len(netty) == 1
      and netty[0]["purl"] == "pkg:maven/io.netty/netty-handler@4.1.136.Final?type=jar"
      and netty[0]["group"] == "io.netty" and netty[0]["version"] == "4.1.136.Final" and netty[0]["type"] == "library")'
bom_expect "an embedded component carries both arcadedb:embedded properties" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
netty = nested("arcadedb-gremlin", "netty-handler")[0]
ok = prop(netty, "arcadedb:embedded") == ["true"] and prop(netty, "arcadedb:embedded-in") == ["arcadedb-gremlin-26.12.1-shaded.jar,arcadedb-studio-26.12.1.jar"]'
bom_expect "the jar depends on what it embeds" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
ok = nested("arcadedb-gremlin", "netty-handler")[0]["bom-ref"] in deps[top["arcadedb-gremlin"]["bom-ref"]]'
bom_expect "every jar that carries an embedded coordinate depends on its one component" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
ok = nested("arcadedb-gremlin", "netty-handler")[0]["bom-ref"] in deps[top["arcadedb-studio"]["bom-ref"]]'
bom_expect "embedded components are in coordinate order, whatever the zip entry order" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
emb = [(c["group"], c["name"], c["version"]) for c in top["arcadedb-gremlin"]["components"] if prop(c, "arcadedb:embedded")]
ok = emb == sorted(emb) and len(emb) == 5'
bom_expect "a jar's own coordinate is not added as embedded" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
ok = (not nested("arcadedb-gremlin", "arcadedb-gremlin") and not nested("arcadedb-engine", "arcadedb-engine")
      and not nested("arcadedb-studio", "arcadedb-studio"))'
bom_expect "a coordinate already in the document is not duplicated" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
ok = (count("pkg:maven/com.arcadedb/arcadedb-engine@26.12.1") == 1
      and count("pkg:maven/org.apache.tinkerpop/gremlin-core@3.7.4") == 1)'
bom_expect "an embedded license is resolved through the parent poms, as an SPDX id" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
ok = nested("arcadedb-gremlin", "licensed")[0].get("licenses") == [{"license": {"id": "Apache-2.0", "url": "https://www.apache.org/licenses/LICENSE-2.0.txt"}}]'
bom_expect "a pom license maps to an SPDX id by name or by url, and keeps its name otherwise" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
lic = lambda a: nested("arcadedb-gremlin", a)[0]["licenses"][0]["license"]
ok = (lic("custom") == {"name": "Some Custom License", "url": "https://example.org/custom"}
      and lic("byurl") == {"id": "MIT", "url": "https://opensource.org/licenses/MIT"}
      and lic("conflict") == {"name": "MIT License", "url": "https://www.apache.org/licenses/LICENSE-2.0"})'
bom_expect "an unresolvable embedded license is left out, not made up" "$work/merge-emb.cdx.json" \
    "$EMB_PY"'
ok = not nested("arcadedb-gremlin", "netty-handler")[0].get("licenses")'
expect "the checker passes an embedded component without hash or license" 0 "" "$SBOMFIELDS" "$work/merge-emb.cdx.json"
expect "...and says how many embedded components have no license" 0 "1 embedded component(s) without a license" \
    "$SBOMFIELDS" "$work/merge-emb.cdx.json"
python3 - "$work/merge-emb.cdx.json" "$work/merge-emb-unmarked.cdx.json" <<'PY' || true
import json, sys
bom = json.load(open(sys.argv[1]))
for jar in bom["components"]:
    for c in jar.get("components", []):
        if c["name"] == "netty-handler":
            c["properties"] = [p for p in c["properties"] if p["name"] != "arcadedb:embedded"]
json.dump(bom, open(sys.argv[2], "w"))
PY
expect "the checker still fails the same component once it is not marked embedded" 1 "netty-handler@4.1.136.Final?type=jar: missing hashes" \
    "$SBOMFIELDS" "$work/merge-emb-unmarked.cdx.json"
python3 - "$work/merge-emb.cdx.json" "$work/merge-emb-toplevel.cdx.json" <<'PY' || true
import json, sys
bom = json.load(open(sys.argv[1]))
jar = next(c for c in bom["components"] if c["name"] == "arcadedb-gremlin")
netty = next(c for c in jar["components"] if c["name"] == "netty-handler")
jar["components"].remove(netty)
bom["components"].append(netty)
json.dump(bom, open(sys.argv[2], "w"))
PY
expect "the embedded exemption needs a library parent: a top-level marked component without a hash fails" 1 \
    "netty-handler@4.1.136.Final?type=jar: missing hashes" "$SBOMFIELDS" "$work/merge-emb-toplevel.cdx.json"

# Studio's SBOM comes from its package-lock.json: every package npm installs without --omit dev, with
# the lockfile's integrity as its hash and its declared license. A generator that let a dev package
# through, mis-decoded the base64 integrity or mangled a scoped name would describe a different
# Studio; one that made a license up would hide exactly the package a licence review must look at.
echo
echo "npm-lock-to-cdx.py"

NPMCDX="$SCRIPTS/npm-lock-to-cdx.py"
NPMLOCK="$SBOMFIX/npm-lock/package-lock.json"
expect "converts a lockfile" 0 "" "$NPMCDX" "$NPMLOCK" "$work/npm.cdx.json"
bom_expect "dev packages are left out, every shipped one is in" "$work/npm.cdx.json" \
    'sorted((c["name"], c["version"]) for c in bom["components"]) == [("@popperjs/core", "2.11.8"), ("fonts", "7.0.0"), ("left", "1.0.0"), ("right", "1.0.0"), ("right", "2.0.0")]'
bom_expect "a scoped package's purl encodes the @" "$work/npm.cdx.json" \
    '[c["purl"] for c in bom["components"] if c["name"] == "@popperjs/core"] == ["pkg:npm/%40popperjs/core@2.11.8"]'
bom_expect "the sha512 integrity becomes a hex SHA-512 hash" "$work/npm.cdx.json" \
    '[c["hashes"] for c in bom["components"] if c["name"] == "@popperjs/core"] == [[{"alg": "SHA-512", "content": "2c048c0ff9b66c56fbe0fe12e167f595032e6c0000dd13706f0da0ea6478f8f9a57f5a0d5d78cbb05f105bd1a77d6607589ae0ea7c7146ac1e9e46193baed03b"}]]'
bom_expect "a single license is an SPDX id, a compound one an expression" "$work/npm.cdx.json" \
    '{c["name"]: c["licenses"] for c in bom["components"] if c["name"] in ("left", "fonts")} == {"left": [{"license": {"id": "ISC"}}], "fonts": [{"expression": "(CC-BY-4.0 AND OFL-1.1 AND MIT)"}]}'
bom_expect "the root depends on its direct dependencies" "$work/npm.cdx.json" \
    'sorted(deps[bom["metadata"]["component"]["bom-ref"]]) == sorted(c["bom-ref"] for c in bom["components"] if c["name"] in ("@popperjs/core", "left", "fonts"))'
bom_expect "a dependency resolves to the nested node_modules copy first" "$work/npm.cdx.json" \
    'deps[next(c["bom-ref"] for c in bom["components"] if c["name"] == "left")] == [next(c["bom-ref"] for c in bom["components"] if c["name"] == "right" and c["version"] == "2.0.0")]'
bom_expect "the root component is the lockfile's package" "$work/npm.cdx.json" \
    'bom["metadata"]["component"]["name"] == "studio-fixture" and bom["metadata"]["component"]["version"] == "1.2.3" and bom["specVersion"] == "1.6"'
"$MERGE" --root "$MERGEJVM" --nested "$work/npm.cdx.json" --name arcadedb-studio "${RELEASE_ARGS[@]}" \
    --out "$work/npm-merged.cdx.json" || true
expect "a generated Studio BOM nested into the JVM BOM passes the strict release check" 0 "" \
    "$SBOMFIELDS" "$work/npm-merged.cdx.json"

python3 - "$NPMLOCK" "$work/npm-nolicense.json" <<'PY'
import json, sys
lock = json.load(open(sys.argv[1]))
del lock["packages"]["node_modules/left"]["license"]
del lock["packages"]["node_modules/fonts"]["license"]
json.dump(lock, open(sys.argv[2], "w"))
PY
expect "a shipped package without a license fails, naming it" 1 "node_modules/left" \
    "$NPMCDX" "$work/npm-nolicense.json" "$work/npm-nolicense.cdx.json"
expect "...and names every such package" 1 "node_modules/fonts" \
    "$NPMCDX" "$work/npm-nolicense.json" "$work/npm-nolicense.cdx.json"
if [[ -e $work/npm-nolicense.cdx.json ]]; then
    fail "a failed conversion writes no BOM"
else
    pass "a failed conversion writes no BOM"
fi
expect "a document without a packages map (lockfile v1) is refused" 1 "lockfileVersion" \
    "$NPMCDX" "$SBOMFIX/merge-jvm.cdx.json" "$work/npm-v1.cdx.json"
expect "npm-lock-to-cdx.py without arguments exits 2" 2 "Usage" "$NPMCDX"

# Policy shared with check-sbom-fields.py: only SHA-512 and SHA-256 count, so a sha384- or sha1-only package fails here,
# by name, instead of producing a BOM the release check rejects with a bare `hashes`.
python3 - "$NPMLOCK" "$work" <<'PY'
import base64, hashlib, json, sys
lock = json.load(open(sys.argv[1]))
def write(name, mutate):
    l = json.loads(json.dumps(lock))
    mutate(l["packages"])
    json.dump(l, open(f"{sys.argv[2]}/npm-{name}.json", "w"))
b64 = lambda alg, data: base64.b64encode(hashlib.new(alg, data).digest()).decode()
write("sha384", lambda p: p["node_modules/left"].update(integrity="sha384-" + b64("sha384", b"left")))
write("sha1", lambda p: p["node_modules/left"].update(integrity="sha1-" + b64("sha1", b"left")))
write("sha384-and-512", lambda p: p["node_modules/left"].update(
    integrity="sha384-" + b64("sha384", b"left") + " sha512-" + b64("sha512", b"left")))
write("proprietary", lambda p: p["node_modules/left"].update(license="Proprietary"))
PY
expect "a sha384-only package fails, naming it" 1 "node_modules/left: no sha512 or sha256 integrity" \
    "$NPMCDX" "$work/npm-sha384.json" "$work/npm-sha384.cdx.json"
expect "a sha1-only package fails, naming it" 1 "node_modules/left: no sha512 or sha256 integrity" \
    "$NPMCDX" "$work/npm-sha1.json" "$work/npm-sha1.cdx.json"
"$NPMCDX" "$work/npm-sha384-and-512.json" "$work/npm-sha384-and-512.cdx.json" || true
bom_expect "of sha384 and sha512, only the SHA-512 hash is emitted" "$work/npm-sha384-and-512.cdx.json" \
    '[h["alg"] for c in bom["components"] if c["name"] == "left" for h in c["hashes"]] == ["SHA-512"]'
"$NPMCDX" "$work/npm-proprietary.json" "$work/npm-proprietary.cdx.json" || true
bom_expect "a license that is not a known SPDX id is a license name, not an id" "$work/npm-proprietary.cdx.json" \
    '[c["licenses"] for c in bom["components"] if c["name"] == "left"] == [[{"license": {"name": "Proprietary"}}]]'
mkdir -p "$work/npm-out-is-a-dir.cdx.json"
expect "an output path that cannot be written fails, naming the destination" 1 "$work/npm-out-is-a-dir.cdx.json: cannot write the SBOM" \
    "$NPMCDX" "$NPMLOCK" "$work/npm-out-is-a-dir.cdx.json"

# A `link: true` entry is a symlink in node_modules, not a package: what ships is its target entry.
python3 - "$NPMLOCK" "$work/npm-link.json" <<'PY'
import base64, hashlib, json, sys
lock = json.load(open(sys.argv[1]))
packages = lock["packages"]
packages[""]["dependencies"]["linked"] = "file:../lib/linked"
packages["node_modules/linked"] = {"resolved": "../lib/linked", "link": True}
packages["../lib/linked"] = {"name": "linked", "version": "0.1.0", "license": "MIT",
                             "integrity": "sha512-" + base64.b64encode(hashlib.sha512(b"linked").digest()).decode()}
json.dump(lock, open(sys.argv[2], "w"))
PY
"$NPMCDX" "$work/npm-link.json" "$work/npm-link.cdx.json" || true
bom_expect "a link entry is skipped and its target ships once" "$work/npm-link.cdx.json" \
    '[(c["bom-ref"], c["name"]) for c in bom["components"] if "linked" in c["bom-ref"]] == [("../lib/linked", "linked")]'
bom_expect "a dependency on a link resolves to the link's target" "$work/npm-link.cdx.json" \
    '"../lib/linked" in deps[bom["metadata"]["component"]["bom-ref"]]'

# check-vex.sh: the document the release gate trusts must be well-formed, and a not_affected needs its reasons.
CHECKVEX="$SCRIPTS/check-vex.sh"
VEXFILE="$SCRIPTS/../../vex/arcadedb.openvex.json"
expect "the shipped VEX document passes" 0 "OK" "$CHECKVEX" "$VEXFILE"
expect "check-vex.sh without arguments exits 2" 2 "Usage" "$CHECKVEX"
vex_variant() {
    jq "$1" "$VEXFILE" > "$work/vex.json"
}
stmt='{"vulnerability":{"name":"CVE-2026-1"},"products":[{"@id":"pkg:maven/a/b@1"}],"status":"not_affected"'
vex_variant ".statements = [$stmt,\"justification\":\"component_not_present\",\"impact_statement\":\"unused\"}]"
expect "a not_affected with justification and impact_statement passes" 0 "OK" "$CHECKVEX" "$work/vex.json"
vex_variant ".statements = [$stmt,\"impact_statement\":\"unused\"}]"
expect "a not_affected without justification fails, naming the CVE" 1 "CVE-2026-1" "$CHECKVEX" "$work/vex.json"
vex_variant ".statements = [$stmt,\"justification\":\"component_not_present\"}]"
expect "a not_affected without impact_statement fails" 1 "impact_statement" "$CHECKVEX" "$work/vex.json"
vex_variant ".statements = [$stmt}]"
expect "a not_affected with neither fails" 1 "not_affected" "$CHECKVEX" "$work/vex.json"
vex_variant '.statements = [{"vulnerability":{"name":"CVE-2026-2"},"products":[{"@id":"pkg:maven/a/b@1"}],"status":"under_investigation"}]'
expect "a status other than not_affected needs no justification" 0 "OK" "$CHECKVEX" "$work/vex.json"
vex_variant '.statements = [{}]'
expect "an empty statement fails instead of being skipped" 1 "statement 0: needs vulnerability.name" "$CHECKVEX" "$work/vex.json"
vex_variant '.statements = [null]'
expect "a statement that is not an object fails" 1 "statement 0: not an object" "$CHECKVEX" "$work/vex.json"
for partial in affect "" not_affected_x; do
    vex_variant ".statements = [{\"vulnerability\":{\"name\":\"CVE-2026-7\"},\"products\":[{\"@id\":\"pkg:maven/a/b@1\"}],\"status\":\"$partial\"}]"
    expect "status '$partial' is refused (exact match, not a substring)" 1 "is not one of not_affected" "$CHECKVEX" "$work/vex.json"
done
vex_variant '.statements = [{"vulnerability":{"name":"CVE-2026-3"},"products":[{"@id":"pkg:maven/a/b@1"}],"status":"fixed"}]'
expect "a fixed statement is refused (grype would suppress a fixable critical with it)" 1 \
    'statement 0 (CVE-2026-3): status "fixed" is not one of not_affected, affected, under_investigation' "$CHECKVEX" "$work/vex.json"
vex_variant '.statements = [{"vulnerability":{"name":"CVE-2026-4"},"products":[],"status":"affected"}]'
expect "a statement with no products fails" 1 "needs a non-empty products array" "$CHECKVEX" "$work/vex.json"
vex_variant '.statements = [{"vulnerability":{"name":"CVE-2026-5"},"products":[{"name":"b"}],"status":"affected"}]'
expect "a product without an @id fails" 1 "each with an @id" "$CHECKVEX" "$work/vex.json"
vex_variant ".statements = [$stmt,\"justification\":[\"component_not_present\"],\"impact_statement\":\"unused\"}]"
expect "a justification that is not a string fails" 1 "must be strings" "$CHECKVEX" "$work/vex.json"
vex_variant '.["@context"] = "https://openvex.dev/ns/v0.1.0"'
expect "a wrong @context fails" 1 "OpenVEX" "$CHECKVEX" "$work/vex.json"
vex_variant 'del(.author)'
expect "a missing author fails" 1 "author" "$CHECKVEX" "$work/vex.json"
vex_variant 'del(.timestamp)'
expect "a missing timestamp fails" 1 "timestamp" "$CHECKVEX" "$work/vex.json"
vex_variant 'del(.version)'
expect "a missing version fails" 1 "version" "$CHECKVEX" "$work/vex.json"
vex_variant '.statements = {}'
expect "statements that is not an array fails" 1 "statements" "$CHECKVEX" "$work/vex.json"
echo '{' > "$work/vex.json"
expect "unparseable JSON fails" 1 "OpenVEX" "$CHECKVEX" "$work/vex.json"
# The gate copy release-scan.sh hands grype keeps only the not_affected statements.
vex_variant ".statements = [$stmt,\"justification\":\"component_not_present\",\"impact_statement\":\"unused\"},
    {\"vulnerability\":{\"name\":\"CVE-2026-6\"},\"products\":[{\"@id\":\"pkg:maven/a/b@1\"}],\"status\":\"under_investigation\"}]"
expect "check-vex.sh writes the gate copy of a valid document" 0 "OK" "$CHECKVEX" "$work/vex.json" "$work/vex-gate.json"
if [[ "$(jq -c '[.statements[] | [.vulnerability.name, .status]]' "$work/vex-gate.json")" == '[["CVE-2026-1","not_affected"]]' ]]; then
    pass "the gate copy carries only the not_affected statements"
else
    fail "the gate copy carries only the not_affected statements" "$(cat "$work/vex-gate.json")"
fi
rm -f "$work/vex-gate.json"
vex_variant '.statements = [{"vulnerability":{"name":"CVE-2026-3"},"products":[{"@id":"pkg:maven/a/b@1"}],"status":"fixed"}]'
expect "an invalid document writes no gate copy" 1 "fixed" "$CHECKVEX" "$work/vex.json" "$work/vex-gate.json"
if [[ -e "$work/vex-gate.json" ]]; then fail "an invalid document writes no gate copy (file)"; else pass "an invalid document writes no gate copy (file)"; fi
if grep -q -- '--vex "${GATE_VEX}"' "$SCRIPTS/release-scan.sh" && grep -q 'check-vex.sh "${VEX}" "${GATE_VEX}"' "$SCRIPTS/release-scan.sh" \
    && ! grep -q -- '--vex "${VEX}"' "$SCRIPTS/release-scan.sh"; then
    pass "release-scan.sh hands grype only the validated not_affected copy"
else
    fail "release-scan.sh hands grype only the validated not_affected copy"
fi

echo
echo "check-release-signing.py"

SIGNING="$SCRIPTS/check-release-signing.py"
SIGNFIX="$SCRIPTS/tests/fixtures/signing"

expect "accepts a signed, guarded release job" 0 "" "$SIGNING" "$SIGNFIX/signed.yml"
expect "rejects a push with no later cosign sign" 1 "unsigned-push.yml:docker:Push: (a)" "$SIGNING" "$SIGNFIX/unsigned-push.yml"
expect "rejects a tag step that precedes the signing step" 1 "sign-after-tag.yml:docker-and-tag:Tag: (b)" "$SIGNING" "$SIGNFIX/sign-after-tag.yml"
expect "rejects an attach_to_release job that signs without a main-only guard" 1 "unguarded-attach.yml:attest:Sign blob: (c)" "$SIGNING" "$SIGNFIX/unguarded-attach.yml"
expect "rejects a multi-line docker buildx build --push with no sign step" 1 "buildx-push.yml:j:Build: (a)" "$SIGNING" "$SIGNFIX/buildx-push.yml"
expect "accepts a docker buildx build --push followed by cosign sign" 0 "" "$SIGNING" "$SIGNFIX/buildx-push-signed.yml"
expect "rejects docker/build-push-action with push: true and no sign step" 1 "action-push.yml:j:<step 1>: (a)" "$SIGNING" "$SIGNFIX/action-push.yml"
expect "accepts docker/build-push-action with push: true followed by cosign sign" 0 "" "$SIGNING" "$SIGNFIX/action-push-signed.yml"
expect "ignores docker/build-push-action with push: false" 0 "" "$SIGNING" "$SIGNFIX/action-nopush.yml"
expect "rejects a ref guard placed after the signing step" 1 "guard-after.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-after.yml"
expect "rejects a ref guard that only echoes" 1 "guard-echo.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-echo.yml"
# A guard counts only if it fails the run off main: the predicate, a non-zero exit, and no continue-on-error.
expect "rejects an off-main if: guard whose run does not exit non-zero" 1 "guard-if-no-exit.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-if-no-exit.yml"
expect "rejects a run guard that only writes an ::error:: annotation" 1 "guard-annotation-only.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-annotation-only.yml"
expect "rejects an off-main guard with continue-on-error" 1 "guard-continue-on-error.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-continue-on-error.yml"
expect "accepts a run guard that names the ref and main and exits 2" 0 "" "$SIGNING" "$SIGNFIX/guard-run-exit.yml"
expect "rejects an inverted if: guard (== main fires ON main)" 1 "guard-inverted.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-inverted.yml"
expect "rejects an inverted run guard (== main)" 1 "guard-run-inverted.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-run-inverted.yml"
expect "rejects a guard whose ref test exists only in a comment" 1 "guard-comment-only.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-comment-only.yml"
expect "rejects an off-main if: guard whose exit exists only in a comment" 1 "guard-comment-exit.yml:j:Sign: (c)" "$SIGNING" "$SIGNFIX/guard-comment-exit.yml"
# A signing step counts only if it runs on a release and its failure fails the job.
expect "rejects a push whose cosign sign step is skipped by if: false" 1 "action-push-sign-skipped.yml:j:<step 1>: (a)" "$SIGNING" "$SIGNFIX/action-push-sign-skipped.yml"
expect "rejects a push whose cosign sign step has continue-on-error" 1 "action-push-sign-continue.yml:j:<step 1>: (a)" "$SIGNING" "$SIGNFIX/action-push-sign-continue.yml"
expect "accepts a cosign sign step gated by exactly the release gate" 0 "" "$SIGNING" "$SIGNFIX/action-push-sign-gated.yml"
expect "labels an unnamed step by its 1-based index" 1 "unnamed.yml:j:<step 2>: (a)" "$SIGNING" "$SIGNFIX/unnamed.yml"
expect "a missing workflow is a usage error" 2 "" "$SIGNING" "$SIGNFIX/does-not-exist.yml"
# SIGNED_IN_LATER_JOB: native-image.yml's `docker` push is signed by `sign-native-images`, and each condition is verified.
expect "accepts a push signed by the mapped, release-gated later job that needs it" 0 "" "$SIGNING" "$SIGNFIX/later-job-ok/native-image.yml"
expect "rejects a mapped signing job that does not need the push job" 1 "native-image.yml:docker:Push per-arch image: (a) SIGNED_IN_LATER_JOB names \`sign-native-images\`, which does not need \`docker\`" "$SIGNING" "$SIGNFIX/later-job-no-needs/native-image.yml"
expect "rejects a mapped signing job that does not exist" 1 "which does not exist" "$SIGNING" "$SIGNFIX/later-job-missing/native-image.yml"
expect "rejects a mapped signing job not gated to releases" 1 "which is not gated by" "$SIGNING" "$SIGNFIX/later-job-ungated/native-image.yml"
expect "rejects a mapped signing job whose gate merely contains the release test" 1 "which is not gated by exactly" "$SIGNING" "$SIGNFIX/later-job-always/native-image.yml"
expect "accepts the release gate with other whitespace and line breaks" 0 "" "$SIGNING" "$SIGNFIX/later-job-spacing/native-image.yml"
expect "accepts the release gate in another case (GitHub expressions ignore case)" 0 "" "$SIGNING" "$SIGNFIX/later-job-gate-case/native-image.yml"
expect "rejects a mapped signing job with no cosign sign step" 1 "which has no effective \`cosign sign\` step" "$SIGNING" "$SIGNFIX/later-job-unsigned/native-image.yml"
expect "rejects a mapped signing job whose cosign sign step is skipped by if: false" 1 "which has no effective \`cosign sign\` step" "$SIGNING" "$SIGNFIX/later-job-step-skipped/native-image.yml"
expect "rejects a mapped signing job whose cosign sign step has continue-on-error" 1 "which has no effective \`cosign sign\` step" "$SIGNING" "$SIGNFIX/later-job-step-continue/native-image.yml"
expect "rejects a mapped signing job with a job-level continue-on-error" 1 "which has a job-level \`continue-on-error\`" "$SIGNING" "$SIGNFIX/later-job-job-continue/native-image.yml"
expect "the mapping applies only to the workflow it names" 1 "later-job-other-workflow.yml:docker:Push per-arch image: (a) image push with no later" "$SIGNING" "$SIGNFIX/later-job-other-workflow.yml"
# mvn-release.yml: `docker` pushes, `sign-image` signs with no job-level gate (the workflow is release-only), `tag` tags.
expect "accepts the release-only split: push job, ungated signing job, tag job after it" 0 "" "$SIGNING" "$SIGNFIX/release-split-ok/mvn-release.yml"
expect "an ungated signing job is refused once the release workflow gains another trigger" 1 \
    "mvn-release.yml:docker:Build and push Docker images: (a) SIGNED_IN_LATER_JOB names \`sign-image\` with no release gate, but mvn-release.yml has a trigger other than workflow_dispatch" \
    "$SIGNING" "$SIGNFIX/release-split-push-trigger/mvn-release.yml"
expect "a job-level if on the release-only signing job is refused" 1 "which must have no job-level \`if:\` in the release-only mvn-release.yml" \
    "$SIGNING" "$SIGNFIX/release-split-gated/mvn-release.yml"
expect "a tag job that does not need the signing job is refused" 1 \
    "mvn-release.yml:tag:Tag and commit release version: (b) tags the release without needing \`sign-image\`, which signs the image" \
    "$SIGNING" "$SIGNFIX/release-split-tag-unordered/mvn-release.yml"
expect "a tag job with if: always() is refused although it needs the signing job" 1 \
    "mvn-release.yml:tag:Tag and commit release version: (b) tags the release in a job whose \`if:\` runs it after the signing failed" \
    "$SIGNING" "$SIGNFIX/release-split-tag-always/mvn-release.yml"
expect "a tag job with if: !cancelled() is refused although it needs the signing job" 1 \
    "mvn-release.yml:tag:Tag and commit release version: (b) tags the release in a job whose \`if:\` runs it after the signing failed" \
    "$SIGNING" "$SIGNFIX/release-split-tag-not-cancelled/mvn-release.yml"
expect "a tag job with if: Always() is refused (function names are case-insensitive)" 1 \
    "mvn-release.yml:tag:Tag and commit release version: (b) tags the release in a job whose \`if:\` runs it after the signing failed" \
    "$SIGNING" "$SIGNFIX/release-split-tag-always-case/mvn-release.yml"
expect "the live release workflows pass the signing check" 0 "release signing is enforced in 2 workflow(s)" "$SIGNING"

echo
echo "release-assets.sh / verify-release.sh"

# Stub gh, cosign, gpg and syft on PATH (fixtures/release/stubs): the release, the workflow runs and their artifacts are
# directories under $STUB_DIR, so every branch runs without GitHub, Sigstore or a signing key.
RELASSETS="$SCRIPTS/release-assets.sh"
VERIFYREL="$SCRIPTS/verify-release.sh"
RELSTUBS="$PWD/$SCRIPTS/tests/fixtures/release/stubs"
RELTAG="26.12.1"

# A fresh stub world: the release already carries what native-image.yml and publish-contract.yml upload, and one
# mvn-release run on main (id 101) holds both artifacts. Run 101 FAILED overall (its `tag` job hit a merge conflict),
# but its `attest` and `sign-image` jobs succeeded, which is all a release needs. Listed before it, each with different
# bytes so picking one shows: a run of the same title dispatched from a branch (103), a rehearsal (102, usable in every
# other respect), a newer run whose `attest` failed (104) and one whose image was never pushed and signed (105).
release_world() {
    local d="$1" v="$RELTAG"
    rm -rf "$d"
    mkdir -p "$d/release" "$d/runs" "$d/artifacts/101/release-dist/sbom/full" "$d/artifacts/101/release-dist/sbom/base" \
        "$d/artifacts/101/release-image-sbom" "$d/artifacts/102/release-dist" "$d/work"
    for f in "arcadedb-$v-linux-x86_64.tar.gz" "arcadedb-$v-linux-aarch_64.tar.gz" "arcadedb-$v-windows-x86_64.zip" \
        "arcadedb-openapi-$v.json" "arcadedb-server-$v.proto" \
        "arcadedb-image-$v-native-amd64.cdx.json" "arcadedb-image-$v-native-amd64.spdx.json" \
        "arcadedb-image-$v-native-arm64.cdx.json" "arcadedb-image-$v-native-arm64.spdx.json"; do
        echo "upstream $f" >"$d/release/$f"
    done
    for f in "arcadedb-$v-linux-x86_64.tar.gz" "arcadedb-$v-linux-aarch_64.tar.gz" "arcadedb-$v-windows-x86_64.zip" \
        "arcadedb-openapi-$v.json" "arcadedb-server-$v.proto"; do
        echo "checksum of $f" >"$d/release/$f.sha256"
    done
    local dist="$d/artifacts/101/release-dist"
    for f in "arcadedb-$v.tar.gz" "arcadedb-$v-base.tar.gz" "arcadedb-$v.zip" "arcadedb-$v-base.zip"; do
        echo "ci $f" >"$dist/$f"
        echo "ci checksum $f" >"$dist/$f.sha256"
    done
    echo '{"bomFormat":"CycloneDX"}' >"$dist/sbom/full/arcadedb-$v.cdx.json"
    echo '{"bomFormat":"CycloneDX","base":true}' >"$dist/sbom/base/arcadedb-$v-base.cdx.json"
    echo '{"bomFormat":"CycloneDX","image":true}' >"$d/artifacts/101/release-image-sbom/arcadedb-image-$v.cdx.json"
    echo '{"spdxVersion":"SPDX-2.3","image":true}' >"$d/artifacts/101/release-image-sbom/arcadedb-image-$v.spdx.json"
    local id
    for id in 102 103 104 105; do
        rm -rf "$d/artifacts/$id"
        cp -R "$d/artifacts/101" "$d/artifacts/$id"
        echo "built by run $id" >"$d/artifacts/$id/release-dist/arcadedb-$v.tar.gz"
    done
    rm -rf "$d/artifacts/105/release-image-sbom"
    cat >"$d/runs/mvn-release.yml.json" <<JSON
[ {"databaseId": 105, "displayTitle": "release $v", "headBranch": "main", "conclusion": "failure"},
  {"databaseId": 104, "displayTitle": "release $v", "headBranch": "main", "conclusion": "failure"},
  {"databaseId": 103, "displayTitle": "release $v", "headBranch": "feature/x", "conclusion": "success"},
  {"databaseId": 102, "displayTitle": "release $v (rehearsal)", "headBranch": "main", "conclusion": "success"},
  {"databaseId": 99, "displayTitle": "release 26.11.1", "headBranch": "main", "conclusion": "success"},
  {"databaseId": 101, "displayTitle": "release $v", "headBranch": "main", "conclusion": "failure"} ]
JSON
    local both='{"artifacts": [ {"name": "release-dist", "expired": false}, {"name": "release-image-sbom", "expired": false} ]}'
    local attest_ok='{"jobs": [ {"name": "central", "conclusion": "success"}, {"name": "attest", "conclusion": "success"},
        {"name": "docker", "conclusion": "success"}, {"name": "sign-image", "conclusion": "success"}, {"name": "tag", "conclusion": "failure"} ]}'
    for id in 101 102 103; do
        echo "$both" >"$d/runs/$id-artifacts.json"
        echo "$attest_ok" >"$d/runs/$id-jobs.json"
    done
    echo "$both" >"$d/runs/104-artifacts.json"
    echo '{"jobs": [ {"name": "central", "conclusion": "success"}, {"name": "attest", "conclusion": "failure"} ]}' >"$d/runs/104-jobs.json"
    echo '{"artifacts": [ {"name": "release-dist", "expired": false} ]}' >"$d/runs/105-artifacts.json"
    echo '{"jobs": [ {"name": "attest", "conclusion": "success"}, {"name": "docker", "conclusion": "failure"} ]}' >"$d/runs/105-jobs.json"
    echo '{"id": 101, "path": ".github/workflows/mvn-release.yml", "head_branch": "main", "display_title": "release '"$v"'"}' >"$d/runs/101-run.json"
    echo '{"id": 102, "path": ".github/workflows/mvn-release.yml", "head_branch": "main", "display_title": "release '"$v"' (rehearsal)"}' >"$d/runs/102-run.json"
    echo '{"id": 103, "path": ".github/workflows/mvn-release.yml", "head_branch": "feature/x", "display_title": "release '"$v"'"}' >"$d/runs/103-run.json"
    echo '{"id": 104, "path": ".github/workflows/mvn-release.yml", "head_branch": "main", "display_title": "release '"$v"'"}' >"$d/runs/104-run.json"
    echo '{"id": 106, "path": ".github/workflows/other.yml", "head_branch": "main", "display_title": "release '"$v"'"}' >"$d/runs/106-run.json"
    echo '[ {"databaseId": 201, "status": "completed", "conclusion": "success"} ]' >"$d/runs/native-image.yml.json"
    echo '[ {"databaseId": 301, "status": "completed", "conclusion": "success"} ]' >"$d/runs/publish-contract.yml.json"
    : >"$d/calls.log"
}

run_release_assets() {
    local d="$1"
    shift
    env PATH="$RELSTUBS:$PATH" STUB_DIR="$d" GH_REPO=ArcadeData/arcadedb GPG_PASSPHRASE=x "$@" "$RELASSETS" "$RELTAG" "$d/work"
}

# Asserts that a line in the stub's calls.log equals (1) or does not equal (0) the given text.
expect_logged() {
    local description="$1" want="$2" line="$3" log="$4"
    if grep -Fxq -- "$line" "$log"; then got=1; else got=0; fi
    if [[ $got -eq $want ]]; then pass "$description"; else fail "$description" "$(cat "$log")"; fi
}

# (a) asset absent -> uploaded, from the mvn-release run (not the rehearsal), with the derived files and SHA256SUMS.
rw="$work/release-a"
release_world "$rw"
expect "release-assets.sh uploads the CI-built files the release does not carry" 0 "release $RELTAG complete" \
    run_release_assets "$rw"
expect_logged "an absent archive is uploaded" 1 "upload arcadedb-$RELTAG.tar.gz" "$rw/calls.log"
expect_logged "an absent image SBOM is uploaded" 1 "upload arcadedb-image-$RELTAG.spdx.json" "$rw/calls.log"
expect_logged "the distribution SBOM is converted to SPDX and uploaded" 1 "upload arcadedb-$RELTAG-base.spdx.json" "$rw/calls.log"
expect_logged "the VEX document is uploaded under the release name" 1 "upload arcadedb-$RELTAG.openvex.json" "$rw/calls.log"
expect_logged "the cosign bundle of SHA256SUMS is uploaded" 1 "upload SHA256SUMS.sigstore.json" "$rw/calls.log"
expect_logged "the gpg signature of SHA256SUMS is uploaded" 1 "upload SHA256SUMS.asc" "$rw/calls.log"
expect_logged "the JVM zip is not published (the release carries tar.gz only)" 0 "upload arcadedb-$RELTAG.zip" "$rw/calls.log"
if cmp -s "$rw/release/arcadedb-$RELTAG.tar.gz" "$rw/artifacts/101/release-dist/arcadedb-$RELTAG.tar.gz"; then
    pass "the uploaded archive is the one from the release run on main whose attest succeeded, although the run failed"
else
    fail "the uploaded archive is the one from the release run on main whose attest succeeded, although the run failed" "$(cat "$rw/release/arcadedb-$RELTAG.tar.gz")"
fi
if grep -Eq '^gh run download (102|103|104|105) ' "$rw/calls.log"; then
    fail "a branch, rehearsal, failed-attest or image-SBOM-less run of the same title is never downloaded" "$(grep '^gh run download' "$rw/calls.log")"
else
    pass "a branch, rehearsal, failed-attest or image-SBOM-less run of the same title is never downloaded"
fi
expect_logged "the run's jobs are read to find the attest job" 1 \
    "gh api repos/ArcadeData/arcadedb/actions/runs/104/jobs?per_page=100" "$rw/calls.log"
sums_names="$(awk '{print $2}' "$rw/release/SHA256SUMS" | tr '\n' ' ')"
sorted_names="$(find "$rw/release" -maxdepth 1 -type f ! -name 'SHA256SUMS*' -exec basename {} \; | LC_ALL=C sort | tr '\n' ' ')"
if [[ "$sums_names" == "$sorted_names" ]]; then
    pass "SHA256SUMS lists every asset but SHA256SUMS*, sorted by name"
else
    fail "SHA256SUMS lists every asset but SHA256SUMS*, sorted by name" "got:  $sums_names"$'\n'"want: $sorted_names"
fi
if (cd "$rw/release" && sha256sum -c SHA256SUMS >/dev/null 2>&1); then
    pass "SHA256SUMS matches the assets on the release"
else
    fail "SHA256SUMS matches the assets on the release" "$(cat "$rw/release/SHA256SUMS")"
fi

# (b) same digest -> not uploaded again. A second run over the release (a) produced is the realistic case: a re-run,
# or the second upstream finishing.
: >"$rw/calls.log"
expect "release-assets.sh succeeds again over a release it already completed" 0 "already on the release, same SHA-256" \
    run_release_assets "$rw"
expect_logged "an archive with the same digest is not re-uploaded" 0 "upload arcadedb-$RELTAG.tar.gz" "$rw/calls.log"
expect_logged "SHA256SUMS is re-signed and replaced" 1 "upload SHA256SUMS" "$rw/calls.log"
if grep -q 'SHA256SUMS\.' "$rw/release/SHA256SUMS"; then
    fail "a previous SHA256SUMS* is not checksummed" "$(cat "$rw/release/SHA256SUMS")"
else
    pass "a previous SHA256SUMS* is not checksummed"
fi

# (c) different digest -> exit 1 with the error, and nothing uploaded at all.
rw="$work/release-c"
release_world "$rw"
echo "replaced by hand" >"$rw/release/arcadedb-$RELTAG-base.tar.gz"
expect "release-assets.sh refuses an asset that differs from the CI-built file" 1 \
    "::error::arcadedb-$RELTAG-base.tar.gz on release $RELTAG differs from the CI-built file" run_release_assets "$rw"
if grep -q '^upload ' "$rw/calls.log"; then
    fail "a digest mismatch uploads nothing, not even the files that are absent" "$(grep '^upload ' "$rw/calls.log")"
else
    pass "a digest mismatch uploads nothing, not even the files that are absent"
fi

# (d) workflow_run with only one upstream finished -> exit 0, nothing uploaded, no signature.
rw="$work/release-d"
release_world "$rw"
echo '[ {"databaseId": 301, "status": "in_progress", "conclusion": ""} ]' >"$rw/runs/publish-contract.yml.json"
: >"$rw/github-output"
expect "release-assets.sh on workflow_run waits for the other upstream" 0 "publish-contract.yml for $RELTAG is in_progress" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run GITHUB_OUTPUT="$rw/github-output"
if [[ -s "$rw/github-output" ]]; then
    fail "an early exit does not report completed (the workflow would verify an incomplete release)" "$(cat "$rw/github-output")"
else
    pass "an early exit does not report completed (the workflow would verify an incomplete release)"
fi
if grep -Eq '^(upload |cosign )' "$rw/calls.log"; then
    fail "with one upstream unfinished nothing is uploaded or signed" "$(cat "$rw/calls.log")"
else
    pass "with one upstream unfinished nothing is uploaded or signed"
fi
expect_logged "the upstream check filters on the release event and the tag" 1 \
    "gh run list --workflow publish-contract.yml --event release --branch $RELTAG --limit 1 --json databaseId,status,conclusion" "$rw/calls.log"
echo '[ {"databaseId": 301, "status": "completed", "conclusion": "failure"} ]' >"$rw/runs/publish-contract.yml.json"
expect "a failed upstream is not finished either" 0 "publish-contract.yml for $RELTAG is completed/failure" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run
expect "a failed upstream is a warning that names it and says SHA256SUMS was not produced" 0 \
    "::warning::publish-contract.yml for $RELTAG is completed/failure; SHA256SUMS was not produced" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run
echo '[ {"databaseId": 301, "status": "in_progress", "conclusion": ""} ]' >"$rw/runs/publish-contract.yml.json"
expect "an upstream still running stays a notice" 0 "::notice::publish-contract.yml for $RELTAG is in_progress" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run
rm "$rw/runs/native-image.yml.json"
expect "an upstream that never ran for the tag is not finished" 0 "native-image.yml for $RELTAG is missing" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run
release_world "$rw"
echo '[ {"databaseId": 201, "status": "in_progress"' >"$rw/runs/native-image.yml.json"
expect "an unparseable upstream run list is an error, not a pending upstream" 1 \
    "::error::the native-image.yml run list for $RELTAG is not a JSON array" run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run
release_world "$rw"
touch "$rw/fail-run-list"
expect "a failed upstream run list is an error, not a pending upstream" 1 \
    "::error::could not list the native-image.yml runs for $RELTAG" run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run
release_world "$rw"
expect "release-assets.sh on workflow_run proceeds once both upstreams succeeded" 0 "release $RELTAG complete" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_run GITHUB_OUTPUT="$rw/github-output"
if [[ "$(cat "$rw/github-output")" == "completed=true" ]]; then
    pass "a completed run reports completed=true for the verify step"
else
    fail "a completed run reports completed=true for the verify step" "$(cat "$rw/github-output")"
fi
expect_logged "and then uploads" 1 "upload arcadedb-$RELTAG.tar.gz" "$rw/calls.log"
: >"$rw/calls.log"
rm "$rw/runs/native-image.yml.json"
expect "workflow_dispatch skips the upstream check" 0 "release $RELTAG complete" \
    run_release_assets "$rw" GITHUB_EVENT_NAME=workflow_dispatch

# Artifacts unavailable. Only an EXPIRED release-dist falls back to the files on the release, and only once their
# attestations verify; any other failure to get the artifacts fails.
rw="$work/release-e"
release_world "$rw"
run_release_assets "$rw" >/dev/null 2>&1
rm -rf "$rw/artifacts/101"
expect "artifacts that cannot be downloaded and are not known to have expired fail" 1 \
    "could not download the artifacts of mvn-release run 101, and they have not expired" run_release_assets "$rw"
echo '{"artifacts": [ {"name": "release-dist", "expired": false}, {"name": "release-image-sbom", "expired": false} ]}' \
    >"$rw/runs/101-artifacts.json"
expect "a download failure of an artifact that has not expired fails" 1 "and they have not expired" run_release_assets "$rw"
echo '{"artifacts": [ {"name": "release-dist", "expired": true}, {"name": "release-image-sbom", "expired": true} ]}' \
    >"$rw/runs/101-artifacts.json"
: >"$rw/calls.log"
expect "with the artifacts expired, the attested files on the release are re-signed" 0 "have expired" \
    run_release_assets "$rw"
expect_logged "with the artifacts expired, SHA256SUMS is still replaced" 1 "upload SHA256SUMS" "$rw/calls.log"
attested_re='^gh attestation verify .*/arcadedb-26\.12\.1(-base)?\.(tar\.gz|cdx\.json) --repo ArcadeData/arcadedb --cert-identity-regex \^https://github\\\.com/ArcadeData/arcadedb/\\\.github/workflows/mvn-release\\\.yml@refs/heads/main\$ '
if [[ "$(grep -cE "$attested_re" "$rw/calls.log")" == 4 ]]; then
    pass "with the artifacts expired, both JVM archives and both distribution SBOMs are checked against the mvn-release.yml@main identity"
else
    fail "with the artifacts expired, both JVM archives and both distribution SBOMs are checked against the mvn-release.yml@main identity" "$(cat "$rw/calls.log")"
fi
echo 'base\.cdx\.json' >"$rw/fail-attestation"
: >"$rw/calls.log"
expect "with the artifacts expired, an unattested file on the release fails" 1 \
    "arcadedb-$RELTAG-base.cdx.json on release $RELTAG has no verified build provenance" run_release_assets "$rw"
if grep -Eq '^(upload |cosign sign-blob)' "$rw/calls.log"; then
    fail "an unattested file is neither signed nor followed by uploads" "$(cat "$rw/calls.log")"
else
    pass "an unattested file is neither signed nor followed by uploads"
fi
rm "$rw/fail-attestation"
rm "$rw/release/arcadedb-image-$RELTAG.cdx.json"
expect "with the artifacts expired, a CI-built file missing from the release fails" 1 \
    "arcadedb-image-$RELTAG.cdx.json is missing" run_release_assets "$rw"

rw="$work/release-f"
release_world "$rw"
cat >"$rw/runs/mvn-release.yml.json" <<JSON
[ {"databaseId": 102, "displayTitle": "release $RELTAG (rehearsal)", "headBranch": "main"},
  {"databaseId": 103, "displayTitle": "release $RELTAG", "headBranch": "feature/x"} ]
JSON
expect "a rehearsal or branch run is never a source of release files" 1 \
    "no mvn-release.yml run on main titled 'release $RELTAG' with successful attest and sign-image jobs" run_release_assets "$rw"
if grep -q '^upload ' "$rw/calls.log"; then
    fail "without a release run nothing is uploaded" "$(cat "$rw/calls.log")"
else
    pass "without a release run nothing is uploaded"
fi

# The run selection: only the `attest` and `sign-image` jobs matter, never the run's own conclusion nor its artifact
# records (GitHub stops listing expired artifacts, while the jobs stay).
rw="$work/release-g"
release_world "$rw"
echo '{"jobs": [ {"name": "attest", "conclusion": "failure"}, {"name": "sign-image", "conclusion": "success"} ]}' >"$rw/runs/101-jobs.json"
expect "a run whose attest job failed is not a release run" 1 "with successful attest and sign-image jobs" run_release_assets "$rw"
echo '{"jobs": [ {"name": "attest", "conclusion": "success"}, {"name": "sign-image", "conclusion": "failure"} ]}' >"$rw/runs/101-jobs.json"
expect "a run whose sign-image job failed is not a release run" 1 "with successful attest and sign-image jobs" run_release_assets "$rw"
release_world "$rw"
run_release_assets "$rw" >/dev/null 2>&1
echo '{"artifacts": []}' >"$rw/runs/101-artifacts.json"
rm -rf "$rw/artifacts/101"
: >"$rw/calls.log"
expect "a run whose artifacts are no longer listed at all is still selected and falls back to the attested release files" 0 \
    "the artifacts of mvn-release run 101 have expired" run_release_assets "$rw"
expect_logged "with the artifact list empty, SHA256SUMS is still replaced" 1 "upload SHA256SUMS" "$rw/calls.log"
if [[ "$(grep -cE "$attested_re" "$rw/calls.log")" == 4 ]]; then
    pass "with the artifact list empty, the release files are trusted only after their attestations verify"
else
    fail "with the artifact list empty, the release files are trusted only after their attestations verify" "$(cat "$rw/calls.log")"
fi
echo '{"artifacts": [ {"name": "release-image-sbom", "expired": true} ]}' >"$rw/runs/101-artifacts.json"
expect "a release-dist record absent from a non-empty list counts as expired" 0 \
    "the artifacts of mvn-release run 101 have expired" run_release_assets "$rw"
expect_logged "the artifact list is filtered by name, so no page of it can hide release-dist" 1 \
    "gh api repos/ArcadeData/arcadedb/actions/runs/101/artifacts?name=release-dist&per_page=100" "$rw/calls.log"
rm "$rw/runs/101-artifacts.json"
expect "after a download failure, an unreadable artifact list fails instead of falling back" 1 \
    "could not list the artifacts of mvn-release run 101" run_release_assets "$rw"
release_world "$rw"
rm "$rw/runs/101-jobs.json"
expect "an unreadable job list fails instead of skipping the run" 1 "could not list the jobs of mvn-release run 101" \
    run_release_assets "$rw"

# The dispatch input names the run; it must pass the same checks.
release_world "$rw"
expect "mvn_release_run_id naming the release run is used" 0 "CI-built files from mvn-release run 101" \
    run_release_assets "$rw" MVN_RELEASE_RUN_ID=101
expect_logged "mvn_release_run_id skips the search" 0 \
    "gh run list --workflow mvn-release.yml --limit 200 --json databaseId,displayTitle,headBranch" "$rw/calls.log"
release_world "$rw"
expect "mvn_release_run_id naming a run whose attest failed is refused" 1 \
    "mvn-release run 104 has no successful \`attest\` and \`sign-image\` jobs" run_release_assets "$rw" MVN_RELEASE_RUN_ID=104
echo '{"id": 105, "path": ".github/workflows/mvn-release.yml", "head_branch": "main", "display_title": "release '"$RELTAG"'"}' \
    >"$rw/runs/105-run.json"
expect "mvn_release_run_id naming a run whose image was never signed is refused" 1 \
    "mvn-release run 105 has no successful" run_release_assets "$rw" MVN_RELEASE_RUN_ID=105
expect "mvn_release_run_id naming a branch run is refused" 1 \
    "run 103 is not an mvn-release.yml run on main titled 'release $RELTAG'" run_release_assets "$rw" MVN_RELEASE_RUN_ID=103
expect "mvn_release_run_id naming a rehearsal is refused in ArcadeData/arcadedb" 1 \
    "run 102 is not an mvn-release.yml run on main" run_release_assets "$rw" MVN_RELEASE_RUN_ID=102
expect "mvn_release_run_id naming another workflow's run is refused" 1 \
    "run 106 is not an mvn-release.yml run on main" run_release_assets "$rw" MVN_RELEASE_RUN_ID=106
expect "mvn_release_run_id naming no run is refused" 1 "mvn-release run 999 not found" \
    run_release_assets "$rw" MVN_RELEASE_RUN_ID=999
expect "mvn_release_run_id that is not a number is refused" 1 "is not a run id" \
    run_release_assets "$rw" "MVN_RELEASE_RUN_ID=101 --foo"
if grep -q '^upload ' "$rw/calls.log"; then
    fail "a refused mvn_release_run_id uploads nothing" "$(grep '^upload ' "$rw/calls.log")"
else
    pass "a refused mvn_release_run_id uploads nothing"
fi

# A fork rehearses the whole chain, so there the rehearsal run is the release run; in ArcadeData/arcadedb never.
rw="$work/release-h"
release_world "$rw"
cat >"$rw/runs/mvn-release.yml.json" <<JSON
[ {"databaseId": 102, "displayTitle": "release $RELTAG (rehearsal)", "headBranch": "main", "conclusion": "success"} ]
JSON
expect "on a fork a rehearsal run supplies the release files" 0 "CI-built files from mvn-release run 102" \
    run_release_assets "$rw" GH_REPO=someone/arcadedb
expect "on a fork mvn_release_run_id may name a rehearsal" 0 "CI-built files from mvn-release run 102" \
    run_release_assets "$rw" GH_REPO=someone/arcadedb MVN_RELEASE_RUN_ID=102
release_world "$rw"
cat >"$rw/runs/mvn-release.yml.json" <<JSON
[ {"databaseId": 102, "displayTitle": "release $RELTAG (rehearsal)", "headBranch": "main", "conclusion": "success"} ]
JSON
expect "in ArcadeData/arcadedb a rehearsal run never supplies release files" 1 "no mvn-release.yml run on main titled" \
    run_release_assets "$rw"
expect "the ArcadeData/arcadedb rule ignores the case of the repository name" 1 "no mvn-release.yml run on main titled" \
    run_release_assets "$rw" GH_REPO=arcadedata/ArcadeDB

expect "release-assets.sh refuses a tag that is not a version" 2 "is not a release tag" \
    env PATH="$RELSTUBS:$PATH" STUB_DIR="$work" "$RELASSETS" "../main" "$work/x"
expect "release-assets.sh without arguments exits 2" 2 "Usage" "$RELASSETS"

# verify-release.sh against the release (a) completed: clean, then with several independent breakages at once,
# every one of which must be reported (not only the first).
rw="$work/release-v"
release_world "$rw"
run_release_assets "$rw" >/dev/null 2>&1
RELFPR="0123456789ABCDEF0123456789ABCDEF01234567"   # the stub gpg's signing key
run_verify() {
    env PATH="$RELSTUBS:$PATH" STUB_DIR="$rw" GITHUB_REPOSITORY=ArcadeData/arcadedb GPG_PUBLIC_KEY_FILE=/dev/null \
        RELEASE_GPG_FINGERPRINT="$RELFPR" "$@" "$VERIFYREL" "$RELTAG"
}
expect "verify-release.sh passes a complete, signed release" 0 "every check passed" run_verify
expect "the release key fingerprint is compared without case or spaces" 0 "every check passed" \
    run_verify RELEASE_GPG_FINGERPRINT="0123 4567 89ab cdef 0123  4567 89AB CDEF 0123 4567"
expect "the primary key fingerprint of a subkey signature matches too" 0 "every check passed" \
    run_verify RELEASE_GPG_FINGERPRINT=89ABCDEF0123456789ABCDEF0123456789ABCDEF
expect "a valid signature by another key fails" 1 \
    "SHA256SUMS.asc is signed by $RELFPR, not by the release key FEDCBA9876543210FEDCBA9876543210FEDCBA98" \
    run_verify RELEASE_GPG_FINGERPRINT=FEDCBA9876543210FEDCBA9876543210FEDCBA98
expect "an unset release key fingerprint fails" 1 \
    "set the repository variable RELEASE_GPG_FINGERPRINT to the release signing key fingerprint" \
    run_verify RELEASE_GPG_FINGERPRINT=
expect "a missing release key fingerprint fails" 1 "set the repository variable RELEASE_GPG_FINGERPRINT" \
    env -u RELEASE_GPG_FINGERPRINT PATH="$RELSTUBS:$PATH" STUB_DIR="$rw" GPG_PUBLIC_KEY_FILE=/dev/null "$VERIFYREL" "$RELTAG"
expect_logged "the signature is checked with gpg status output" 1 "gpg --batch --status-fd 1 --verify SHA256SUMS.asc SHA256SUMS" \
    "$rw/calls.log"
expect_logged "cosign verify-blob uses the pinned release-workflow identity" 1 \
    'cosign verify-blob --bundle SHA256SUMS.sigstore.json --certificate-identity-regexp ^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$ --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS' \
    "$rw/calls.log"
expect_logged "the provenance of a native archive is verified" 1 \
    "gh attestation verify arcadedb-$RELTAG-windows-x86_64.zip --repo ArcadeData/arcadedb --cert-identity-regex ^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$ --cert-oidc-issuer https://token.actions.githubusercontent.com" \
    "$rw/calls.log"
expect_logged "the per-arch native image SBOM attestation is verified" 1 \
    "cosign verify-attestation --type cyclonedx --certificate-identity-regexp ^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$ --certificate-oidc-issuer https://token.actions.githubusercontent.com arcadedata/arcadedb:$RELTAG-native-arm64" \
    "$rw/calls.log"
expect_logged "the native manifest list is checked for a signature only" 0 \
    "cosign verify-attestation --type cyclonedx --certificate-identity-regexp ^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$ --certificate-oidc-issuer https://token.actions.githubusercontent.com arcadedata/arcadedb:$RELTAG-native" \
    "$rw/calls.log"
for ref in "arcadedata/arcadedb:$RELTAG" "arcadedata/arcadedb:$RELTAG-native-amd64" "arcadedata/arcadedb:$RELTAG-native-arm64"; do
    expect_logged "the SLSA provenance of image $ref is verified with the release identity" 1 \
        "gh attestation verify oci://index.docker.io/$ref --repo ArcadeData/arcadedb --cert-identity-regex ^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$ --cert-oidc-issuer https://token.actions.githubusercontent.com" \
        "$rw/calls.log"
done
if grep -q "^gh attestation verify oci://index.docker.io/arcadedata/arcadedb:$RELTAG-native " "$rw/calls.log"; then
    fail "the native manifest list is not checked for provenance (it carries a signature only)" "$(grep oci:// "$rw/calls.log")"
else
    pass "the native manifest list is not checked for provenance (it carries a signature only)"
fi
echo "arcadedb:$RELTAG-native-arm64\$" >"$rw/fail-attestation"
expect "an image that lost its SLSA provenance fails" 1 \
    "arcadedata/arcadedb:$RELTAG-native-arm64 has no verified build provenance" run_verify
rm "$rw/fail-attestation"
mkdir -p "$rw/consumer"
echo "stub key" >"$rw/consumer/release-key.asc"
: >"$rw/calls.log"
expect "a relative GPG_PUBLIC_KEY_FILE is read from the caller's directory, not the download directory" 0 "every check passed" \
    bash -c 'cd "$1" && shift && "$@"' _ "$rw/consumer" env PATH="$RELSTUBS:$PATH" STUB_DIR="$rw" GITHUB_REPOSITORY=ArcadeData/arcadedb \
    GPG_PUBLIC_KEY_FILE=release-key.asc RELEASE_GPG_FINGERPRINT="$RELFPR" "$PWD/$VERIFYREL" "$RELTAG"
expect_logged "the relative key file is imported by its absolute path" 1 "gpg --batch --import $rw/consumer/release-key.asc" "$rw/calls.log"
: >"$rw/calls.log"
expect "a fork verifies against its own identity and namespace" 0 "every check passed" \
    env PATH="$RELSTUBS:$PATH" STUB_DIR="$rw" GITHUB_REPOSITORY=someone/arcadedb DOCKER_NAMESPACE=someone \
    GPG_PUBLIC_KEY_FILE=/dev/null RELEASE_GPG_FINGERPRINT="$RELFPR" "$VERIFYREL" "$RELTAG"
if grep -q 'cosign verify .*github\\.com/someone/arcadedb/.* someone/arcadedb:'"$RELTAG"'$' "$rw/calls.log"; then
    pass "the fork's JVM image is verified in its namespace with its identity"
else
    fail "the fork's JVM image is verified in its namespace with its identity" "$(cat "$rw/calls.log")"
fi
for ref in "someone/arcadedb:$RELTAG-native" "someone/arcadedb:$RELTAG-native-amd64" "someone/arcadedb:$RELTAG-native-arm64"; do
    if grep -q "^cosign verify .* $ref\$" "$rw/calls.log"; then
        pass "the fork's native image $ref is verified in its namespace"
    else
        fail "the fork's native image $ref is verified in its namespace" "$(cat "$rw/calls.log")"
    fi
done
if grep -q 'arcadedata/arcadedb' "$rw/calls.log"; then
    fail "a fork's verification never reaches the arcadedata namespace" "$(grep arcadedata/arcadedb "$rw/calls.log")"
else
    pass "a fork's verification never reaches the arcadedata namespace"
fi

echo "tampered" >>"$rw/release/arcadedb-$RELTAG-linux-x86_64.tar.gz"
echo "late" >"$rw/release/arcadedb-late-$RELTAG.txt"
rm "$rw/release/arcadedb-$RELTAG.openvex.json"
echo 'verify-blob' >"$rw/fail-cosign"
echo 'arcadedata/arcadedb:'"$RELTAG"'-native-amd64$' >>"$rw/fail-cosign"
touch "$rw/fail-gpg"
echo 'aarch_64|openapi' >"$rw/fail-attestation"
for needle in \
    "arcadedb-$RELTAG.openvex.json is missing from the release" \
    "SHA256SUMS does not match the release assets" \
    "arcadedb-late-$RELTAG.txt is not covered by SHA256SUMS" \
    "the cosign signature of SHA256SUMS does not verify" \
    "the gpg signature of SHA256SUMS does not verify" \
    "arcadedb-$RELTAG-linux-aarch_64.tar.gz has no verified build provenance" \
    "arcadedb-openapi-$RELTAG.json has no verified build provenance" \
    "arcadedata/arcadedb:$RELTAG-native-amd64 has no verified signature" \
    "arcadedata/arcadedb:$RELTAG-native-amd64 has no verified CycloneDX SBOM attestation" \
    "9 check(s) failed"; do
    expect "verify-release.sh reports every failure: $needle" 1 "$needle" run_verify
done
expect "verify-release.sh refuses a tag that is not a version" 2 "is not a release tag" "$VERIFYREL" "main"
expect "verify-release.sh without arguments exits 2" 2 "Usage" "$VERIFYREL"

echo
echo "check-release-credentials.sh"

# Real gpg (throwaway keys in a private keyring) and a stub curl (fixtures/release-credentials/stubs): the GPG checks
# are the ones a wrong secret breaks, so they run against gpg itself; the HTTP services answer from $STUB_DIR/http.
RELCRED="$SCRIPTS/check-release-credentials.sh"
CREDSTUBS="$PWD/$SCRIPTS/tests/fixtures/release-credentials/stubs"
credgpg="$work/credgpg"
mkdir -m 700 "$credgpg"
gen_key() {   # <uid> <expiry>: prints the fingerprint
    gpg --homedir "$credgpg" --batch --pinentry-mode loopback --passphrase pw --quick-gen-key "$1" ed25519 sign "$2" >/dev/null 2>&1
    gpg --homedir "$credgpg" --batch --with-colons --list-keys "$1" | awk -F: '$1 == "fpr" { print $10; exit }'
}
CREDFPR="$(gen_key "Release Test <release@example.com>" 3y)"
SOONFPR="$(gen_key "Soon Expiring <soon@example.com>" 10d)"
CREDKEY="$(gpg --homedir "$credgpg" --batch --pinentry-mode loopback --passphrase pw --armor --export-secret-keys "$CREDFPR")"
SOONKEY="$(gpg --homedir "$credgpg" --batch --pinentry-mode loopback --passphrase pw --armor --export-secret-keys "$SOONFPR")"
gpgconf --homedir "$credgpg" --kill gpg-agent >/dev/null 2>&1 || true

docker_body() {   # <repository> <actions json array>: a token endpoint answer whose JWT grants those actions
    local payload
    payload="$(printf '{"access":[{"type":"repository","name":"%s","actions":%s}]}' "$1" "$2" | base64 | tr -d '\n=' | tr '/+' '_-')"
    printf '{"token":"eyJhbGciOiJub25lIn0.%s.c2ln"}' "$payload"
}

# A fresh stub world in which every service accepts the credentials and nothing is published or tagged yet.
cred_world() {
    local d="$work/cred.$1"
    rm -rf "$d"
    mkdir -p "$d/http"
    echo '{"published":false}' >"$d/http/central.body"
    docker_body arcadedata/arcadedb '["pull","push"]' >"$d/http/docker.body"
    echo "$d"
}

run_cred() {   # <stub dir> [VAR=value ...]: the check with good credentials, overridden by the arguments
    local d="$1"
    shift
    env -u GITHUB_TOKEN -u SKIP_CENTRAL -u DOCKER_REPOSITORY PATH="$CREDSTUBS:$PATH" STUB_DIR="$d" \
        GPG_PRIVATE_KEY="$CREDKEY" GPG_PASSPHRASE=pw RELEASE_GPG_FINGERPRINT="$CREDFPR" \
        CENTRAL_USERNAME=cu CENTRAL_PASSWORD=cp DOCKER_USERNAME=du DOCKER_PASSWORD=dp GITHUB_REPOSITORY=ArcadeData/arcadedb \
        "$@" "$RELCRED" 26.12.1
}

cw="$(cred_world good)"
expect "check-release-credentials.sh passes when every credential works" 0 "all checks passed, 0 warning(s)" run_cred "$cw"
if grep -qx "Authorization: Bearer $(printf 'cu:cp' | base64)" "$cw/calls.log" &&
    grep -qx "Authorization: Basic $(printf 'du:dp' | base64)" "$cw/calls.log"; then
    pass "check-release-credentials.sh sends the Central token as Bearer and the Docker Hub token as Basic"
else
    fail "check-release-credentials.sh sends the Central token as Bearer and the Docker Hub token as Basic" "$(cat "$cw/calls.log")"
fi
if grep -q 'scope=repository:arcadedata/arcadedb:push,pull' "$cw/calls.log" &&
    grep -q "central.sonatype.com/api/v1/publisher/published?namespace=com.arcadedb&name=arcadedb-engine&version=26.12.1" "$cw/calls.log"; then
    pass "check-release-credentials.sh asks Docker Hub for push on the release repository and Central about the release version"
else
    fail "check-release-credentials.sh asks Docker Hub for push on the release repository and Central about the release version" "$(cat "$cw/calls.log")"
fi
expect "check-release-credentials.sh accepts the fingerprint in lower case with spaces" 0 "all checks passed" \
    run_cred "$(cred_world spaced)" RELEASE_GPG_FINGERPRINT="$(tr '[:upper:]' '[:lower:]' <<<"$CREDFPR" | sed 's/..../& /g')"

expect "check-release-credentials.sh fails on a wrong passphrase" 1 "MAVEN_GPG_PASSPHRASE is probably not the passphrase" \
    run_cred "$(cred_world pass)" GPG_PASSPHRASE=wrong
expect "check-release-credentials.sh fails when the key is not RELEASE_GPG_FINGERPRINT" 1 "update them together" \
    run_cred "$(cred_world fpr)" RELEASE_GPG_FINGERPRINT="$SOONFPR"
expect "check-release-credentials.sh fails without RELEASE_GPG_FINGERPRINT" 1 "RELEASE_GPG_FINGERPRINT is not set" \
    run_cred "$(cred_world nofpr)" RELEASE_GPG_FINGERPRINT=
expect "check-release-credentials.sh fails on an empty private key" 1 "MAVEN_GPG_PRIVATE_KEY) is empty" \
    run_cred "$(cred_world nokey)" GPG_PRIVATE_KEY=
expect "check-release-credentials.sh fails on a private key that does not import" 1 "does not import" \
    run_cred "$(cred_world badkey)" GPG_PRIVATE_KEY="not a key"
expect "check-release-credentials.sh fails on a key expiring within EXPIRY_FAIL_DAYS" 1 "the signing key expires in" \
    run_cred "$(cred_world soon)" GPG_PRIVATE_KEY="$SOONKEY" RELEASE_GPG_FINGERPRINT="$SOONFPR"
expect "check-release-credentials.sh only warns on a key expiring within EXPIRY_WARN_DAYS" 0 "all checks passed, 1 warning(s)" \
    run_cred "$(cred_world warnsoon)" GPG_PRIVATE_KEY="$SOONKEY" RELEASE_GPG_FINGERPRINT="$SOONFPR" EXPIRY_FAIL_DAYS=5

cw="$(cred_world onekeyserver)"
echo 404 >"$cw/http/openpgp.code"
expect "check-release-credentials.sh only warns when one keyserver lacks the key" 0 "but not on keys.openpgp.org" run_cred "$cw"
cw="$(cred_world nokeyserver)"
echo 404 >"$cw/http/openpgp.code"
echo 404 >"$cw/http/ubuntu.code"
expect "check-release-credentials.sh fails when no keyserver has the key" 1 "on neither keyserver.ubuntu.com nor keys.openpgp.org" run_cred "$cw"

cw="$(cred_world central401)"
echo 401 >"$cw/http/central.code"
expect "check-release-credentials.sh fails when Central rejects the token" 1 "generate a new Portal user token" run_cred "$cw"
cw="$(cred_world published)"
echo '{"published":true}' >"$cw/http/central.body"
expect "check-release-credentials.sh fails when the version is already on Central" 1 "is already published on Maven Central" run_cred "$cw"
expect "check-release-credentials.sh fails on empty Central credentials" 1 "MAVEN_CENTRAL_PASSWORD) are empty" \
    run_cred "$(cred_world nocentral)" CENTRAL_PASSWORD=
cw="$(cred_world skipcentral)"
expect "check-release-credentials.sh skips Central with SKIP_CENTRAL=true" 0 "skipped - Maven Central" \
    run_cred "$cw" SKIP_CENTRAL=true CENTRAL_USERNAME= CENTRAL_PASSWORD=
if [[ ! -s "$cw/calls.log" ]] || grep -qE 'central.sonatype.com|keyserver.ubuntu.com|keys.openpgp.org' "$cw/calls.log"; then
    fail "check-release-credentials.sh with SKIP_CENTRAL=true calls neither Central nor the keyservers" "$(cat "$cw/calls.log")"
else
    pass "check-release-credentials.sh with SKIP_CENTRAL=true calls neither Central nor the keyservers"
fi
cw="$(cred_world rehearsal)"
echo 404 >"$cw/http/openpgp.code"
echo 404 >"$cw/http/ubuntu.code"
expect "check-release-credentials.sh with SKIP_CENTRAL=true accepts a test key on no keyserver" 0 "skipped - keyservers" \
    run_cred "$cw" SKIP_CENTRAL=true

cw="$(cred_world pullonly)"
docker_body arcadedata/arcadedb '["pull"]' >"$cw/http/docker.body"
expect "check-release-credentials.sh fails on a read-only Docker Hub token" 1 "cannot push to arcadedata/arcadedb (granted: pull)" run_cred "$cw"
cw="$(cred_world otherrepo)"
docker_body someone/arcadedb '["pull","push"]' >"$cw/http/docker.body"
expect "check-release-credentials.sh fails when push is granted only on another repository" 1 "cannot push to arcadedata/arcadedb" run_cred "$cw"
cw="$(cred_world docker401)"
echo 401 >"$cw/http/docker.code"
expect "check-release-credentials.sh fails when Docker Hub rejects the credentials" 1 "Docker Hub rejects DOCKER_USERNAME" run_cred "$cw"
cw="$(cred_world forkns)"
docker_body someone/arcadedb '["pull","push"]' >"$cw/http/docker.body"
expect "check-release-credentials.sh checks push on DOCKER_REPOSITORY" 0 "can push to someone/arcadedb" run_cred "$cw" DOCKER_REPOSITORY=someone/arcadedb

cw="$(cred_world tagged)"
echo 200 >"$cw/http/github.code"
expect "check-release-credentials.sh fails when the release tag exists" 1 "the tag 26.12.1 already exists in ArcadeData/arcadedb" run_cred "$cw"

cw="$(cred_world everything)"
echo 401 >"$cw/http/central.code"
docker_body arcadedata/arcadedb '["pull"]' >"$cw/http/docker.body"
echo 200 >"$cw/http/github.code"
expect "check-release-credentials.sh reports every failure in one run" 1 "4 check(s) failed" run_cred "$cw" GPG_PASSPHRASE=wrong
expect "check-release-credentials.sh without arguments exits 2" 2 "Usage" "$RELCRED"

echo
if [[ $failures -gt 0 ]]; then
    echo "$failures of $checks checks failed"
    exit 1
fi
echo "$checks checks passed"
