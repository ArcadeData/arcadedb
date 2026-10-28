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
RELEASEASSETS="$SCRIPTS/check-release-upload-assets.py"
CREDSCOPE="$SCRIPTS/check-deploy-credential-scope.py"
WRAPPER="$SCRIPTS/check-maven-wrapper.py"
PROMPTMUT="$SCRIPTS/check-claude-prompt-mutations.py"
PINCOMMENTS="$SCRIPTS/check-action-pin-comments.py"
CHAOSSUMMARY="$SCRIPTS/chaos-run-summary.sh"

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
# is "no-profiles". Extra profile XML can be passed as $5.
march_pom() {
    local path="$1" arg="$2" default="$3" profiles="${4:-}" extra="${5:-}"
    local arg_xml="" profile_xml=""
    [[ -n $arg ]] && arg_xml="<buildArg>$arg</buildArg>"
    if [[ $profiles != no-profiles ]]; then
        profile_xml='
        <profile><id>native-march-amd64</id><activation><os><arch>amd64</arch></os></activation>
            <properties><native.march>x86-64-v3</native.march></properties></profile>
        <profile><id>native-march-x86_64</id><activation><os><arch>x86_64</arch></os></activation>
            <properties><native.march>x86-64-v3</native.march></properties></profile>
        <profile><id>native-march-aarch64</id><activation><os><arch>aarch64</arch></os></activation>
            <properties><native.march>armv8.1-a</native.march></properties></profile>'
    fi
    cat >"$path" <<XML
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <properties>
        <native.march>$default</native.march>
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
printf '## CPU requirements\n\n| Arch | -march |\n|---|---|\n| amd64 | `x86-64-v3` |\n| arm64 | `armv8.1-a` |\n\nOverride: `-Dnative.march=x86-64-v2`.\n\n## Next\n\n| `x86-64-v4` |\n' >"$work/march/docs.md"
printf 'jobs:\n  build:\n    steps:\n      - run: ./mvnw -Pnative -Dnative.static=true package\n' >"$work/march/clean.yml"

march_pom "$work/march/ok-pom.xml" '-march=${native.march}' compatibility
expect "accepts a per-arch -march that the docs name" 0 "amd64=x86-64-v3, x86_64=x86-64-v3, aarch64=armv8.1-a" \
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

# The repository's own pom, docs, workflow and scripts must pass.
expect "the repository's own native CPU baseline is explicit and documented" 0 "explicit and documented" \
    "$NATIVEMARCH"

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
  "faults": "kill:3,pause",
  "result": "PASS",
  "message": "Completed 12 steps\nsecond line",
  "steps": 12,
  "ledger": { "acked": 100, "ackedLate": 1, "failed": 2, "unknown": 0, "lostUnknown": 0, "inFlight": 0 },
  "lateCommits": 5,
  "reformats": 7,
  "stepsWithUnreadReformats": 1,
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
    ledger.lostUnknown ledger.inFlight lateCommits reformats stepsWithUnreadReformats replay addedLater nothing; do
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
chaos_keys="$(sed -n '/public void summary(/,/Files.writeString/p' \
    e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosReport.java | grep -oE '\.put\("[a-zA-Z]+"' | sed -E 's/\.put\("(.*)"/\1/')"
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

# The label the review flags with must be the one the repository has: `do not merge`, with spaces.
# #10600's instruction named "do to not merge", and its own report then named `do-not-merge`.
if grep -qF "labels[]=do not merge'" .github/workflows/claude-code-review.yml; then
    pass "the review flags a hazardous PR with the repository's 'do not merge' label"
else
    fail "the review flags a hazardous PR with the repository's 'do not merge' label"
fi

echo
if [[ $failures -gt 0 ]]; then
    echo "$failures of $checks checks failed"
    exit 1
fi
echo "$checks checks passed"
