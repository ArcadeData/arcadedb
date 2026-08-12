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
RESULTS="$SCRIPTS/check-test-results.py"
GUARDS="$SCRIPTS/check-test-reporter-guards.py"
ALLOWLIST="$SCRIPTS/check-license-allowlist.py"

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
if [[ $failures -gt 0 ]]; then
    echo "$failures of $checks checks failed"
    exit 1
fi
echo "$checks checks passed"
