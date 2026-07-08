# Bolt CI gating + nightly driver-version matrix (#4891) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add JUnit report aggregation to the JS/Python/C#/Go Bolt e2e jobs, a standalone nightly workflow that runs all five Bolt suites against the full driver-version matrix, and tooling that merges per-suite results into one machine-readable compatibility matrix for #4892.

**Architecture:** Two Python tools under `bolt/conformance/tools/` parse the JUnit XML each suite already can emit (scenario IDs like `CONN-001` are embedded in test names) into a normalized per-language JSON, then merge all languages into `bolt-compat-matrix.json`. A new `bolt-nightly.yml` builds the branch image once and fans out a per-language driver-version matrix (`fail-fast: false`), each cell overriding the driver version at the package-manager level; a final job merges the artifacts and a report job opens/updates one `bolt-compat-regression` tracking issue on any red cell. `mvn-test.yml` gains dorny/test-reporter + artifact upload on the four non-Java e2e jobs.

**Tech Stack:** GitHub Actions, Python 3.12 (stdlib `xml.etree`, `unittest`), `dorny/test-reporter`, `actions/github-script`, jest-junit (JS, already present), pytest `--junitxml`, `JunitXml.TestLogger` (.NET), `gotestsum` (Go), `actionlint`.

## Global Constraints

- Java 21 toolchain; Docker profile build (`./mvnw ... -Pdocker`) for images.
- No Claude attribution anywhere (commit trailers, source, comments) - repo rule.
- New workflow YAML files: NO license header (repo convention for `.github/workflows/`).
- New Python source files under `bolt/conformance/tools/`: include the Apache-2.0 header exactly as `bolt/conformance/validate_spec.py` does.
- All third-party actions pinned to a full commit SHA with a `# vX.Y.Z` comment, matching every existing step in `mvn-test.yml`.
- Only Apache-2.0-compatible dependencies (jest-junit MIT, gotestsum Apache-2.0, JunitXml.TestLogger MIT - all allowed).
- Scenario IDs are the join key; the ID pattern is `[A-Z]+-\d{3}` and every ID must exist in `bolt/conformance/spec.yaml`.
- Comments/Javadoc/docstrings state behavioral invariants only - no issue numbers in source comments (issue refs belong in commit messages and the plan/spec docs).

---

### Task 1: `junit_to_matrix.py` - JUnit XML -> normalized per-language JSON

**Files:**
- Create: `bolt/conformance/tools/junit_to_matrix.py`
- Create: `bolt/conformance/tools/__init__.py` (empty, makes the package importable by tests)
- Test: `bolt/conformance/tools/test_junit_to_matrix.py`
- Create fixtures: `bolt/conformance/tools/testdata/sample-junit.xml`, `bolt/conformance/tools/testdata/unknown-id-junit.xml`

**Interfaces:**
- Produces: `parse_junit(xml_path: str) -> dict[str, str]` (scenario-id -> `"pass"|"fail"|"skip"`); `build_matrix(xml_path: str, language: str, driver_version: str, known_ids: set[str]) -> dict`; `load_known_ids(spec_path: str) -> set[str]`. CLI: `python3 junit_to_matrix.py <junit.xml> --language L --driver-version V --spec ../spec.yaml -o out.json`.
- JSON shape: `{"language": L, "driver_version": V, "scenarios": {"CONN-001": "pass", ...}}`.

- [ ] **Step 1: Write the fixtures**

`bolt/conformance/tools/testdata/sample-junit.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="bolt" tests="4" failures="1" skipped="1" errors="0">
    <testcase name="CONN-001 Connect via bolt:// scheme" classname="conn"/>
    <testcase name="AUTH-003 none auth rejected" classname="auth">
      <failure message="expected rejection">boom</failure>
    </testcase>
    <testcase name="TYPE-007 Duration round-trip" classname="type">
      <skipped message="known gap"/>
    </testcase>
    <testcase name="ERR-002 syntax error code" classname="err">
      <error message="unexpected">stacktrace</error>
    </testcase>
  </testsuite>
</testsuites>
```

`bolt/conformance/tools/testdata/unknown-id-junit.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="bolt" tests="1" failures="0" skipped="0">
    <testcase name="ZZZ-999 not a real scenario" classname="x"/>
  </testsuite>
</testsuites>
```

- [ ] **Step 2: Write the failing tests**

`bolt/conformance/tools/test_junit_to_matrix.py`:
```python
import json
import os
import tempfile
import unittest

from junit_to_matrix import parse_junit, build_matrix

HERE = os.path.dirname(__file__)
SAMPLE = os.path.join(HERE, "testdata", "sample-junit.xml")
UNKNOWN = os.path.join(HERE, "testdata", "unknown-id-junit.xml")
KNOWN = {"CONN-001", "AUTH-003", "TYPE-007", "ERR-002"}


class ParseJunitTest(unittest.TestCase):
    def test_maps_status_by_child_element(self):
        result = parse_junit(SAMPLE)
        self.assertEqual(result["CONN-001"], "pass")
        self.assertEqual(result["AUTH-003"], "fail")
        self.assertEqual(result["TYPE-007"], "skip")
        self.assertEqual(result["ERR-002"], "fail")

    def test_extracts_id_from_testcase_name_prefix(self):
        result = parse_junit(SAMPLE)
        self.assertEqual(set(result.keys()), KNOWN)


class BuildMatrixTest(unittest.TestCase):
    def test_shape_and_metadata(self):
        m = build_matrix(SAMPLE, "python", "6.2.0", KNOWN)
        self.assertEqual(m["language"], "python")
        self.assertEqual(m["driver_version"], "6.2.0")
        self.assertEqual(m["scenarios"]["CONN-001"], "pass")

    def test_unknown_scenario_id_raises(self):
        with self.assertRaises(ValueError):
            build_matrix(UNKNOWN, "python", "6.2.0", KNOWN)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd bolt/conformance/tools && python3 -m unittest test_junit_to_matrix.py -v`
Expected: FAIL - `ModuleNotFoundError: No module named 'junit_to_matrix'`.

- [ ] **Step 4: Write the implementation**

`bolt/conformance/tools/junit_to_matrix.py` (prepend the Apache-2.0 header copied verbatim from `bolt/conformance/validate_spec.py`):
```python
"""Convert a suite's JUnit XML into a normalized scenario-id -> status map.

Scenario IDs (pattern ``[A-Z]+-\\d{3}``) are embedded in every suite's test
names; this reduces a language/driver-version run to a comparable map keyed by
those IDs so runs can be merged across languages.
"""
import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET

ID_RE = re.compile(r"\b([A-Z]+-\d{3})\b")


def parse_junit(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    results = {}
    for tc in root.iter("testcase"):
        name = tc.get("name", "")
        match = ID_RE.search(name)
        if not match:
            continue
        scenario = match.group(1)
        status = "pass"
        for child in tc:
            tag = child.tag.split("}")[-1]
            if tag in ("failure", "error"):
                status = "fail"
                break
            if tag == "skipped":
                status = "skip"
                break
        # A scenario asserted by several testcases fails if any fails.
        prev = results.get(scenario)
        if prev == "fail" or status == "fail":
            results[scenario] = "fail"
        elif prev == "pass" or status == "pass":
            results[scenario] = "pass"
        else:
            results[scenario] = status
    return results


def load_known_ids(spec_path):
    ids = set()
    with open(spec_path, encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped.startswith("- id:"):
                ids.add(stripped.split("- id:", 1)[1].strip())
    return ids


def build_matrix(xml_path, language, driver_version, known_ids):
    scenarios = parse_junit(xml_path)
    unknown = set(scenarios) - set(known_ids)
    if unknown:
        raise ValueError(f"JUnit references unknown scenario IDs: {sorted(unknown)}")
    return {"language": language, "driver_version": driver_version, "scenarios": scenarios}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit_xml")
    parser.add_argument("--language", required=True)
    parser.add_argument("--driver-version", required=True)
    parser.add_argument("--spec", required=True, help="path to spec.yaml")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    known = load_known_ids(args.spec)
    matrix = build_matrix(args.junit_xml, args.language, args.driver_version, known)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(matrix, fh, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd bolt/conformance/tools && python3 -m unittest test_junit_to_matrix.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add bolt/conformance/tools/junit_to_matrix.py bolt/conformance/tools/__init__.py bolt/conformance/tools/test_junit_to_matrix.py bolt/conformance/tools/testdata/
git commit -m "feat(#4891): junit_to_matrix tool - JUnit XML to normalized scenario map"
```

---

### Task 2: `merge_matrix.py` - merge per-language JSON into one compatibility matrix

**Files:**
- Create: `bolt/conformance/tools/merge_matrix.py`
- Test: `bolt/conformance/tools/test_merge_matrix.py`

**Interfaces:**
- Consumes: per-language JSON produced by `build_matrix` (Task 1).
- Produces: `merge(matrices: list[dict]) -> dict` returning `{"scenarios": {"CONN-001": {"python": {"6.2.0": "pass"}, "java": {...}}}, "languages": [...], "has_failures": bool}`. CLI: `python3 merge_matrix.py cellA.json cellB.json ... -o bolt-compat-matrix.json`.

- [ ] **Step 1: Write the failing tests**

`bolt/conformance/tools/test_merge_matrix.py`:
```python
import unittest

from merge_matrix import merge


class MergeTest(unittest.TestCase):
    def _cell(self, lang, ver, scen):
        return {"language": lang, "driver_version": ver, "scenarios": scen}

    def test_nests_scenario_language_version(self):
        a = self._cell("python", "6.2.0", {"CONN-001": "pass", "ERR-002": "fail"})
        b = self._cell("go", "5.28.4", {"CONN-001": "pass"})
        out = merge([a, b])
        self.assertEqual(out["scenarios"]["CONN-001"]["python"]["6.2.0"], "pass")
        self.assertEqual(out["scenarios"]["CONN-001"]["go"]["5.28.4"], "pass")
        self.assertEqual(out["scenarios"]["ERR-002"]["python"]["6.2.0"], "fail")

    def test_languages_sorted_and_deduped(self):
        out = merge([self._cell("go", "1", {}), self._cell("java", "2", {}), self._cell("go", "3", {})])
        self.assertEqual(out["languages"], ["go", "java"])

    def test_has_failures_flag(self):
        self.assertTrue(merge([self._cell("py", "1", {"X-001": "fail"})])["has_failures"])
        self.assertFalse(merge([self._cell("py", "1", {"X-001": "pass"})])["has_failures"])
        self.assertFalse(merge([self._cell("py", "1", {"X-001": "skip"})])["has_failures"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd bolt/conformance/tools && python3 -m unittest test_merge_matrix.py -v`
Expected: FAIL - `ModuleNotFoundError: No module named 'merge_matrix'`.

- [ ] **Step 3: Write the implementation**

`bolt/conformance/tools/merge_matrix.py` (prepend the Apache-2.0 header):
```python
"""Merge per-language/-version scenario maps into one compatibility matrix.

Output is keyed scenario -> language -> driver_version -> status, plus a
``has_failures`` flag consumed by the nightly report job.
"""
import argparse
import json
import sys


def merge(matrices):
    scenarios = {}
    languages = set()
    has_failures = False
    for cell in matrices:
        lang = cell["language"]
        ver = cell["driver_version"]
        languages.add(lang)
        for scenario, status in cell["scenarios"].items():
            scenarios.setdefault(scenario, {}).setdefault(lang, {})[ver] = status
            if status == "fail":
                has_failures = True
    return {
        "scenarios": scenarios,
        "languages": sorted(languages),
        "has_failures": has_failures,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cells", nargs="+", help="per-language JSON files")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    matrices = []
    for path in args.cells:
        with open(path, encoding="utf-8") as fh:
            matrices.append(json.load(fh))
    merged = merge(matrices)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd bolt/conformance/tools && python3 -m unittest test_merge_matrix.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/tools/merge_matrix.py bolt/conformance/tools/test_merge_matrix.py
git commit -m "feat(#4891): merge_matrix tool - combine per-language maps into compat matrix"
```

---

### Task 3: Wire the conformance-tools tests into `bolt-conformance-spec.yml`

**Files:**
- Modify: `.github/workflows/bolt-conformance-spec.yml`

**Interfaces:**
- Consumes: the two `test_*.py` files from Tasks 1-2.

- [ ] **Step 1: Add a tools-test step**

After the existing "Run validator unit tests" step in `.github/workflows/bolt-conformance-spec.yml`, add:
```yaml
      - name: Run conformance tools unit tests
        working-directory: bolt/conformance/tools
        run: |
          python3 -m unittest test_junit_to_matrix.py -v
          python3 -m unittest test_merge_matrix.py -v
```

- [ ] **Step 2: Verify workflow lint**

Run: `actionlint .github/workflows/bolt-conformance-spec.yml`
Expected: no output (clean).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/bolt-conformance-spec.yml
git commit -m "ci(#4891): run conformance tools unit tests in spec-validation workflow"
```

---

### Task 4: JS e2e - surface the existing jest-junit report in CI

**Files:**
- Modify: `.github/workflows/mvn-test.yml` (`js-e2e-tests` job, lines ~346-377)

**Interfaces:**
- Consumes: `e2e-js/reports/jest-junit.xml` (already produced - `package.json` configures jest-junit to `reports/jest-junit.xml`).

- [ ] **Step 1: Add reporter + artifact steps**

In `js-e2e-tests`, after the "E2E Node.js Tests" step, add:
```yaml
      - name: JS E2E Tests Reporter
        uses: dorny/test-reporter@a43b3a5f7366b97d083190328d2c652e1a8b6aa2 # v3.0.0
        if: success() || failure()
        with:
          name: JS E2E Tests Report
          path: "e2e-js/reports/jest-junit.xml"
          list-suites: "failed"
          list-tests: "failed"
          reporter: java-junit

      - name: Upload JS E2E JUnit report
        if: success() || failure()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: js-e2e-junit
          path: e2e-js/reports/jest-junit.xml
          retention-days: 7
```
Also add `permissions: { contents: read, checks: write }` to the `js-e2e-tests` job (required by dorny/test-reporter), matching `java-e2e-tests`.

- [ ] **Step 2: Verify workflow lint**

Run: `actionlint .github/workflows/mvn-test.yml`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/mvn-test.yml
git commit -m "ci(#4891): aggregate JS e2e JUnit report + upload artifact"
```

---

### Task 5: Python e2e - emit and aggregate JUnit

**Files:**
- Modify: `.github/workflows/mvn-test.yml` (`python-e2e-tests` job, lines ~438-473)

- [ ] **Step 1: Emit JUnit + add reporter/artifact**

In `python-e2e-tests`, change the test command to write JUnit, then add reporter + artifact steps:
```yaml
      - name: E2E Python Tests
        working-directory: e2e-python
        run: |
          uv pip install --system -e .
          uv pip install --system pytest
          mkdir -p reports
          pytest tests/ --junitxml=reports/python-junit.xml
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          ARCADEDB_DOCKER_IMAGE: ${{ needs.build-and-package.outputs.image-tag }}

      - name: Python E2E Tests Reporter
        uses: dorny/test-reporter@a43b3a5f7366b97d083190328d2c652e1a8b6aa2 # v3.0.0
        if: success() || failure()
        with:
          name: Python E2E Tests Report
          path: "e2e-python/reports/python-junit.xml"
          list-suites: "failed"
          list-tests: "failed"
          reporter: java-junit

      - name: Upload Python E2E JUnit report
        if: success() || failure()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: python-e2e-junit
          path: e2e-python/reports/python-junit.xml
          retention-days: 7
```
Add `permissions: { contents: read, checks: write }` to the `python-e2e-tests` job.

- [ ] **Step 2: Verify locally that pytest emits the file**

Run: `cd e2e-python && python3 -c "import pytest" && echo "pytest --junitxml is a built-in flag, no extra dep needed"`
Expected: prints the confirmation line (pytest ships `--junitxml`).

- [ ] **Step 3: Verify workflow lint**

Run: `actionlint .github/workflows/mvn-test.yml`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/mvn-test.yml
git commit -m "ci(#4891): emit + aggregate Python e2e JUnit report"
```

---

### Task 6: C# e2e - emit and aggregate JUnit

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/ArcadeDB.E2ETests.csproj`
- Modify: `.github/workflows/mvn-test.yml` (`csharp-e2e-tests` job, lines ~475-502)

- [ ] **Step 1: Add the JUnit logger package**

In `ArcadeDB.E2ETests.csproj`, add inside the existing `<ItemGroup>` that holds `PackageReference`s:
```xml
    <PackageReference Include="JunitXml.TestLogger" Version="8.0.0" />
```

- [ ] **Step 2: Emit JUnit + add reporter/artifact in CI**

In `csharp-e2e-tests`, replace the "E2E C# Tests" step and add reporter/artifact:
```yaml
      - name: E2E C# Tests
        working-directory: e2e-csharp/ArcadeDB.E2ETests
        run: dotnet test --logger "trx;LogFileName=test-results.trx" --logger "junit;LogFilePath=reports/csharp-junit.xml"
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          ARCADEDB_DOCKER_IMAGE: ${{ needs.build-and-package.outputs.image-tag }}

      - name: C# E2E Tests Reporter
        uses: dorny/test-reporter@a43b3a5f7366b97d083190328d2c652e1a8b6aa2 # v3.0.0
        if: success() || failure()
        with:
          name: C# E2E Tests Report
          path: "e2e-csharp/ArcadeDB.E2ETests/reports/csharp-junit.xml"
          list-suites: "failed"
          list-tests: "failed"
          reporter: java-junit

      - name: Upload C# E2E JUnit report
        if: success() || failure()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: csharp-e2e-junit
          path: e2e-csharp/ArcadeDB.E2ETests/reports/csharp-junit.xml
          retention-days: 7
```
Add `permissions: { contents: read, checks: write }` to the `csharp-e2e-tests` job.

- [ ] **Step 3: Verify the logger resolves and emits locally (if .NET SDK present)**

Run: `cd e2e-csharp/ArcadeDB.E2ETests && dotnet restore`
Expected: restore succeeds, `JunitXml.TestLogger` resolved. (If no local .NET SDK, rely on the CI trial run in Task 12.)

- [ ] **Step 4: Verify workflow lint**

Run: `actionlint .github/workflows/mvn-test.yml`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/ArcadeDB.E2ETests.csproj .github/workflows/mvn-test.yml
git commit -m "ci(#4891): emit + aggregate C# e2e JUnit report"
```

---

### Task 7: Go e2e - emit and aggregate JUnit via gotestsum

**Files:**
- Modify: `.github/workflows/mvn-test.yml` (`go-e2e-tests` job, lines ~504-542)

- [ ] **Step 1: Install gotestsum + emit JUnit + add reporter/artifact**

In `go-e2e-tests`, after "Verify keytool" and replacing the "E2E Go Tests" step:
```yaml
      - name: Install gotestsum
        run: go install gotest.tools/gotestsum@v1.12.0

      - name: E2E Go Tests
        working-directory: e2e-go
        run: |
          mkdir -p reports
          gotestsum --junitfile reports/go-junit.xml --format standard-verbose -- -timeout 20m ./...
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          ARCADEDB_DOCKER_IMAGE: ${{ needs.build-and-package.outputs.image-tag }}

      - name: Go E2E Tests Reporter
        uses: dorny/test-reporter@a43b3a5f7366b97d083190328d2c652e1a8b6aa2 # v3.0.0
        if: success() || failure()
        with:
          name: Go E2E Tests Report
          path: "e2e-go/reports/go-junit.xml"
          list-suites: "failed"
          list-tests: "failed"
          reporter: java-junit

      - name: Upload Go E2E JUnit report
        if: success() || failure()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: go-e2e-junit
          path: e2e-go/reports/go-junit.xml
          retention-days: 7
```
Note: `go install` adds gotestsum to `$(go env GOPATH)/bin`, already on `PATH` on `ubuntu-latest`. Add `permissions: { contents: read, checks: write }` to the `go-e2e-tests` job.

- [ ] **Step 2: Verify workflow lint**

Run: `actionlint .github/workflows/mvn-test.yml`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/mvn-test.yml
git commit -m "ci(#4891): emit + aggregate Go e2e JUnit report via gotestsum"
```

---

### Task 8: Resolve concrete driver versions and record them

**Files:**
- Create: `bolt/conformance/driver-versions.md` (the resolved matrix, cross-linked from `spec.yaml`)
- Modify: `bolt/conformance/spec.yaml` (append resolved versions to each band's `resolution_note`)

**Interfaces:**
- Produces: the concrete version lists consumed by Task 9's matrix.

- [ ] **Step 1: Query each registry for the current band members**

Run each and record the newest patch of the relevant minor line:
```bash
# JS (npm)
npm view neo4j-driver versions --json | python3 -c "import json,sys; vs=json.load(sys.stdin); print([v for v in vs if v.startswith(('4.4.','5.','6.'))][-15:])"
# Python (PyPI)
python3 -m pip index versions neo4j 2>/dev/null || curl -s https://pypi.org/pypi/neo4j/json | python3 -c "import json,sys; print(sorted(json.load(sys.stdin)['releases'])[-10:])"
# C# (NuGet)
curl -s https://api.nuget.org/v3-flatcontainer/neo4j.driver/index.json | python3 -c "import json,sys; print(json.load(sys.stdin)['versions'][-10:])"
# Go
cd e2e-go && go list -m -versions github.com/neo4j/neo4j-go-driver/v5
# Java (Maven Central)
curl -s "https://search.maven.org/solrsearch/select?q=g:org.neo4j.driver+AND+a:neo4j-java-driver&core=gav&rows=20&wt=json" | python3 -c "import json,sys; print([d['v'] for d in json.load(sys.stdin)['response']['docs']])"
```

- [ ] **Step 2: Write `driver-versions.md` with the resolved bands**

Fill in the exact versions returned above. The band-to-version mapping rule: `oldest-supported-4.x`/`lts` = newest patch of the oldest still-maintained line; `latest-5.x`/`current` = newest 5.x (or repo pin); `latest-6.x`/`latest` = newest 6.x. Java `oldest-supported-4.x` is fixed at `4.4.20` (the `bolt-driver-legacy` profile). Record as a table:
```markdown
# Resolved Bolt driver-version matrix (consumed by bolt-nightly.yml)

| Language | Band | Version |
|---|---|---|
| java   | oldest-supported-4.x | 4.4.20 |
| java   | latest-5.x           | <resolved 5.x> |
| java   | latest-6.x           | 6.2.0 |
| javascript | oldest-supported-4.x | <resolved 4.4.x> |
| javascript | latest-5.x           | <resolved 5.x> |
| javascript | latest-6.x           | <resolved 6.x> |
| python | lts     | <resolved> |
| python | current | 6.2.0 |
| python | latest  | <resolved newest> |
| csharp | lts     | <resolved> |
| csharp | current | 6.2.1 |
| csharp | latest  | <resolved newest> |
| go     | lts     | <resolved> |
| go     | current | 5.28.4 |
| go     | latest  | <resolved newest v5> |
```

- [ ] **Step 3: Cross-link from spec.yaml**

In `bolt/conformance/spec.yaml`, append to each band's `resolution_note`: `Concrete nightly versions: see bolt/conformance/driver-versions.md (#4891).`

- [ ] **Step 4: Validate the spec still passes its schema**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: validation passes.

- [ ] **Step 5: Commit**

```bash
git add bolt/conformance/driver-versions.md bolt/conformance/spec.yaml
git commit -m "docs(#4891): resolve and record concrete Bolt driver-version bands"
```

---

### Task 9: `bolt-nightly.yml` - build-image + five per-language matrix jobs

**Files:**
- Create: `.github/workflows/bolt-nightly.yml`

**Interfaces:**
- Consumes: `junit_to_matrix.py` (Task 1), resolved versions (Task 8), each suite's Bolt tests.
- Produces: per-cell artifacts `bolt-matrix-<lang>-<version>` each containing one normalized JSON.

- [ ] **Step 1: Scaffold triggers + build-image job**

Create `.github/workflows/bolt-nightly.yml` (NO license header per convention). Copy the exact `build-and-package` job body from `mvn-test.yml` (build the branch image, `docker save`, cache with a run-scoped key) into a `build-image` job. Header:
```yaml
name: Bolt nightly driver-version matrix

on:
  schedule:
    - cron: "0 3 * * *"
  workflow_dispatch:

permissions:
  contents: read

jobs:
  build-image:
    runs-on: ubuntu-latest
    steps:
      # ... identical to mvn-test.yml build-and-package: checkout, JDK 21,
      # QEMU, Buildx, mvnw clean install -Pdocker -DskipTests, docker save,
      # cache/save /tmp/arcadedb-image.tar under key
      # docker-image-${{ github.run_id }}-${{ github.run_attempt }}
```

- [ ] **Step 2: Add the five per-language matrix jobs**

Each `needs: build-image`, restores + loads the image, overrides the driver version, runs only the Bolt suite, then converts JUnit to JSON. Example (`bolt-nightly-python`); replicate the shape for java/js/csharp/go with that language's override + Bolt-only test invocation from Tasks 4-7 and the versions from Task 8:
```yaml
  bolt-nightly-python:
    needs: build-image
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        driver-version: ["<lts>", "6.2.0", "<latest>"]
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0
      - uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6.3.0
        with:
          python-version: "3.13.0"
      - name: Restore Docker image
        uses: actions/cache/restore@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: /tmp/arcadedb-image.tar
          key: docker-image-${{ github.run_id }}-${{ github.run_attempt }}
      - name: Load Docker image
        run: docker load < /tmp/arcadedb-image.tar
      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh
      - name: Run Bolt suite against pinned driver
        working-directory: e2e-python
        run: |
          uv pip install --system -e .
          uv pip install --system pytest "neo4j==${{ matrix.driver-version }}"
          mkdir -p reports
          pytest tests/test_bolt.py --junitxml=reports/python-junit.xml
        env:
          ARCADEDB_DOCKER_IMAGE: arcadedata/arcadedb:latest
      - name: Convert to matrix JSON
        if: success() || failure()
        run: |
          python3 bolt/conformance/tools/junit_to_matrix.py \
            e2e-python/reports/python-junit.xml \
            --language python --driver-version "${{ matrix.driver-version }}" \
            --spec bolt/conformance/spec.yaml \
            -o bolt-matrix-python-${{ matrix.driver-version }}.json
      - name: Upload cell result
        if: success() || failure()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: bolt-matrix-python-${{ matrix.driver-version }}
          path: bolt-matrix-python-*.json
          retention-days: 7
```
Per-language override + Bolt-only invocation:
- java: `./mvnw verify -Pintegration -pl e2e -Dtest=RemoteBoltDatabaseIT -Dneo4j-driver.version=${{ matrix.driver-version }}` (and the `4.4.20` cell adds `-Pbolt-driver-legacy -Dtest=RemoteBoltLegacyDriverIT`). Needs Maven artifacts restore like `java-e2e-tests`.
- js: `npm install neo4j-driver@${{ matrix.driver-version }} && npx jest src/js-bolt-conformance.test.js` (jest-junit already writes `reports/jest-junit.xml`).
- csharp: `dotnet add package Neo4j.Driver --version ${{ matrix.driver-version }} && dotnet test --filter FullyQualifiedName~Bolt --logger "junit;LogFilePath=reports/csharp-junit.xml"`.
- go: `go get github.com/neo4j/neo4j-go-driver/v5@v${{ matrix.driver-version }} && go mod tidy && gotestsum --junitfile reports/go-junit.xml -- -timeout 20m -run 'Bolt|Conformance' ./...`.

- [ ] **Step 3: Verify workflow lint**

Run: `actionlint .github/workflows/bolt-nightly.yml`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/bolt-nightly.yml
git commit -m "ci(#4891): bolt-nightly workflow - image build + per-language driver matrix"
```

---

### Task 10: `bolt-nightly.yml` - merge job producing `bolt-compat-matrix.json`

**Files:**
- Modify: `.github/workflows/bolt-nightly.yml`

**Interfaces:**
- Consumes: all `bolt-matrix-*` cell artifacts.
- Produces: `bolt-compat-matrix` artifact (single `bolt-compat-matrix.json`) - the input #4892 consumes.

- [ ] **Step 1: Add the merge-matrix job**

```yaml
  merge-matrix:
    needs: [bolt-nightly-java, bolt-nightly-js, bolt-nightly-python, bolt-nightly-csharp, bolt-nightly-go]
    if: ${{ always() }}
    runs-on: ubuntu-latest
    outputs:
      has-failures: ${{ steps.merge.outputs.has-failures }}
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0
      - uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6.3.0
        with:
          python-version: "3.12"
      - name: Download all cell artifacts
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        with:
          path: cells
          pattern: bolt-matrix-*
          merge-multiple: true
      - name: Merge into compatibility matrix
        id: merge
        run: |
          python3 bolt/conformance/tools/merge_matrix.py cells/*.json -o bolt-compat-matrix.json
          echo "has-failures=$(python3 -c "import json;print(str(json.load(open('bolt-compat-matrix.json'))['has_failures']).lower())")" >> "$GITHUB_OUTPUT"
      - name: Upload compatibility matrix
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: bolt-compat-matrix
          path: bolt-compat-matrix.json
          retention-days: 30
```

- [ ] **Step 2: Verify workflow lint**

Run: `actionlint .github/workflows/bolt-nightly.yml`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/bolt-nightly.yml
git commit -m "ci(#4891): merge nightly cell results into bolt-compat-matrix.json"
```

---

### Task 11: `bolt-nightly.yml` - auto-open/update regression tracking issue

**Files:**
- Modify: `.github/workflows/bolt-nightly.yml`

**Interfaces:**
- Consumes: `merge-matrix.outputs.has-failures` and the `bolt-compat-matrix` artifact.

- [ ] **Step 1: Add the report job**

```yaml
  report-regression:
    needs: merge-matrix
    if: ${{ always() && needs.merge-matrix.outputs.has-failures == 'true' }}
    runs-on: ubuntu-latest
    permissions:
      contents: read
      issues: write
    steps:
      - uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        with:
          name: bolt-compat-matrix
      - name: Open or update regression issue
        uses: actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1
        with:
          script: |
            const fs = require('fs');
            const matrix = JSON.parse(fs.readFileSync('bolt-compat-matrix.json', 'utf8'));
            const failing = [];
            for (const [scen, langs] of Object.entries(matrix.scenarios)) {
              for (const [lang, vers] of Object.entries(langs)) {
                for (const [ver, status] of Object.entries(vers)) {
                  if (status === 'fail') failing.push(`- ${scen} / ${lang} / ${ver}`);
                }
              }
            }
            const title = 'Bolt driver compatibility regression (nightly)';
            const label = 'bolt-compat-regression';
            const runUrl = `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`;
            const body = `Nightly Bolt matrix reported failing cells.\n\nRun: ${runUrl}\n\n${failing.sort().join('\n')}`;
            const existing = await github.rest.issues.listForRepo({
              owner: context.repo.owner, repo: context.repo.repo,
              state: 'open', labels: label,
            });
            const hit = existing.data.find(i => i.title === title);
            if (hit) {
              await github.rest.issues.update({ owner: context.repo.owner, repo: context.repo.repo, issue_number: hit.number, body });
            } else {
              await github.rest.issues.create({ owner: context.repo.owner, repo: context.repo.repo, title, body, labels: [label] });
            }
```

- [ ] **Step 2: Ensure the label exists**

Run: `gh label create bolt-compat-regression --color B60205 --description "Nightly Bolt driver compatibility regression" --force`
Expected: label created or updated (github-script `labels: [label]` also creates it on first issue if missing, but pre-creating gives it a stable color).

- [ ] **Step 3: Verify workflow lint**

Run: `actionlint .github/workflows/bolt-nightly.yml`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/bolt-nightly.yml
git commit -m "ci(#4891): open/update bolt-compat-regression issue on nightly failure"
```

---

### Task 12: End-to-end verification via workflow_dispatch trial

**Files:** none (verification only)

- [ ] **Step 1: Lint every touched workflow**

Run: `actionlint .github/workflows/mvn-test.yml .github/workflows/bolt-nightly.yml .github/workflows/bolt-conformance-spec.yml`
Expected: no output.

- [ ] **Step 2: Run the full conformance-tools test suite once more**

Run: `cd bolt/conformance/tools && python3 -m unittest discover -p 'test_*.py' -v`
Expected: all tests PASS (7 total).

- [ ] **Step 3: Dry-run the matrix tools against real JUnit locally**

Produce a JUnit from one suite (e.g. `cd e2e-js && npx jest src/js-bolt-conformance.test.js` if Docker is available), then:
```bash
python3 bolt/conformance/tools/junit_to_matrix.py e2e-js/reports/jest-junit.xml --language javascript --driver-version 6.0.1 --spec bolt/conformance/spec.yaml -o /tmp/cell.json
python3 bolt/conformance/tools/merge_matrix.py /tmp/cell.json -o /tmp/matrix.json
python3 -m json.tool /tmp/matrix.json
```
Expected: `/tmp/matrix.json` shows `scenarios -> javascript -> 6.0.1 -> status` and a `has_failures` boolean. (If Docker is unavailable locally, defer to Step 4.)

- [ ] **Step 4: Push branch and trigger the nightly manually**

After the PR branch is pushed:
```bash
gh workflow run bolt-nightly.yml --ref feat/4891-bolt-ci-nightly
gh run watch
```
Expected: `build-image` succeeds, all five matrix jobs run (some cells may legitimately fail on known-gap scenarios - that is the point), `merge-matrix` uploads `bolt-compat-matrix.json`, and `report-regression` opens/updates the tracking issue if any cell failed.

- [ ] **Step 5: Final commit if any fixups were needed**

```bash
git add -A && git commit -m "ci(#4891): fixups from nightly trial run" || echo "no fixups needed"
```

---

## Self-Review

**Spec coverage:**
- Component 1 (JUnit aggregation JS/Python/C#/Go) -> Tasks 4-7. ✓
- Component 2 (`bolt-nightly.yml` + matrix) -> Tasks 8-9. ✓
- Component 3 (normalized JSON + merge) -> Tasks 1-2, 10. ✓
- Component 4 (auto-issue) -> Task 11. ✓
- Verification approach (actionlint, unit tests, dispatch trial) -> Tasks 3, 12. ✓
- All four acceptance criteria map to tasks (PR-gating verified in Task 12 Step 4; aggregation Tasks 4-7; nightly Tasks 9-11; matrix JSON Task 10). ✓

**Placeholder scan:** The `<resolved ...>` markers in Task 8 are intentional - that task's job is to resolve them via the given registry commands; every other task has concrete code/commands.

**Type consistency:** `parse_junit` / `build_matrix` / `load_known_ids` (Task 1) and `merge` (Task 2) signatures match their uses in Tasks 9-11. JSON shapes are consistent: cell = `{language, driver_version, scenarios}`; merged = `{scenarios, languages, has_failures}`. Artifact names (`bolt-matrix-<lang>-<version>`, `bolt-compat-matrix`) are consistent between producer (Task 9/10) and consumer (Task 10/11).
