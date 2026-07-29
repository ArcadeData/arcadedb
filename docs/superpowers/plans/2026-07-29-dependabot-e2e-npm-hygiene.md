# Dependabot E2E npm Hygiene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clear all 6 open Dependabot alerts (all dev-scoped npm transitives in the E2E harnesses) and close the process gaps that let them accumulate, without touching any shipped Java, Studio, or Docker artifact.

**Architecture:** Three independent changes on one branch. Tier 1 is a pure lockfile refresh in `e2e-js/` and `e2e-studio/` that stays inside existing semver ranges, so no `package.json` changes. Tier 2 makes the committed lockfile authoritative in CI (`npm install` -> `npm ci`), groups the two E2E npm ecosystems in `dependabot.yml` so transitive bumps stop consuming one PR slot each, and adds a focused audit workflow that blocks only on runtime-scope findings while reporting dev-scope findings informationally. Tier 3 (stale Maven PRs #3235 / #4969) is explicitly **out of scope** for this branch by decision.

**Tech Stack:** npm 10+ lockfile v3, GitHub Actions, Dependabot v2 config.

## Global Constraints

- **Branch:** `chore/deps-e2e-npm-hygiene`, worktree `.worktrees/chore/deps-e2e-npm-hygiene`, based on `origin/main` at `5b4fd077c`.
- **No `package.json` edits.** Every dependency change must fit existing semver ranges and land in `package-lock.json` only. If a fix would require a `package.json` change, stop and report instead.
- **No Java, no `pom.xml`, no `studio/` changes.** This branch touches only `e2e-js/`, `e2e-studio/`, `.github/dependabot.yml`, `.github/workflows/`, and `docs/`.
- **Do not chase `npm audit` to zero.** `brace-expansion` GHSA-mh99-v99m-4gvg / CVE-2026-14257 is vulnerable at `<= 5.0.7` and patched only in `5.0.8`. There is no 2.x backport and `minimatch` 5/9 pin `^2.x`. npm will suggest "fixing" it by downgrading to `jest@25` / `testcontainers@7`; that is a five-major downgrade and is forbidden. GitHub already auto-dismissed it (alerts #156, #158).
- **Never run `npm audit fix --force`** in this repo. Plain `npm audit fix` only.
- **Pin GitHub Actions by commit SHA** with a trailing `# vX.Y.Z` comment, matching the existing style in `.github/workflows/`.
- **Node versions are per-project and must not be unified:** `e2e-js` requires Node **24** (testcontainers@12 pulls archiver@8, pure ESM; Jest's `require(ESM)` bridge needs Node 24.9+). `e2e-studio` uses Node **22**.
- **Commit style:** Conventional Commits, matching repo history (`chore(deps):`, `ci:`, `docs:`). Do not add Claude as an author or co-author.
- **Do not run `git commit` on the user's behalf beyond the per-task commits specified here, and never push or open a PR without being asked.**

### The 6 alerts this branch must clear

| # | Sev | Package | CVE | Lockfile | Vulnerable | Patched |
|---|-----|---------|-----|----------|-----------|---------|
| 157 | high | brace-expansion | CVE-2026-13149 | e2e-studio | >=2.0.0 <2.1.2 | 2.1.2 |
| 154 | high | brace-expansion | CVE-2026-13149 | e2e-js | >=2.0.0 <2.1.2 | 2.1.2 |
| 153 | high | brace-expansion | CVE-2026-13149 | e2e-js | <1.1.16 | 1.1.16 |
| 155 | high | js-yaml | CVE-2026-59869 | e2e-js | >=3.0.0 <3.15.0 | 3.15.0 |
| 150 | medium | js-yaml | CVE-2026-53550 | e2e-js | <3.15.0 | 3.15.0 |
| 139 | low | @babel/core | CVE-2026-49356 | e2e-js | <=7.29.0 | 7.29.6 |

---

### Task 1: Refresh the E2E lockfiles (Tier 1)

Clears all 6 alerts. Verified in advance: this is a lockfile-only change in both projects.

**Files:**
- Modify: `e2e-js/package-lock.json`
- Modify: `e2e-studio/package-lock.json`
- Must remain unchanged: `e2e-js/package.json`, `e2e-studio/package.json`

**Interfaces:**
- Consumes: nothing.
- Produces: lockfiles where `brace-expansion >= 2.1.2` (and the nested `test-exclude` copy `>= 1.1.16`), `js-yaml >= 3.15.0`, `@babel/core >= 7.29.6`. Task 3 and Task 4 depend on these lockfiles being installable with `npm ci`.

- [ ] **Step 1: Record the "before" state so the fix is provable**

```bash
cd e2e-js
npm audit --package-lock-only --json > /tmp/e2e-js-audit-before.json
python3 - <<'PY'
import json
d = json.load(open('/tmp/e2e-js-audit-before.json'))
print('e2e-js before:', d['metadata']['vulnerabilities'])
PY
```

Expected: `high` is non-zero and `js-yaml`, `@babel/core`, `brace-expansion` all appear.

- [ ] **Step 2: Assert the vulnerable versions are actually present (the failing check)**

```bash
cd e2e-js
python3 - <<'PY'
import json, sys
lock = json.load(open('package-lock.json'))
want = {
    'node_modules/brace-expansion': '2.0.2',
    'node_modules/test-exclude/node_modules/brace-expansion': '1.1.14',
    'node_modules/js-yaml': '3.14.2',
    'node_modules/@babel/core': '7.29.0',
}
bad = [(k, v, lock['packages'][k]['version']) for k, v in want.items()
       if lock['packages'][k]['version'] != v]
print('MISMATCH' if bad else 'CONFIRMED vulnerable baseline', bad)
PY
```

Expected: `CONFIRMED vulnerable baseline []`. If it prints `MISMATCH`, the base branch moved; stop and re-derive the alert list before continuing.

- [ ] **Step 3: Apply the lockfile-only fix in `e2e-js`**

```bash
cd e2e-js
npm audit fix --package-lock-only
```

Do **not** pass `--force`.

- [ ] **Step 4: Verify `e2e-js` reached the patched versions and `package.json` is untouched**

```bash
cd e2e-js
git diff --quiet package.json && echo "package.json UNCHANGED: OK" || { echo "FAIL: package.json changed"; git diff package.json; }
python3 - <<'PY'
import json
lock = json.load(open('package-lock.json'))
floors = {
    'node_modules/brace-expansion': (2, 1, 2),
    'node_modules/test-exclude/node_modules/brace-expansion': (1, 1, 16),
    'node_modules/js-yaml': (3, 15, 0),
    'node_modules/@babel/core': (7, 29, 6),
}
ok = True
for path, floor in floors.items():
    got = lock['packages'][path]['version']
    parsed = tuple(int(x) for x in got.split('-')[0].split('.'))
    status = 'OK' if parsed >= floor else 'FAIL'
    ok &= parsed >= floor
    print(f'{status:4} {path} = {got} (need >= {".".join(map(str, floor))})')
print('ALL PATCHED' if ok else 'NOT PATCHED')
PY
```

Expected: `package.json UNCHANGED: OK`, four `OK` lines, and `ALL PATCHED`.

- [ ] **Step 5: Apply and verify the same fix in `e2e-studio`**

```bash
cd e2e-studio
npm audit fix --package-lock-only
git diff --quiet package.json && echo "package.json UNCHANGED: OK" || { echo "FAIL: package.json changed"; git diff package.json; }
python3 - <<'PY'
import json
lock = json.load(open('package-lock.json'))
got = lock['packages']['node_modules/brace-expansion']['version']
parsed = tuple(int(x) for x in got.split('-')[0].split('.'))
print(('OK' if parsed >= (2, 1, 2) else 'FAIL'), 'brace-expansion =', got, '(need >= 2.1.2)')
PY
```

Expected: `package.json UNCHANGED: OK` and `OK brace-expansion = 2.1.3 (need >= 2.1.2)`.

- [ ] **Step 6: Confirm the residual audit noise is only the unfixable advisory**

```bash
for d in e2e-js e2e-studio; do
  echo "=== $d residual ==="
  (cd $d && npm audit --package-lock-only --json 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
names = sorted(d.get('vulnerabilities', {}))
print(' total:', d['metadata']['vulnerabilities'])
for n in names:
    print('  -', n, d['vulnerabilities'][n]['severity'])
")
done
```

Expected: residual entries are limited to the `brace-expansion` / `minimatch` / `glob` / `archiver` / `readdir-glob` / `zip-stream` / `jest*` / `test-exclude` / `testcontainers` family, all tracing to CVE-2026-14257. **`js-yaml` and `@babel/core` must be gone entirely.** If either still appears, the fix did not take; stop and investigate.

- [ ] **Step 7: Confirm runtime scope is clean**

```bash
for d in e2e-js e2e-studio; do
  (cd $d && echo -n "$d prod-scope: " && npm audit --omit=dev --package-lock-only --json 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['metadata']['vulnerabilities'])")
done
```

Expected: both report all-zero. This is the invariant Task 4's blocking gate will enforce.

- [ ] **Step 8: Commit**

```bash
git add e2e-js/package-lock.json e2e-studio/package-lock.json
git commit -m "chore(deps): patch dev-scoped npm transitives in the E2E harnesses

Refreshes the e2e-js and e2e-studio lockfiles to clear six Dependabot
alerts, all development-scope transitive dependencies:

  brace-expansion 2.0.2 -> 2.1.3, nested 1.1.14 -> 1.1.16 (CVE-2026-13149)
  js-yaml         3.14.2 -> 3.15.0 (CVE-2026-59869, CVE-2026-53550)
  @babel/core     7.29.0 -> 7.29.7 (CVE-2026-49356)

Every bump fits an existing semver range, so no package.json changes and
no major upgrades. Runtime-scope dependencies were already clean and stay
clean.

brace-expansion CVE-2026-14257 remains reported by npm audit. It is
vulnerable at <= 5.0.7 and patched only in 5.0.8, with no 2.x backport,
while minimatch 5/9 pin ^2.x. It is not fixable without downgrading
testcontainers and jest by several majors, and GitHub auto-dismissed it."
```

---

### Task 2: Group the E2E npm ecosystems in Dependabot config (Tier 2a)

`/e2e-js` and `/e2e-studio` are the only npm entries with no `groups:` block, so each transitive bump burns one of 10 PR slots. `/studio` and `/e2e-go` already demonstrate the pattern to follow.

**Files:**
- Modify: `.github/dependabot.yml` (the `/e2e-js` and `/e2e-studio` npm entries near the end of the file)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Read the current entries**

```bash
grep -n -A6 'directory: "/e2e-js"' .github/dependabot.yml
grep -n -A6 'directory: "/e2e-studio"' .github/dependabot.yml
```

Expected: each is 6 lines with `schedule` and `open-pull-requests-limit: 10`, and no `groups:`.

- [ ] **Step 2: Replace the `/e2e-js` entry**

Find this exact block:

```yaml
  # E2E JavaScript Testing Dependencies
  - package-ecosystem: "npm"
    directory: "/e2e-js"
    schedule:
      interval: weekly
      day: "sunday"
    open-pull-requests-limit: 10
```

Replace it with:

```yaml
  # E2E JavaScript Testing Dependencies
  #
  # Everything here is test-harness only and never ships in a distribution.
  # Jest and testcontainers drag in a wide transitive tree (glob, minimatch,
  # brace-expansion, babel), so ungrouped patch bumps exhaust the PR limit and
  # bury the updates that matter. Group minor/patch into one PR per week and
  # let majors open individually, matching the /e2e-go convention.
  - package-ecosystem: "npm"
    directory: "/e2e-js"
    schedule:
      interval: weekly
      day: "sunday"
    open-pull-requests-limit: 10
    groups:
      e2e-js-deps:
        patterns:
          - "*"
        update-types:
          - "minor"
          - "patch"
```

- [ ] **Step 3: Replace the `/e2e-studio` entry**

Find this exact block:

```yaml
  # E2E Studio Testing Dependencies
  - package-ecosystem: "npm"
    directory: "/e2e-studio"
    schedule:
      interval: weekly
      day: "sunday"
    open-pull-requests-limit: 10
```

Replace it with:

```yaml
  # E2E Studio Testing Dependencies
  #
  # Playwright and testcontainers only; test-harness scope, never shipped.
  # Grouped for the same reason as /e2e-js above.
  - package-ecosystem: "npm"
    directory: "/e2e-studio"
    schedule:
      interval: weekly
      day: "sunday"
    open-pull-requests-limit: 10
    groups:
      e2e-studio-deps:
        patterns:
          - "*"
        update-types:
          - "minor"
          - "patch"
```

- [ ] **Step 4: Record the auto-triage gap where a maintainer will find it**

Dependabot has no config key for auto-triage rules, so this must be a comment. Insert it immediately after the `version: 2` line at the top of `.github/dependabot.yml`:

```yaml
version: 2

# Alert triage note
# -----------------
# The repository's Dependabot auto-triage rule dismisses development-scope
# alerts for CWE-400 / CWE-770 / CWE-835 / CWE-674 (resource exhaustion), but
# NOT CWE-407 (inefficient algorithmic complexity) or CWE-22. That gap is why
# ReDoS-class advisories against test-only packages such as brace-expansion and
# js-yaml still surface as open alerts even though they are unreachable from any
# shipped artifact. Widening the rule to include CWE-407 for development scope
# is a Security -> Dependabot -> auto-triage rules setting in the GitHub UI and
# cannot be expressed in this file.

updates:
```

- [ ] **Step 5: Validate the YAML parses and the groups are registered**

```bash
python3 - <<'PY'
import yaml
cfg = yaml.safe_load(open('.github/dependabot.yml'))
assert cfg['version'] == 2, cfg['version']
for u in cfg['updates']:
    if u['directory'] in ('/e2e-js', '/e2e-studio') and u['package-ecosystem'] == 'npm':
        print(u['directory'], '-> groups:', list(u.get('groups', {})) or 'MISSING')
npm = [u['directory'] for u in cfg['updates'] if u['package-ecosystem'] == 'npm']
print('npm dirs:', npm)
print('ungrouped npm:', [u['directory'] for u in cfg['updates']
                         if u['package-ecosystem'] == 'npm' and not u.get('groups')])
PY
```

Expected: `/e2e-js -> groups: ['e2e-js-deps']`, `/e2e-studio -> groups: ['e2e-studio-deps']`, and `ungrouped npm: []`.

- [ ] **Step 6: Commit**

```bash
git add .github/dependabot.yml
git commit -m "ci: group E2E npm updates and record the auto-triage CWE gap

/e2e-js and /e2e-studio were the only npm ecosystems without a groups
block, so every transitive patch bump consumed one of ten PR slots. Group
minor/patch into a single weekly PR per directory, matching the /e2e-go
and /studio conventions, and leave majors opening individually.

Also documents why ReDoS-class advisories against test-only packages keep
surfacing: the auto-triage rule covers CWE-400/770/835/674 for development
scope but not CWE-407. That rule lives in the GitHub UI, not in this file."
```

---

### Task 3: Make the committed lockfile authoritative in CI (Tier 2b)

Both E2E jobs run `npm install`, which is free to re-resolve and drift from the committed lockfile. Dependabot scans the lockfile, so as long as CI ignores it, pinning a patched version proves nothing about what CI actually ran. Both jobs already declare `cache-dependency-path` pointing at the lockfile, so `npm ci` is the intended pattern.

**Files:**
- Modify: `.github/workflows/mvn-test.yml` (job `js-e2e-tests`, the `E2E Node.js Tests` step; job `studio-e2e-tests`, the `Install Playwright Browsers` step)

**Interfaces:**
- Consumes: the patched lockfiles from Task 1. `npm ci` hard-fails if `package.json` and `package-lock.json` disagree, so Task 1 must land first.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Prove `npm ci` succeeds against the patched lockfiles before changing CI**

This is the failing-check-first step: if `npm ci` cannot install these lockfiles, the CI change would break the build.

```bash
for d in e2e-js e2e-studio; do
  echo "=== $d npm ci ==="
  (cd $d && rm -rf node_modules && npm ci >/dev/null 2>&1 && echo "npm ci OK" || echo "npm ci FAILED")
done
```

Expected: `npm ci OK` for both. If either fails, `package.json` and the lockfile are out of sync; re-run `npm install --package-lock-only` in that directory and re-verify Task 1 Step 4 before proceeding.

- [ ] **Step 2: Switch `js-e2e-tests` to `npm ci`**

In `.github/workflows/mvn-test.yml`, find:

```yaml
      - name: E2E Node.js Tests
        working-directory: e2e-js
        run: |
          npm install
          npm test
```

Replace with:

```yaml
      - name: E2E Node.js Tests
        working-directory: e2e-js
        run: |
          npm ci
          npm test
```

- [ ] **Step 3: Switch `studio-e2e-tests` to `npm ci`**

Find:

```yaml
      - name: Install Playwright Browsers
        working-directory: e2e-studio
        run: |
          npm install
          npm run install-browsers
```

Replace with:

```yaml
      - name: Install Playwright Browsers
        working-directory: e2e-studio
        run: |
          npm ci
          npm run install-browsers
```

- [ ] **Step 4: Verify no `npm install` remains in the E2E jobs and the YAML still parses**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/mvn-test.yml')); print('mvn-test.yml parses OK')"
echo "--- remaining npm install in mvn-test.yml ---"
grep -n "npm install" .github/workflows/mvn-test.yml || echo "none"
echo "--- npm ci occurrences ---"
grep -n "npm ci" .github/workflows/mvn-test.yml
```

Expected: parses OK, `none` for `npm install`, and exactly two `npm ci` lines.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/mvn-test.yml
git commit -m "ci: install E2E dependencies with npm ci instead of npm install

Both E2E jobs ran npm install, which re-resolves and may drift from the
committed lockfile. Dependabot scans the lockfile, so pinning a patched
transitive there proved nothing about what CI actually installed.

npm ci installs the lockfile exactly and fails loudly when it disagrees
with package.json. Both jobs already set cache-dependency-path to the
lockfile, so this is the pattern they were written for."
```

---

### Task 4: Add a focused E2E dependency audit workflow (Tier 2c)

A blocking full `npm audit` would be red forever because of the unfixable CVE-2026-14257. Split the gate by scope instead: **fail** on runtime-scope findings (`--omit=dev`, currently zero in both projects, so the gate starts green and stays meaningful), and **report** dev-scope findings to the job summary without failing.

**Files:**
- Create: `.github/workflows/e2e-dependency-audit.yml`

**Interfaces:**
- Consumes: the patched lockfiles from Task 1 (`npm ci` must succeed, per Task 3 Step 1) and the runtime-clean invariant proven in Task 1 Step 7.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Create the workflow**

Action SHAs are copied verbatim from the existing pinned versions in `.github/workflows/mvn-test.yml`.

```yaml
# E2E harness dependency audit
#
# The e2e-js and e2e-studio projects are test harnesses; nothing they install
# ships in an ArcadeDB distribution. Their dependency risk is therefore split
# in two, and this workflow treats the halves differently:
#
#   runtime scope (--omit=dev)  -> BLOCKING. Currently zero findings in both
#                                  projects, so the gate is meaningful rather
#                                  than decorative.
#   development scope           -> REPORTED to the job summary, never blocking.
#
# A blocking gate over the full tree would be permanently red: brace-expansion
# CVE-2026-14257 is vulnerable at <= 5.0.7 and patched only in 5.0.8, there is
# no 2.x backport, and minimatch 5/9 pin ^2.x. npm audit "fixes" it by
# proposing jest@25 and testcontainers@7, a five-major downgrade. Do not take
# that suggestion, and never run npm audit fix --force here.

name: "E2E Dependency Audit"

on:
  push:
    branches: [ main ]
    paths:
      - "e2e-js/package*.json"
      - "e2e-studio/package*.json"
      - ".github/workflows/e2e-dependency-audit.yml"
  pull_request:
    branches: [ main ]
    paths:
      - "e2e-js/package*.json"
      - "e2e-studio/package*.json"
      - ".github/workflows/e2e-dependency-audit.yml"
  schedule:
    - cron: "0 6 * * 1"  # Every Monday at 6 AM UTC
  workflow_dispatch:

jobs:
  audit:
    name: "Audit ${{ matrix.project }}"
    runs-on: ubuntu-latest
    permissions:
      contents: read

    strategy:
      fail-fast: false
      matrix:
        include:
          # Node 24.9+ required: testcontainers@12 pulls archiver@8 (pure ESM),
          # and Jest's require(ESM) bridge only works on Node 24.9+.
          - project: e2e-js
            node-version: "24"
          - project: e2e-studio
            node-version: "22"

    defaults:
      run:
        working-directory: ${{ matrix.project }}

    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1

      - name: Set up Node
        uses: actions/setup-node@820762786026740c76f36085b0efc47a31fe5020 # v7.0.0
        with:
          node-version: ${{ matrix.node-version }}
          cache: "npm"
          cache-dependency-path: "${{ matrix.project }}/package-lock.json"

      - name: Verify the lockfile is installable and in sync
        run: npm ci --ignore-scripts

      - name: Audit runtime-scope dependencies (blocking)
        run: |
          echo "Auditing runtime-scope dependencies of ${{ matrix.project }}."
          npm audit --omit=dev --audit-level=high

      - name: Report development-scope findings (non-blocking)
        if: always()
        run: |
          npm audit --json > audit.json || true
          python3 - <<'PY' >> "$GITHUB_STEP_SUMMARY"
          import json, os
          project = os.environ["PROJECT"]
          with open("audit.json") as fh:
              data = json.load(fh)
          counts = data.get("metadata", {}).get("vulnerabilities", {})
          print(f"### {project}: development-scope audit\n")
          order = ("critical", "high", "moderate", "low", "info")
          print("| severity | count |")
          print("| --- | --- |")
          for key in order:
              print(f"| {key} | {counts.get(key, 0)} |")
          vulns = data.get("vulnerabilities", {})
          if vulns:
              print("\n<details><summary>Affected packages</summary>\n")
              for name in sorted(vulns):
                  print(f"- `{name}` ({vulns[name]['severity']})")
              print("\n</details>")
          print(
              "\nDevelopment-scope findings do not fail this job. These packages "
              "run only in CI against inputs the repository controls. See the "
              "header of `.github/workflows/e2e-dependency-audit.yml` for the "
              "unfixable brace-expansion advisory.\n"
          )
          PY
        env:
          PROJECT: ${{ matrix.project }}
```

- [ ] **Step 2: Verify the workflow YAML parses and the matrix is correct**

```bash
python3 - <<'PY'
import yaml
wf = yaml.safe_load(open('.github/workflows/e2e-dependency-audit.yml'))
print('name:', wf['name'])
# PyYAML parses the `on:` key as boolean True
trig = wf.get('on') or wf.get(True)
print('triggers:', sorted(trig))
job = wf['jobs']['audit']
print('matrix:', job['strategy']['matrix']['include'])
steps = [s.get('name', s.get('uses')) for s in job['steps']]
print('steps:')
for s in steps:
    print('  -', s)
PY
```

Expected: two matrix entries (`e2e-js`/24, `e2e-studio`/22) and five steps ending with the blocking runtime audit followed by the non-blocking report.

- [ ] **Step 3: Confirm the blocking gate would pass today**

This reproduces exactly what the blocking step runs, so a red CI run is not the first time anyone finds out.

```bash
for d in e2e-js e2e-studio; do
  echo -n "$d blocking gate: "
  (cd $d && npm audit --omit=dev --audit-level=high >/dev/null 2>&1 && echo PASS || echo FAIL)
done
```

Expected: `PASS` for both. A `FAIL` here means the gate would break `main` on merge; do not commit until it passes.

- [ ] **Step 4: Confirm no stray files are staged**

`npm ci` in Step 1 of Task 3 and the audit runs create `node_modules/` and possibly `audit.json` locally.

```bash
git status --porcelain
```

Expected: only `.github/workflows/e2e-dependency-audit.yml` as untracked. If `node_modules/` or `audit.json` appear, confirm they are ignored (`git check-ignore -v e2e-js/node_modules`) and remove any stray `audit.json` before committing.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/e2e-dependency-audit.yml
git commit -m "ci: audit E2E harness dependencies by scope

Adds a weekly and path-triggered audit for e2e-js and e2e-studio that
splits the gate by dependency scope. Runtime-scope findings fail the job
and are currently zero in both projects, so the gate is meaningful.
Development-scope findings are written to the job summary and never fail.

A blocking gate over the full tree would be permanently red: the
brace-expansion advisory CVE-2026-14257 is patched only in 5.0.8, has no
2.x backport, and minimatch pins ^2.x. npm audit proposes escaping it by
downgrading jest and testcontainers by several majors, which the workflow
header explicitly warns against.

The job also runs npm ci, so a lockfile that drifts out of sync with
package.json fails here rather than inside a full E2E run."
```

---

### Task 5: Verify the branch end to end

**Files:**
- No changes. Verification only.

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: the evidence to report.

- [ ] **Step 1: Confirm the diff is confined to the allowed surface**

```bash
git diff --stat origin/main...HEAD
echo "--- forbidden paths touched? ---"
git diff --name-only origin/main...HEAD | grep -E '\.java$|pom\.xml$|^studio/' && echo "FAIL: out-of-scope file touched" || echo "OK: no Java, pom.xml, or studio/ changes"
echo "--- package.json touched? ---"
git diff --name-only origin/main...HEAD | grep -E 'package\.json$' && echo "FAIL: package.json changed" || echo "OK: no package.json changes"
```

Expected: 5 files changed (2 lockfiles, `dependabot.yml`, `mvn-test.yml`, the new workflow), plus this plan document. Both checks print `OK`.

- [ ] **Step 2: Re-run the full alert verification from a clean install**

```bash
for d in e2e-js e2e-studio; do
  echo "=== $d ==="
  (cd $d && rm -rf node_modules && npm ci >/dev/null 2>&1 && echo "  npm ci: OK" || echo "  npm ci: FAILED")
  (cd $d && echo -n "  runtime scope: " && npm audit --omit=dev --json 2>/dev/null \
     | python3 -c "import json,sys; print(json.load(sys.stdin)['metadata']['vulnerabilities'])")
  (cd $d && npm ls js-yaml @babel/core 2>/dev/null | grep -E 'js-yaml@|@babel/core@' || true)
done
```

Expected: `npm ci: OK` and all-zero runtime scope for both. Any `js-yaml@` line must show `3.15.0` or higher and any `@babel/core@` line `7.29.6` or higher.

- [ ] **Step 3: Confirm every YAML the branch touched still parses**

```bash
for f in .github/dependabot.yml .github/workflows/mvn-test.yml .github/workflows/e2e-dependency-audit.yml; do
  python3 -c "import yaml,sys; yaml.safe_load(open('$f')); print('OK  $f')" || echo "FAIL $f"
done
```

Expected: three `OK` lines.

- [ ] **Step 4: Commit the plan document**

```bash
git add docs/superpowers/plans/2026-07-29-dependabot-e2e-npm-hygiene.md
git commit -m "docs: plan for E2E npm dependency hygiene"
```

- [ ] **Step 5: Report, do not push**

Summarize: which alerts clear, that runtime scope is clean, that CVE-2026-14257 is knowingly left open and why, and the one manual step this branch cannot perform (widening the Dependabot auto-triage rule to cover CWE-407 for development scope, in the GitHub UI). Leave pushing and PR creation to the user.

---

## Out of Scope

**Tier 3 (stale Maven PRs) is deliberately excluded from this branch** by decision on 2026-07-29. For the record, since it is the larger real risk:

- **PR #3235** `antlr4.version` 4.9.1 -> 4.13.2, open since 2026-01-25, 13 failing checks. ANTLR is consumed by `gremlin`, `gremlin-consumer-it`, `gremlin-it`, `graphql`, `postgresw`, and `engine`, so this is a cross-cutting parser change.
- **PR #4969** `org.apache.groovy:groovy` 4.0.32 -> 5.0.7, open since 2026-07-04, 6 failing checks, a major upgrade of the Gremlin scripting path.

Each needs its own branch and its own investigation. Folding them in here would make this branch unreviewable and unmergeable.
