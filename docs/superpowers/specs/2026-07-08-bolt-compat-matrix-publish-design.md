# Publish the Bolt driver compatibility matrix (#4892)

**Epic:** #4882 (Bolt Driver Compatibility Certification), Group D - publication.
**Depends on:** #4891 / PR #5115 (merged 2026-07-08), which delivered the data pipeline this issue consumes.
**Status:** design approved 2026-07-08.

## Problem

The Bolt certification epic exists to produce one visible artifact: a published,
auto-generated driver x feature x version compatibility matrix, plus a status
badge in the README. Today proof lives only in per-language test files and in a
machine-readable JSON artifact that no human reads.

PR #5115 (#4891) already built the entire upstream pipeline:

- `bolt-compat-matrix.json` - produced nightly by the `merge-matrix` job in
  `.github/workflows/bolt-nightly.yml`, uploaded as the `bolt-compat-matrix`
  artifact (30-day retention, **not committed**). Shape:
  `scenarios[SCEN-ID][language][version] = status`, plus `languages`,
  `missing_cells`, `unexpected_cells`, `empty_cells`, `has_failures`.
- `bolt/conformance/spec.yaml` - scenario metadata: `id`, `area`, `title`,
  `current_status` (`passing` / `expected-fail` / `not-applicable` /
  `unverified`), and `known_limitation` / `tracking_issue` (required when
  `expected-fail`). The conformance README explicitly names #4892 as the
  consumer that parses this file into the published matrix.
- `bolt/conformance/driver-versions.md` - the authoritative language x band x
  version table; already the "expected cells" source of truth for the merge.
- A nightly regression issue (`bolt-compat-regression`) auto-opened/closed on red
  cells.

What is missing, and what this issue delivers: a **human-facing page**, a
**README badge**, **per-cell links to tracking issues**, and the **automation**
that keeps both current from the CI artifact.

Current data reality (2026-07-08): 38 scenarios `passing`, 1 `not-applicable`
(ERR-003), zero `expected-fail` - the type-fidelity/protocol gaps were closed by
#4890's sub-issues. The matrix is all-green today, but the renderer must handle
fail / partial / missing cells generically for future nights.

## Acceptance criteria (from #4892)

1. Matrix page published, covering every A1 scenario x every certified driver x
   every pinned version.
2. Badge generated and linked from the main README.
3. Generation automated from the CI artifacts (not hand-maintained). Full
   automation is in scope here; the documented-manual-refresh fallback is not
   needed but the "last verified" timestamp is retained on the page regardless.
4. Every non-passing cell links to its corresponding limitation issue.

## Decisions (locked during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Hosting | In-repo, committed | Zero infra/settings changes; works inside a PR + the nightly. No GitHub Pages setup exists in this repo. |
| Page format | Markdown table | Renders natively on GitHub; committed HTML would show source without Pages. Diff-friendly, greppable, matches repo doc convention. |
| Refresh mechanism | Commit to `main`, `[skip ci]`, only-if-changed | Always current; badge/page live from `main`'s raw URL; cleanest discoverability. |
| Scope | Build page + badge | #5115's JSON is not human-facing; ACs 1-4 are unmet. On merge, close #4892 -> completes Group D -> epic #4882 can close. |
| Location | `bolt/conformance/` | Co-located with the spec + tools it is generated from. |

## Architecture

A single pure-stdlib Python renderer, co-located with the existing conformance
tooling, turns the nightly artifact into two committed files. A new nightly job
regenerates and commits them.

```
bolt-compat-matrix.json (nightly artifact) ─┐
spec.yaml (scenario metadata)               ├─► render_matrix.py ─► COMPATIBILITY.md
driver-versions.md (column order / bands)   ─┘                    └─► badge.json
```

### Components

- **`bolt/conformance/tools/render_matrix.py`** - new. Pure stdlib, mirrors the
  style of the existing `merge_matrix.py` / `junit_to_matrix.py`. One module, no
  new dependencies.
  - Inputs: `--matrix bolt-compat-matrix.json` (optional), `--spec spec.yaml`,
    `--versions driver-versions.md`, plus `--repo`, `--run-url`, `--timestamp`
    for the header (passed in by the workflow; never derived with a forbidden
    clock call inside library code).
  - Outputs: `--out-page COMPATIBILITY.md`, `--out-badge badge.json`.
  - **Fallback mode**: when `--matrix` is absent or a cell is missing from it,
    fall back to the scenario's `spec.yaml` `current_status` as the baseline,
    and stamp the page "last verified: pending first nightly". This makes the
    PR-committed initial page real (all-green baseline from spec.yaml) and keeps
    the page robust when a single nightly cell fails to produce an artifact.
- **`bolt/conformance/COMPATIBILITY.md`** - generated page (committed).
- **`bolt/conformance/badge.json`** - shields.io endpoint schema (committed).
- **`bolt/conformance/tools/test_render_matrix.py`** - unit tests, wired into
  the existing `.github/workflows/bolt-conformance-spec.yml`.
- **`.github/workflows/bolt-nightly.yml`** - new `publish-matrix` job.
- **`README.md`** - one badge added to the existing shields row.

## The page (`COMPATIBILITY.md`)

- **Rows**: scenarios, grouped by the 9 areas (connection, auth, transactions,
  causal-consistency, multi-database, result-handling, type-roundtrip, errors,
  protocol). Row label = `id` + `title` (e.g. `CONN-001 - bolt:// connection`).
- **Columns**: one per `language:version` cell from `driver-versions.md`, in that
  file's row order (java 4.4.20 / 5.28.5 / 6.2.0, javascript 4.4.11 / 5.28.3 /
  6.2.0, python 5.28.4 / 6.1.0 / 6.2.0, csharp 4.4.0 / 5.28.4 / 6.2.1, go 5.27.0
  / 5.28.0 / 5.28.4). A grouped header row names each language over its versions.
- **Cell glyphs**:
  - ✅ passing
  - ❌ fail - links to the scenario's `tracking_issue`; if none, links the
    `bolt-compat-regression` nightly issue.
  - ⚠️ expected-fail / known-limitation - links `tracking_issue`.
  - ➖ not-applicable (e.g. ERR-003) - tooltip/footnote from `known_limitation`.
  - `·` cell not run / missing from the matrix for that language:version.
- **Header block**: title, one-line "auto-generated, do not hand-edit" note,
  **Last verified** timestamp + link to the source nightly run, and the resolved
  driver-version table (or a link to `driver-versions.md`).
- **Per-area summary** line (e.g. `type-roundtrip: 12/12 passing across 15
  cells`) and a legend.

## The badge (`badge.json`)

- shields.io endpoint schema:
  `{"schemaVersion":1,"label":"bolt drivers","message":<msg>,"color":<color>}`.
- Status logic derived from the matrix (or spec.yaml fallback). `not-applicable`
  cells are **excluded from the denominator entirely** - they are N/A, not a
  shortfall, and never downgrade the badge (today's ERR-003 must keep the badge
  green):
  - **green** `"5/5 passing"` - every *applicable* cell passes, no
    `has_failures`. Presence of `not-applicable` cells does not change this.
  - **yellow** `"partial"` - at least one `expected-fail` cell exists, but no
    real fail / missing / unexpected / empty.
  - **red** `"N failing"` - any `fail`, or `has_failures` true (missing /
    unexpected / empty cells).
- README: add to the existing shields row -
  `[![Bolt drivers](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ArcadeData/arcadedb/main/bolt/conformance/badge.json)](bolt/conformance/COMPATIBILITY.md)`.

## Nightly wiring (`publish-matrix` job)

Appended to `bolt-nightly.yml`:

- `needs: merge-matrix`
- `if: ${{ always() }}` - publish the true state, including red nights.
- `permissions: { contents: write }`
- Steps:
  1. `actions/checkout` (`main`, full so we can commit).
  2. `actions/setup-python`.
  3. `actions/download-artifact` `bolt-compat-matrix` (`continue-on-error` -
     absent artifact triggers the spec.yaml fallback path).
  4. Run `render_matrix.py` with `--run-url`/`--timestamp` from workflow context,
     regenerating `COMPATIBILITY.md` + `badge.json`.
  5. Commit to `main` as `github-actions[bot]` **only if `git status` shows a
     change** (no empty commits), message ending `[skip ci]`; `git push`.

`concurrency: bolt-nightly` (already set at workflow level) serializes runs so
the commit-then-push cannot race a manual dispatch.

## Verification

How we prove this works:

- **Unit tests** (`test_render_matrix.py`, run in `bolt-conformance-spec.yml`):
  - all-passing matrix -> all ✅, green badge `5/5 passing`;
  - a `fail` cell -> ❌ linked to the scenario's `tracking_issue`;
  - a `fail` cell with no `tracking_issue` -> ❌ linked to the regression issue;
  - `expected-fail` -> ⚠️ + yellow `partial` badge;
  - `not-applicable` -> ➖ + footnote, excluded from the denominator: badge
    stays green when it is the only non-passing status (today's ERR-003 case);
  - missing cell (in `driver-versions.md`, absent from matrix) -> `·` +
    `has_failures`/red;
  - fallback mode: no `--matrix` -> page rendered from spec.yaml `current_status`
    with "pending first nightly" stamp;
  - badge color thresholds (green/yellow/red) exhaustively.
- **Functional dry-run**: render the committed `bolt/conformance/testdata`
  sample JUnit -> `junit_to_matrix.py` -> `merge_matrix.py` -> `render_matrix.py`
  and assert the page/badge shape (documented in the PR, mirrors #5115's
  dry-run note).
- **Initial committed artifacts**: the PR commits `COMPATIBILITY.md` +
  `badge.json` generated in fallback mode (real all-green baseline from
  spec.yaml), so the page and badge are live from the moment the PR merges; the
  first nightly overwrites them with true per-version data.
- `actionlint` clean on the edited `bolt-nightly.yml`.

## Closure

On merge the PR **closes #4892**, satisfying all four ACs and completing Group D.
With Groups A-D done, epic **#4882** can close; the PR notes this for the
maintainer to action.

## Non-goals

- GitHub Pages / a served HTML site (explicitly deferred; would be a follow-up if
  a browsable URL is later wanted - the renderer's data model would carry over).
- Pushing to the separate `arcadedb-docs` repo.
- Any change to the conformance spec, the nightly matrix jobs, or driver-version
  bands - this issue only *consumes* those.
- Re-deciding the advertised server identity (owned by #4884 / `SERVER_IDENTITY.md`).
