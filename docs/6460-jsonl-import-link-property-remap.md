# Issue #6460: JSONL import does not remap LINK-typed property values

## Root cause

`JsonlImporterFormat` rebuilds an old-RID -> new-RID map (`ridIndex`) while importing, but only consulted it for
edge endpoints (`loadEdge`'s `ridIndex.get(out)` / `ridIndex.get(in)`). A regular `LINK`-typed **property** value
(and a `LIST`/`MAP` declared `OF LINK`) was passed straight through `loadProperties` -> `json2map` ->
`convertFromJSONType` -> `MutableDocument.fromMap()`, none of which consult `ridIndex`. `Type.convert`'s LINK
branch parses the string into an `RID` using the source database's RID string verbatim (`RID.create(database,
"#<bucket>:<pos>")`). Because import recreates records at fresh positions, that RID pointed at an unrelated
record (or none) in the target database - silent data corruption, no error raised.

## Fix

`integration/src/main/java/com/arcadedb/integration/importer/format/JsonlImporterFormat.java`:

- `loadProperties` now calls a new `remapLinkProperties(DocumentType, Map)` after `json2map()` and before
  `fromMap()`. For every property present in the JSON that the schema declares as `LINK`, or as `LIST`/`MAP`
  declared `OF LINK`, the source RID string(s) are looked up in `ridIndex` and replaced with the resolved new
  RID. Only schema-declared properties are touched - without a declared type there is no reliable way to tell a
  LINK string ("#12:34") apart from an ordinary string that merely looks like one.
- A value that cannot be resolved yet - it references a record that appears **later** in the source stream (a
  forward reference) - is left as-is and its property name is recorded, keyed by the record's own new identity,
  in `pendingLinkReconciliation`.
- After the whole file has been read (every RID mapping is now known), `reconcileUnresolvedLinks()` revisits
  every such record once, re-resolves the still-outstanding LINK values through `ridIndex`, and saves the record
  again if anything changed. A value that still cannot be resolved after this (the referenced record was
  excluded from the export, or the source link was already dangling) is left unchanged, matching the documented
  pre-fix behavior for that case, and is not counted as an import error.
- `loadDocument`, `loadVertex`, and `loadEdge` were updated to capture `loadProperties`' returned unresolved-name
  set and register it against the record's identity once `save()` has assigned one.

This mirrors, for properties, the same `ridIndex` mechanism edges already use for their endpoints - with a
reconciliation pass added on top so forward references (which edges do not need to handle, since an edge always
throws if an endpoint isn't found yet) are also fixed up instead of silently left wrong.

## Test

`integration/src/test/java/com/arcadedb/integration/importer/JsonLImporterIT.java`:
`importDatabaseRemapsLinkTypedPropertyValues` - hand-crafted JSONL with a `Person` type carrying a scalar `LINK`
property (`bestFriend`) and a `LIST OF LINK` property (`friends`). Source `"r"` RIDs are deliberately NOT the
sequence a fresh import would allocate, so any unremapped (still-old) value is directly observable:

- `Person#2.bestFriend -> Person#1` is a **backward** reference (Person#1 already imported) - resolved
  immediately by `remapLinkProperties`.
- `Person#3.friends -> [Person#4]` is a **forward** reference (Person#4 imported later in the stream) - resolved
  only by `reconcileUnresolvedLinks`.

Verified TDD-style: reverted the fix (`git stash` on the production file only) and confirmed the test fails
(`expected: #1:0 but was: #6:5`), then restored the fix and confirmed it passes.

### Verification run

- `JsonLImporterIT`, `JsonlExporterIT`, `Issue6455JsonlDateTimeRoundTripIT`: 12/12 passed.
- Full `integration` module suite (`-DexcludedGroups=benchmark,slow,vector`): 157 tests, 0 failures, 9 skipped.
- `mvn -pl integration -am compile`: BUILD SUCCESS.

## Scope / limitations carried forward

- Embedded-document (`EMBEDDED`) properties are not walked recursively for nested LINK values - out of scope of
  the reported bug, which is about direct LINK/LIST-of-LINK/MAP-of-LINK properties on the record itself.
- A LINK value that never resolves (the referenced record was excluded from the export via `-includeTypes` /
  `-excludeTypes`, or was already a dangling link in the source database) is left as the original source RID
  string, same as the pre-fix behavior for that specific case - there is nothing more correct to do without
  external knowledge of intent, and it is not counted as an import error.
- An unresolved forward-reference LINK value is transiently persisted as the raw *source*-database RID until
  `reconcileUnresolvedLinks()` runs. If that property backs a UNIQUE index this can coincidentally collide with
  another record's already-resolved value, raising a `DuplicateKeyException`. Confirmed real by tracing the code;
  accepted as a known, documented (in `reconcileUnresolvedLinks`'s Javadoc and the PR's "Review follow-ups"
  section) limitation rather than fixed, since a proper fix is a design choice (null placeholder / pre-scan /
  keep documented) surfaced during review but not resolved in this PR - see "Review cycles" below.

## PR

https://github.com/ArcadeData/arcadedb/pull/6654

## Review cycles

Ran via the `resolve-issue-with-review` skill, `--max-cycles=4`. The `claude` bot on this repo posts its review
as a plain PR issue comment (no commit SHA attached), gated on `createdAt` vs. the push timestamp.

| Cycle | Head SHA | Review outcome | What was applied / deferred / skipped |
|---|---|---|---|
| 1 | `a0aedaa9f4` (initial PR) | Review posted 2026-08-23T20:53:47Z: 5 findings (2 correctness, 1 perf, 2 test-coverage, 1 minor) | Applied: periodic commit in `reconcileUnresolvedLinks()`; regression tests for MAP-of-LINK, the never-resolves case, and a LINK property on an edge. Deferred: UNIQUE-index collision on the transient placeholder RID (design decision). Skipped (rationale recorded): `pendingLinkReconciliation`'s memory footprint, duplicate list/map-walking logic between the two remap methods. |
| 2 | `8c588accee` | Review posted 2026-08-24T07:44:40Z: `reconcileUnresolvedLinks()` had no error isolation, unlike the main loop | Applied: wrapped the per-entry reconciliation body in the same log/count/abort-or-skip handling as `loadDocument`/`loadVertex`/`loadEdge`; skip mode now commits every reconciled record. Skipped (rationale recorded): broad `catch (Exception e)` in `remapLinkValue`, eager allocation in `remapLinkProperties`, and a process question about committing review-transcript docs under `docs/`. |
| 3 | `c83f2f7d72` | Review posted 2026-08-24T07:50:30Z: recommended an in-code Javadoc note for the UNIQUE-index limitation, and repeated the `docs/review-deferred-*.md` convention concern | Applied: added the limitation directly to `reconcileUnresolvedLinks()`'s Javadoc; removed the two `docs/review-deferred-*.md` sidecar files and folded their content into the PR body's new "Review follow-ups" section (two consecutive review cycles flagged permanent per-PR review transcripts under `docs/` as the wrong home for this). Skipped (rationale recorded in the PR body): minor asymmetries between `remapLinkProperties`/`reconcileUnresolvedLinks`, and the pre-existing (not a regression) fact that edge records are never added to `ridIndex`. |
| 4 | `5383bcb1a0` | **Timeout** - the `claude-review` GitHub Action ran and completed successfully (`gh run view`: `status=completed`, `conclusion=success`) but posted no PR comment within the 15-minute poll window (checked again ~17 minutes after push: still nothing). Most likely read: nothing left to flag on this small, mostly-documentation diff, so the bot stayed silent rather than posting a redundant approval. | No new findings to act on. |

**Final state: timeout** (cycle 4). All findings from cycles 1-3 were resolved (applied, or deferred/skipped with
recorded rationale, now living in the PR body rather than in `docs/`) before the loop ran out of its configured
cycles. Nothing is left pending review from this session's side; the PR is left open for the developer, per this
skill's merge policy.
