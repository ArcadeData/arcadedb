# Issue #6458 - Postgres Execute max-rows (portal suspension) broken

Issue: https://github.com/ArcadeData/arcadedb/issues/6458
PR: https://github.com/ArcadeData/arcadedb/pull/6658

## Problem

The Postgres extended-protocol `Execute` with a non-zero max-row count (portal suspension) was broken three
ways in `PostgresNetworkExecutor.executeCommand()`:

- `PortalSuspended` was written *before* the data rows instead of after.
- It was sent *together with* `CommandComplete` - the wire protocol allows exactly one terminator per
  Execute.
- The portal was removed on the very Execute that suspended it, so the client's follow-up `Execute` meant to
  continue the fetch found no portal and got `NoData` - the result was silently truncated to the first batch
  with no way to resume.

A client that drives this path is any JDBC/cursor client with a non-zero `Execute` max-row count - pgjdbc
with `autoCommit(false)` + `setFetchSize(n)` is the common case.

## Fix

- `executeCommand()` no longer removes the portal on lookup (`getPortal(portalName, false)`); it is only
  discarded by an explicit `Close` or by a `Bind` reusing the same name.
- The portal's whole result is materialized once into a new `PostgresPortal.fullResultSet` field - by
  whichever of `Describe('P')` or the first `Execute` runs the statement first (`Describe` already has to
  read every row to discover sparse-document columns, so reusing that materialization avoids re-running the
  statement). Every `Execute` on the portal from then on slices `fullResultSet` by its own row-limit via a
  `resultCursor`, so `DataRow`s are always written first, followed by exactly one terminator:
  `PortalSuspended` when the slice stops on the limit with rows left, `CommandComplete` once drained.
- `describeCommand()`'s own unconditional full materialization (a related, broader bug beyond the issue's
  original scope: it discarded the client's row-limit entirely) is fixed the same way, sharing
  `fullResultSet` with `executeCommand()` guarded by `portal.executed` so the statement runs exactly once
  regardless of which message reaches it first.

## Known, tracked limitations (out of scope for this PR)

- **#6659** - the fix above eagerly materializes the entire result set into memory on the first
  `Execute`/`Describe`, even for a client that only wants a single small page and never continues. This is a
  real memory regression for that specific case compared to a (still-broken) pre-fix baseline. Documented in
  `PostgresPortal.fullResultSet`'s Javadoc; a streaming redesign belongs in #6659, not here.
- **#6660** - `bindCommand()` reuses the same `PostgresPortal` instance on a re-Bind of an already-executed
  prepared statement (pgjdbc promotes a repeated `PreparedStatement` to a server-side statement) without
  resetting `executed`/`fullResultSet`/`resultCursor`/`suspended`/`columns`/`rowDescriptionSent`. Pre-existing
  bug, made more visible by this PR's new pagination state (a stale `resultCursor` can make the next slice
  computation land on an empty range and report `CommandComplete` with an empty result on what should be a
  fresh execution). Tracked separately; not conflated with this fix.

## Tests

`postgresw/src/test/java/com/arcadedb/postgres/Issue6458PortalSuspensionIT.java`:

- `executeMaxRowsSendsRowsBeforeSuspendedNeverBothTerminatorsAndThePortalSurvivesToContinue` - raw wire-level
  reproduction of all three original defects (Parse/Bind/Execute/Execute, no Describe), asserting message
  ordering and terminator byte-by-byte. Confirmed failing pre-fix.
- `describePortalThenExecuteWithASmallLimitReturnsOnlyThatManyRowsThenSuspends` - the Describe('P')-first
  counterpart, covering the more consequential `describeCommand()` defect directly at the wire level (added
  in review cycle 2, see below).
- `aFetchSizeSmallerThanTheResultReturnsEveryRowAcrossSeveralSuspendedBatches` /
  `aFetchSizeThatDividesTheResultExactlyStillReturnsEveryRow` - pgjdbc `setFetchSize` tests covering the
  remainder-batch and exact-division pagination boundaries.
- `aPortalThatSuspendsCanStillBeClosedExplicitlyWithoutBreakingTheConnection` - a suspended portal must still
  be closeable without wedging the connection.

Verified: `mvn -pl postgresw verify -DskipITs=false` (all classes, `excludedGroups=benchmark,slow,vector`).

## Review cycles

### Cycle 1 - head `0b13f9c7da` -> `08759526f1`

`claude` bot review (PR issue comment, 2026-08-23T21:54:28Z). Two actionable points, both applied:

- Eager full materialization on the plain-Execute path is now called out explicitly in
  `PostgresPortal.fullResultSet`'s Javadoc, tracked as #6659.
- The pre-existing re-Bind-of-an-already-executed-statement staleness bug is flagged at its exact site in
  `bindCommand()`, tracked as #6660.
- An "extract shared helper" nit was left as-is (reviewer marked it non-blocking; the two call sites are not
  full duplicates).

Commit: `address review: document known limitations, file follow-up issues` (`08759526f1`).

### Cycle 2 - head `08759526f1` (unchanged through this review)

`claude` bot review (PR issue comment, 2026-08-23T22:00:50Z) - landed on the same head as cycle 1's fix but
was missed by the original SHA-only polling (the `claude` bot posts as a plain issue comment with no commit
SHA on this org's repos) and is processed here once that gap in the orchestrating skill was fixed.

- **Applied**: added `describePortalThenExecuteWithASmallLimitReturnsOnlyThatManyRowsThenSuspends`, closing
  the one test-coverage gap the review found (no test had directly exercised `Describe('P')` followed by a
  small-limit `Execute` at the wire level).
- **Skipped** (rationale in `docs/review-deferred-0875952.md`): the eager-materialization performance note
  (already tracked as #6659), the stale-rebind note (already tracked as #6660), and a cosmetic DEBUG-only
  logging false-positive for sparse documents across batches (no correctness impact; the reviewer's own
  assessment agrees it's cosmetic).

See `docs/review-deferred-0875952.md` for the full rationale on each skipped item.

Commit: `address review: add Describe('P')-then-small-limit wire test` (`59f3975122`).

### Cycle 3 - head `59f3975122`

`claude` bot review (PR issue comment, 2026-08-24T08:36:02Z) - independently re-verified the pagination
slice-boundary logic, the `!portal.executed` guard consistency between `describeCommand()` and
`executeCommand()`, and the catalog-query bypass, all confirmed correct. Two minor/optional points:

- **Applied**: `openJdbcConnection()`'s test helper hardcoded `localhost:5432` instead of using
  `GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()` like the raw-socket tests earlier in the same
  file - switched for consistency.
- **Skipped** (rationale in `docs/review-deferred-59f3975.md`): a naming-convention nit about
  `review-deferred-*.md` not matching the repo's `<issue-number>-<description>.md` postmortem convention -
  that filename is this orchestrating skill's own convention for a per-cycle review-response log, a distinct
  kind of artifact from the issue-postmortem docs.

See `docs/review-deferred-59f3975.md` for the full rationale.

Commit: `address review: use configured Postgres port in JDBC test helper` (`b9c3a65177`).

### Cycle 4 - head `b9c3a65177` (final)

`claude` bot review (PR issue comment, 2026-08-24T08:40:50Z) - read the full diff across all 4 commits plus
the current state of both source files and independently re-traced `executeCommand()`/`describeCommand()`.
No actionable items:

- **Correctness**: all three original defects confirmed fixed; the `end == total` boundary, the
  cumulative-row-count `CommandComplete` tag, the shared `!portal.executed` guard, and a specific NPE
  concern on `portal.fullResultSet.isEmpty()` in the catalog-query branch were all checked by hand and found
  safe. No changes requested.
- **Performance / #6659**: reiterates the already-tracked eager-materialization concern and recommends
  treating #6659 as a near-term follow-up rather than a "someday" - a priority opinion on an already-filed,
  already-open issue, not a request for a code change in this PR. No action taken here; noted for whoever
  picks up #6659.
- **#6660**: reconfirms the already-tracked, already-flagged pre-existing issue; explicitly "not asking for
  it here."
- **Minor / non-blocking**: repeats the `review-deferred-*.md` naming-convention observation from cycle 3
  (same rationale as `docs/review-deferred-59f3975.md` still applies - skill convention, not this PR's to
  redesign) and separately notes the `PostgresPortal.suspended` field-level comment reads as general-purpose
  state when it's really pagination-branch-only - explicitly flagged by the reviewer itself as "not a real
  issue, just noting."
- **Security**: no concerns; the unrelated `bindCommand()` parameter-size guard visible in the diff context
  was checked and fails closed correctly.

No code changes made this cycle - every point was either already addressed (with a still-applicable
rationale on record) or explicitly non-actionable per the reviewer's own text ("Nothing... rises to
blocking," "not a real issue," "not asking for it here"). Working tree stayed empty; no new
`review-deferred-*.md` file was needed since nothing new required a rationale beyond what cycles 2-3 already
recorded.

### Post-review: merge conflict with `main`, and #6660 turned out to be a live regression, not pre-existing

Merging `main` into this branch conflicted in `PostgresNetworkExecutor.java`: `main` had picked up #6473
(alias-column-type resolution), which added a third `resolveAliasToSourceProperty(portal)` argument to
`getColumns()` at the same two call sites this PR changed to read from the new `portal.fullResultSet`
instead of the pre-existing `portal.cachedResultSet`. Resolved by keeping this branch's `fullResultSet`
reads and adding main's new argument; verified via `postgresw` compiling clean and
`Issue6458PortalSuspensionIT` + `Issue6473AliasedColumnTypeResolutionIT` passing.

That merge triggered the PR's CI, and `python-e2e-tests` failed on `test_asyncpg.py::test_parameterized_insert`
(`assert len(rows) == 1` / `where 0 = len([])`) - a test that passed cleanly on `main` immediately before this
merge. Reproduced directly against a locally built server with a minimal `asyncpg` script: a parameterized
INSERT (which independently still hits a pre-existing, unrelated column-count `ProtocolError` the test already
tolerates) succeeds and is visible to an immediate `SELECT ... WHERE id = $1`, but the exact same `SELECT`
re-run moments later on the same connection returns zero rows.

Root cause: **this was #6660**, already filed as "pre-existing and out of scope" during cycles 2-4 above - but
it was #6458 itself that made it reachable from ordinary client behavior. Before #6458, a portal was removed
right after its Execute completed, so a client reusing a cached prepared statement (asyncpg's default
statement cache reusing a `PreparedStatement` for a repeated identical query, exactly like pgjdbc's
server-side-statement promotion) always rebound onto a fresh portal. After #6458 deliberately stopped removing
portals (needed for suspension to survive across Executes), that same reuse pattern now rebinds onto the
*same, already-exhausted* `PostgresPortal` instance - `bindCommand()` never reset `executed`/`fullResultSet`/
`resultCursor`, so the second run's Execute sliced an already-fully-consumed `fullResultSet` and returned an
empty result instead of re-running the query. Confirmed at the wire level with a new regression test,
`Issue6458PortalSuspensionIT#reBindingAnAlreadyExecutedPortalWithNoNewParseReRunsInsteadOfServingTheExhaustedFirstRun`
(TDD: fails pre-fix with the second Execute jumping straight to an empty `CommandComplete`; passes post-fix).

Fixed by resetting `executed`/`fullResultSet`/`cachedResultSet`/`resultCursor`/`suspended` in `bindCommand()`
whenever it reuses an already-executed portal - but **only** when `portal.sqlStatement != null` (a real,
engine-parsed query deferred to Execute). An earlier, ungated version of this reset also fired for
`BEGIN`/`COMMIT`/`ROLLBACK`, whose response is precomputed once during `Parse` itself via
`setEmptyResultSet()` and never touches `sqlStatement` - resetting that pre-computed state broke it before
Execute ever ran, which surfaced as 3 of this class's own pgjdbc-driven tests failing with "Received
resultset tuples, but no field structure for them" (pgjdbc pipelines its own internal `BEGIN`/`COMMIT`
through the extended protocol using a cached statement, the same pattern as the bug itself). `columns`/
`rowDescriptionSent` are deliberately left untouched by the reset too: they describe the statement's row
shape, invariant across re-binds, and a client that skips `Describe` on a re-bind doesn't expect an
unsolicited second `RowDescription`.

Verified: the new regression test (red pre-fix, green post-fix, isolated and full-class runs); the full
`postgresw` module - 408 unit tests + 183 integration tests, including every `BEGIN`/`COMMIT`/`ROLLBACK`-
specific IT (`Issue6457AbortedTransactionSimpleQueryIT`, `Issue6543RollbackExtendedProtocolIT`,
`Issue6545AbortedTransactionExtendedQueryIT`, `Issue6548AbortedTransactionRollbackExtendedProtocolIT`),
`PostgresProtocolIT` (75 tests), and `PostgresWJdbcIT` (45 tests) - all green; and the original `asyncpg`
repro re-run end-to-end against a freshly built server, confirming the second `SELECT` now returns the
inserted row instead of an empty result.

### Post-#6660: CodeRabbit found the deeper design bug the #6660 patch only papered over

A second bot review, from `coderabbitai[bot]` (`#pullrequestreview-5008648991`, on head `105cdbb0b8` - the
#6660 fix above), flagged the actual root cause: `bindCommand()` looked up the portal to bind by the
*prepared statement's* name (`getPortal(sourcePreparedStatement, false)`) and stored that same mutable
object under the new portal name too. Two portal names bound from one statement were never independent
portals - they were two names for the same object. Concretely: bind `P1`, suspend it mid-fetch, then bind
`P2` from the same statement with different parameters - `P2`'s Bind (and Execute) would silently
reset/overwrite `P1`'s suspended progress, and a later `Execute(P1)` would resume or lose `P2`'s state
instead of its own. The user asked "is it related?" for this one too, checked it directly against the code
(confirmed: `getPortal(sourcePreparedStatement, false)` predates this PR by a long history - not something
#6458 introduced), and chose to fix it now rather than defer to a follow-up issue.

The #6660 patch (resetting `executed`/`fullResultSet`/etc. on a stale rebind) treated the symptom on one
axis (same name, re-bound) but not the other (two different names, same object) - CodeRabbit's finding
applies to the same lines that patch touched.

**Real fix**: split "prepared statement" (PARSE's immutable output: query text, language, parameter types,
the parsed `sqlStatement`, and - for `BEGIN`/`COMMIT`/`ROLLBACK`/a resolved catalog answer - PARSE's own
precomputed response) from "portal" (one Bind's independent, mutable execution state). Added
`PostgresPortal.bindFrom(template)`, which `bindCommand()` now calls to create a **fresh** portal for every
single Bind, rather than mutating and re-storing the statement's own object. A new `preparedStatements` map
(keyed by statement name, written once by `parseCommand()`, never mutated afterwards) sits alongside the
existing `portals` map (keyed by portal name, written by `bindCommand()`, read by `describeCommand()`/
`executeCommand()`/`closeCommand()`); `describeCommand()`'s `Describe('S')` branch (which names a *statement*,
not a portal) now reads/writes `preparedStatements` instead. This **removed the #6660 patch's reset code
entirely** - cloning from a never-mutated template makes every rebind start fresh on its own, more simply
than the earlier gated reset did.

Regression tests: extended `reBindingAnAlreadyExecutedPortalWithNoNewParseReRunsInsteadOfServingTheExhaustedFirstRun`
(now expects a fresh `RowDescription` on the second run too, since it's a genuinely new portal - not the
"preserve rowDescriptionSent" behavior the #6660 patch had), plus a new
`twoPortalsFromOneStatementDoNotShareStateEvenWhenOneIsSuspended` matching CodeRabbit's exact scenario (bind
`P1`, suspend it, bind+drain `P2` from the same statement, then confirm `P1` resumes from exactly where it
left off). Both confirmed red against the pre-fix code, green after. Full `postgresw` module re-verified
green (408 unit + 184 IT tests - one more IT than before, the new test), and the original `asyncpg` repro
re-run end-to-end once more against a freshly built server.

## Final state

**Clean approval after 4 review cycles, then a two-stage post-review fix** once merging `main` triggered this
PR's own CI: #6660 first (a filed-but-deferred issue turned into a demonstrated `python-e2e-tests` regression
by #6458 itself), then a `coderabbitai` review on that fix exposed the deeper root cause (portal identity
shared across Bind names) and the real fix replaced the #6660 patch entirely. #6659 (eager result
materialization) remains open and out of scope. See PR #6658 for the full comment history.

Merge remains the developer's responsibility - this loop does not merge PRs.
