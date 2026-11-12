# Per-record maintenance for INCREMENTAL materialized views (#11475)

## Intent

A `REFRESH INCREMENTAL` materialized view must not hold a commit past a client timeout. Today every commit that touches a
source type pays one full refresh pass on the committing thread (`MaterializedViewRefresher.writeSnapshot`: collect every
backing RID, re-run the defining query, rewrite or delete every row). That pass is O(rows of the view). #11464 bounded the
number of passes on the committing thread and #11470/#11476 bounded the scheduler loop; neither bounds the size of the one
pass. The observed symptom is #11464: a `CREATE VERTEX User` stalling ~30 s on the Raft leader in
`ThreeNodesLoadTestIT.threeNodeReplicationWithMaterializedView`, whose view is:

```sql
CREATE MATERIALIZED VIEW UserStats AS
  SELECT id AS userId,
    out('HasUploaded').in('Likes').size() AS totalLikes,
    out('FriendOf').size() AS totalFriendships
  FROM User
  REFRESH INCREMENTAL
```

## Goals

- For views whose rows map 1:1 to source records ("per-record views", defined below), the pass owed to a commit costs
  O(source records changed by that commit), not O(view).
- Read-your-writes is kept: the pass still runs after commit on the committing thread, as today, so a client that commits
  and then reads the view sees its own change.
- `UserStats` qualifies, so the load-test stall goes away.
- Every view that does not qualify behaves exactly as today.

## Non-goals

- Making views with GROUP BY, aggregates, ORDER BY/LIMIT/SKIP, DISTINCT, LET, UNWIND or subqueries incremental. They keep
  the full O(view) pass, now documented as such.
- Asynchronous refresh (moving the pass off the committing thread). Considered as approach B below; a separate follow-up if
  someone asks for it.
- Crash consistency between a source commit and its view pass. Today a crash between the two leaves the view stale; this
  design keeps that behavior.
- Upgrading existing views automatically at open (see Migration; `ALTER ... REFRESH INCREMENTAL` is the explicit upgrade).

## Anti-goals (a failure even if it technically works)

- Any added cost on the commit path of a database with no per-record view.
- A per-record pass that silently loses a change: a delta dropped during a concurrent pass, after a failed pass, or on
  overflow must lead to a full pass, never to a VALID view missing rows.
- Index churn on the backing type from a full refresh (the in-place rewrite in `writeSnapshot` exists precisely to avoid
  tombstones, see its Javadoc).

## Accepted behavior change (decided)

A per-record pass recomputes only the rows of the source records the commit changed. Columns computed by following links or
edges (`out()`, `in()`, `address.city`) therefore refresh only when their own source record changes. Today they refresh as a
side effect whenever any source record of the view changes, because every commit rewrites every row. `MaterializedViewSources`
already documents that links and edges are not tracked sources, so `REFRESH INCREMENTAL` never promised more than this. For
`UserStats`: a new `Likes` edge no longer shows in `totalLikes` of every user at the next `User` commit; it shows when that
user's `User` record changes, or at the next full refresh (`REFRESH MATERIALIZED VIEW`). This needs a release note.

## Approaches considered

| | Approach | Trade-off | Outcome |
|---|---|---|---|
| A | Per-record maintenance for a strict class of views; backing rows linked to their source by a stored, indexed property | Commit cost O(changed records), keeps read-your-writes. Costs a stricter classifier, one index entry per view row, and the link/edge freshness change above | **Chosen** |
| B | Explicit opt-in `REFRESH INCREMENTAL ASYNC`: the first pass goes to the scheduler via `MaterializedViewScheduler.handOff` | Small; gives up read-your-writes on views that opt in | Deferred, follow-up only on demand |
| C | Async above a refresh-duration threshold (`lastRefreshDurationMs`) | Whether a view gives read-your-writes would flip silently with its size and machine load | Rejected |

## Design

### 1. Classification

A new predicate (in `MaterializedViewQueryClassifier`, e.g. `isPerRecord(sql, database)`) accepts a SELECT when all hold:

- `isSimple()` holds (no GROUP BY, no aggregate projection, FROM is a plain type identifier);
- no ORDER BY, LIMIT, SKIP, DISTINCT, LET, UNWIND, `expand()` in the projection;
- no subquery anywhere (projection, WHERE, LET);
- exactly one source type (`MaterializedViewSources.typeNames().size() == 1`).

Projection and WHERE may call functions and follow links/edges (accepted behavior change above). Anything else is not
per-record and uses the full pass. The predicate is the single authority; when in doubt it answers false.

The classification runs when the view is created (and on the upgrade path, see Migration), and its outcome is persisted as
the `perRecord` flag (section 2). The persisted flag is the authority from then on: it is not recomputed at load, so a later
change to the classifier cannot flip a view whose rows already carry the link (call made while writing the design). The
persisted `simpleQuery`, written by the looser `isSimple()`, is not used for this decision and stays in the JSON for
compatibility.

### 2. Source link on backing rows

A per-record view's backing type gets:

- a property `_sourceRID` of type `LINK` on every row, holding the RID of the source record the row was computed from;
- a UNIQUE LSM index on `_sourceRID`.

Both are created in `MaterializedViewBuilder.create()`, inside the existing `schema.recordFileChanges(...)` block, before the
initial refresh, so HA replicas receive them with the view. Building an index calls `database.waitForAsyncCompletion()`,
which never returns inside that frame while the async executor holds a batch (#10537, see engine/CLAUDE.md): `create()` must
take `database.quiesceAsync()` before entering the frame, which it does not do today. The property is visible to readers of the view
(`SELECT * FROM UserStats` returns it). A defining query that already projects a column named `_sourceRID` is not
per-record (falls back to full).

The schema JSON of the view records that its rows carry the link (e.g. `"perRecord": true`). Only a view with that flag
takes the per-record path.

### 3. Capturing the delta

`MaterializedViewChangeListener` stops ignoring the record. For a per-record view, `onAfterCreate/Update/Delete` append the
record's RID to a per-transaction, per-view buffer (`TransactionContext.putDerived(listener, buffer)` or an attachment; the
builder picks, see below) and register the after-commit callback once, as today. The buffer holds RIDs as primitive arrays
(bucket id `int[]`, position `long[]`, the same shape as `RecordIdentities`), deduplicated.

When the buffer exceeds a cap, it is dropped and replaced by a "full pass needed" mark for that transaction: a bulk
transaction touching most of the source must not allocate a delta larger than the view and then pay a per-record pass slower
than one full pass. Default cap: builder's call (suggestion: 10 000 RIDs, as a `GlobalConfiguration` entry).

Non-per-record views keep the current listener behavior unchanged.

### 4. The view-level pending delta and ownership

The refresh ownership state machine in `MaterializedViewImpl` (`tryBeginRefresh`, `markRefreshPendingIfRunning`,
`finishRefreshPassAndCheckPending`, `releaseRefreshAfterFailure`) is kept. A per-record view adds a view-level pending delta
next to it: a set of RIDs plus a "full pass needed" flag, guarded so appends and drains are atomic with respect to each other.

After commit, the callback first appends its transaction's RIDs (or its full-pass mark) to the view-level pending delta, then
tries to begin the refresh exactly as today:

- it becomes the owner: it drains the pending delta and makes one pass (per-record, or full if the flag is set);
- a pass is running: it marks the request pending, as today. The running owner's next pass drains the delta, which now
  includes this commit's RIDs.

Draining happens after the append, and the append happens after the commit, so every RID is re-evaluated against a state
that includes the commit that changed it. The existing hand-off to the scheduler thread (#11464, #11476) is unchanged: the
further passes it runs drain the same pending delta.

If the pending delta grows past the cap, it collapses into the full-pass flag.

### 5. The per-record pass

In its own transaction (`database.transaction(..., false)`), under `BackingTypeWriteScope`, as `writeSnapshot` does today:

1. Re-evaluate the defining query for the drained RIDs only. The query is derived from the parsed statement: the projection
   gains `@rid AS _sourceRID`, the FROM target becomes the RID list (for example a `FROM :rids` parameter or an equivalent
   built on the parse tree), WHERE is kept and gains a type filter that accepts the source type and its subtypes (for example
   `@this INSTANCEOF '<source>'`). The filter is required: a RID-list target returns whatever record sits at each RID,
   whatever its type, and the listener fires polymorphically. The derived query is cached on the view and not re-parsed per
   pass, but the cache is invalidated wherever the view's `query` changes: `MaterializedViewImpl.retargetSourceType` (source
   rename, #10096) rewrites it in place, and the ALTER path builds a copy of the view.
2. For each result row: look up the backing row through the `_sourceRID` index; update it in place with `applyRow` if found,
   create it otherwise.
3. For each drained RID with no result row (deleted source record, or no longer matching WHERE): delete its backing row if
   one exists.

A RID whose record now belongs to a type that is not the source (or a subtype) produces no row and is handled by step 3.

### 6. The full pass on a per-record view

A full pass (initial build, `REFRESH MATERIALIZED VIEW`, a full-pass flag, recovery after failure) on a per-record view runs
the derived query over the whole source type and matches rows by `_sourceRID` through the index instead of by scan order:
update the row with the same source, create missing ones, then delete backing rows whose source no longer produced a row.
Matching by scan order, as `writeSnapshot` does, would move `_sourceRID` values between rows and churn the index on every
full pass. Non-per-record views keep `writeSnapshot` as is.

Any full pass on a per-record view, whatever triggered it (manual `REFRESH`, a full-pass flag, failure recovery), takes the
pending delta and clears it atomically BEFORE running its query (call made while writing the design): everything appended
before that point is covered by the full pass, and RIDs appended while it runs stay pending for the next pass, so none is
lost and none is re-evaluated twice for nothing.

### 6b. Views over views

A view whose source is another view's backing type is allowed to be per-record (call made while writing the design). The
upstream per-record pass writes only the changed backing rows, so the downstream view's listener sees exactly those RIDs,
which is cheaper than today, where every upstream pass rewrites every row and fires the downstream listener for all of them.
An upstream full pass fires the downstream listener for every row, which hits the downstream overflow cap and becomes a
downstream full pass: the same cost as today, no worse.

### 7. Failure

A failed per-record pass behaves as a failed full pass does today (status ERROR then STALE, ownership released, the error
logged), and in addition sets the view's full-pass flag: the RIDs of the failed pass are gone, so the next pass must be a
full one. A VALID status after a per-record pass that followed a failure without a full pass is the anti-goal.

Decided: the recovery full pass does not run on a committing thread. The next commit that finds the full-pass flag set by a
failure (as opposed to by an overflow, see below) hands the pass to the scheduler through the existing
`MaterializedViewScheduler.handOff`, keeping ownership across the hand-off as #11464 does, and returns. The view stays STALE
until that pass succeeds, so the status tells readers the view is behind. Commits arriving meanwhile append their RIDs to the
pending delta, which the recovery pass drains (section 6). If the scheduler cannot take it (closed, not a local schema), the
committing thread makes the pass, as the existing hand-off fallback does. The two reasons for a full pass therefore need to
be distinguishable in the view-level state (e.g. a "recovery" bit next to the full-pass flag).

Decided: the overflow full pass (section 3) runs on the committing thread of the bulk transaction that overflowed, as today's
pass does, keeping read-your-writes for that committer.

### 8. HA

Unchanged in shape: the pass runs where the commit runs (the leader) and its transaction replicates like any other. The
follower branch of `handOff` and the leader-only refresh rule (#11031) still apply. The schema additions (property, index,
`perRecord` flag) are made inside `recordFileChanges`, so followers get them with the view.

### 9. Migration

Decided: only `REFRESH INCREMENTAL` views get the link, and `ALTER MATERIALIZED VIEW ... REFRESH INCREMENTAL` is the upgrade
path.

- `CREATE MATERIALIZED VIEW ... REFRESH INCREMENTAL` with a per-record query: link property, index and `perRecord` flag at
  creation. MANUAL and PERIODIC views never get them and pay nothing.
- `ALTER MATERIALIZED VIEW ... REFRESH INCREMENTAL` on a view whose query is per-record and that is not yet flagged (a MANUAL
  or PERIODIC view, or an INCREMENTAL view created before this change, re-altered to INCREMENTAL): inside the ALTER's schema
  change (same `quiesceAsync` + `recordFileChanges` rule as creation, leader only) it adds the property and the index, runs
  one full pass that writes `_sourceRID` on every row, and sets the flag. On a view already flagged it is a no-op for the
  link.
- Views created before this change are not upgraded at open (that would be a schema change at open time, which a follower
  must not make). They keep the full pass until altered as above.
- `ALTER` away from INCREMENTAL keeps the link, the index and the flag (call made while writing the design): dropping them
  is a second schema change for no gain, and a later ALTER back is then free. Its full passes use the matching pass of
  section 6.
- The ALTER path that rebuilds the view object (`LocalSchema`, `copyWithRefreshMode`, around the `registerListeners` call
  for the replacement view) carries the `perRecord` flag and invalidates the derived-query cache.

The release note covers: drop-and-recreate is not needed, `ALTER ... REFRESH INCREMENTAL` upgrades; the link/edge freshness
change; `_sourceRID` appears in the view's rows.

## Verification

Tests in `engine/src/test/java/com/arcadedb/schema/`, in the style of `MaterializedViewIncrementalTest`:

- classifier: each excluded shape (ORDER BY, LIMIT, SKIP, DISTINCT, LET, UNWIND, `expand()`, subquery in WHERE/projection,
  two source types, a projected `_sourceRID`) is not per-record; `UserStats`-shaped query is.
- create, update into and out of WHERE, delete, subtype record: view rows match a full re-run of the defining query.
- the pass touches only the changed rows: counted through records read/written, not wall clock (no timing assertions).
- concurrent committers: no delta lost (final view equals a full re-run), using the existing concurrency test patterns
  (`MaterializedViewConcurrentRefreshTest`).
- overflow: a transaction past the cap triggers a full pass and the view is correct.
- failure: an injected failure in a per-record pass leaves the view non-VALID; the next commit returns without making the
  full pass itself, the scheduler makes it, and the view ends VALID and correct, including RIDs committed meanwhile.
- overflow: the full pass runs on the committing thread, and the committer reads its own change in the view right after.
- ALTER upgrade: a MANUAL view altered to INCREMENTAL gets the link, the index and a correct view; a pre-existing
  INCREMENTAL view (no flag) re-altered to INCREMENTAL is upgraded; ALTER away from INCREMENTAL keeps the flag.
- full pass on a per-record view leaves the `_sourceRID` index without churn (index entries stable across passes on an
  unchanged source).
- reopen: the `perRecord` flag survives close/open; a pre-existing view without it stays on the full pass.
- link/edge freshness: a test pinning the accepted behavior (an edge added to a source vertex does not update its row until
  the vertex changes or a manual refresh).
- source type rename: after `ALTER TYPE ... NAME`, a per-record pass still reads the renamed type (derived query cache
  invalidated).
- a record at a drained RID that is not of the source type produces no row.
- a view over a per-record view's backing type stays correct after per-record and full passes upstream.
- regression for #11475: a large per-record view, one commit, the pass reads O(1) backing rows.
- HA: the existing MV HA tests (`Issue11031MaterializedViewRefreshLeaderOnlyTest` and the ha-raft MV tests) still pass; the
  nightly Load Tests MV arm is the end-to-end check.

## Left to the builder

- Where the per-transaction buffer lives (`putDerived` vs attachment). Note `setAttachment` marks the statement
  unrestorable, which `putDerived` does not; prefer the one that does not change statement-level rollback behavior.
- How the derived per-RID query is built (parameter in FROM vs parse-tree rewrite), as long as it is built once per view.
- The overflow cap default and its configuration key name.
- The exact guard for the view-level pending delta (lock vs CAS on an immutable snapshot), within the engine-concurrency
  skill's rules.
