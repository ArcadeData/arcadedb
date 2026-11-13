# Per-record INCREMENTAL Materialized Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The refresh pass a `REFRESH INCREMENTAL` view owes a commit costs O(source records the commit changed) for views whose rows map 1:1 to source records (#11475).

**Architecture:** A stricter classifier marks "per-record" views. Their backing rows carry an indexed `_sourceRID` LINK to the source record. The change listener buffers changed RIDs per transaction; after commit they join a view-level pending delta that the refresh owner drains, re-running the defining query only for those RIDs and upserting/deleting backing rows through the `_sourceRID` index. Overflow and failures degrade to a full pass that matches rows by `_sourceRID`.

**Tech Stack:** Java 21, ArcadeDB engine (`com.arcadedb.schema`), JUnit 5 + AssertJ, Maven.

**Spec:** `docs/superpowers/specs/2026-11-11-mv-per-record-incremental-design.md`

## Global Constraints

- Property name on backing rows: `_sourceRID`, type `Type.LINK`, UNIQUE LSM index on it. Visible to readers of the view.
- Schema JSON flag on the view: `"perRecord"` (boolean, default `false` when absent).
- New config: `GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA`, key `arcadedb.materializedViewPerRecordMaxDelta`, `SCOPE.DATABASE`, `Integer`, default `10_000`.
- Only `REFRESH INCREMENTAL` views get the link (at CREATE, or via `ALTER ... REFRESH INCREMENTAL`). MANUAL/PERIODIC views created as such never do.
- Overflow full pass: on the committing thread. Failure-recovery full pass: handed to the scheduler (`MaterializedViewScheduler.handOff`); view STALE until it succeeds.
- Index creation (CREATE and ALTER upgrade) takes `databaseInternal.quiesceAsync()` OUTSIDE `recordFileChanges` (pattern: `ContinuousAggregateBuilder.create()`, #10537).
- No added cost on the commit path of a database with no per-record view; non-per-record views behave exactly as today (`writeSnapshot` untouched).
- Primitive arrays over boxed collections for RID buffers (reuse `RecordIdentities`).
- Project rules (CLAUDE.md): `final` on locals/params, no braces for single-statement ifs, AssertJ `assertThat(...).isTrue()` style, no wall-clock assertions, no fully qualified names, no `System.out`, no `git commit` (the user commits after review), compile with `mvn -o -pl engine -am compile -q` after each backend change.

## Review Focus

1. A source record updated so it leaves the WHERE clause, then back in: the row disappears and reappears (Task 3 test `updateOutOfAndBackIntoWhere`).
2. Insert and delete of the same record inside one transaction: no backing row, no exception from a lookup of a deleted RID (Task 3 test `createAndDeleteInSameTransactionLeavesNoRow`).
3. A transaction rolled back after touching a source: per-record view unchanged and no delta leaks into the next commit's pass (Task 3 test `rolledBackChangesDoNotReachTheView`).
4. A source record whose defining query evaluates to an error for that record only (e.g. a division by a zero field): the pass fails like a full pass does, the view goes non-VALID, recovery runs (Task 5 test reuses the injected-failure helper).
5. Close and reopen of the database with a per-record view: `perRecord` survives, listener re-registered, next commit takes the per-record path (Task 2 test `perRecordFlagSurvivesReopen` and Task 3 test `perRecordPathAfterReopen`).

---

### Task 1: Per-record classifier

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/MaterializedViewQueryClassifier.java`
- Modify (if no getter): `engine/src/main/java/com/arcadedb/query/sql/parser/SelectStatement.java` add `public Limit getLimit()`
- Test: `engine/src/test/java/com/arcadedb/schema/MaterializedViewPerRecordClassifierTest.java`

**Interfaces:**
- Produces: `public static boolean MaterializedViewQueryClassifier.isPerRecord(String sql, Database database)`

- [ ] **Step 1: Write the failing test** (extends `TestHelper`; create types `User` (vertex), `Other`, edge type `FriendOf` in `@BeforeEach`)

```java
@Test void userStatsShapeIsPerRecord() {
  assertThat(MaterializedViewQueryClassifier.isPerRecord(
      "SELECT id AS userId, out('FriendOf').size() AS friends FROM User", database)).isTrue();
}
@Test void plainProjectionWithWhereIsPerRecord() {
  assertThat(MaterializedViewQueryClassifier.isPerRecord("SELECT name FROM User WHERE age > 18", database)).isTrue();
}
@ParameterizedTest @ValueSource(strings = {
  "SELECT name FROM User ORDER BY name",
  "SELECT name FROM User LIMIT 10",
  "SELECT name FROM User SKIP 5",
  "SELECT DISTINCT name FROM User",
  "SELECT name FROM User LET $x = 1",
  "SELECT name, tags FROM User UNWIND tags",
  "SELECT expand(out('FriendOf')) FROM User",
  "SELECT name FROM User WHERE id IN (SELECT id FROM Other)",
  "SELECT name, (SELECT count(*) FROM Other) AS c FROM User",
  "SELECT count(*) FROM User",
  "SELECT name, count(*) FROM User GROUP BY name",
  "SELECT name AS _sourceRID FROM User",
  "SELECT FROM #1:0" })
void notPerRecord(final String sql) {
  assertThat(MaterializedViewQueryClassifier.isPerRecord(sql, database)).isFalse();
}
```

- [ ] **Step 2: Run it, expect compile failure** - `cd engine && mvn -o test -Dtest=MaterializedViewPerRecordClassifierTest -q` → "cannot find symbol isPerRecord".

- [ ] **Step 3: Implement `isPerRecord`**: `isSimple(sql, db)` AND parsed `SelectStatement` has null `getOrderBy()`, `getLimit()`, `getSkip()`, `getLetClause()`, `getUnwind()`; `!getProjection().isDistinct()`; no projection item `isExpand()`; no projection alias equal to `_sourceRID`; `MaterializedViewSources.of(db, sql).typeNames().size() == 1`; and no subquery anywhere: walk the ANTLR tree (`new SQLAntlrParser(db).parseWithTree(sql).tree()`, as `MaterializedViewSources.of` does) and reject when it contains more than one select-statement node (a subquery over the same type would pass the type-count check). Any exception → `false`.

- [ ] **Step 4: Run, expect PASS.** Also run `MaterializedViewEdgeCaseTest` (existing `isSimple` callers) → PASS.

- [ ] **Step 5: No commit** (user commits after review).

---

### Task 2: Link schema at creation and the matching full pass

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewPerRecordRefresher.java` (the per-record passes: full matching pass now, delta pass in Task 3)
- Create: `engine/src/main/java/com/arcadedb/schema/RecordIdentities.java` (moved out of `MaterializedViewRefresher`, package-private, add `int size()`, `RID get(int)`, `void addAll(RecordIdentities)`, `void sortAndDeduplicate()`, `void clear()`)
- Modify: `MaterializedViewRefresher.java` (use the extracted class; `refreshOnce` dispatches to `MaterializedViewPerRecordRefresher.fullPass` when `view.isPerRecord()`)
- Modify: `MaterializedViewImpl.java` (field `volatile boolean perRecord`, `isPerRecord()`, `setPerRecord(boolean)`, JSON `perRecord`, `copyWithRefreshMode` carries it; derived-query cache field)
- Modify: `MaterializedViewBuilder.java` (`create()`: quiesce outside the frame; for INCREMENTAL + `isPerRecord`, create property + index before initial refresh, set flag)
- Test: `engine/src/test/java/com/arcadedb/schema/MaterializedViewPerRecordTest.java`

**Interfaces:**
- Produces:
  - `static final String MaterializedViewPerRecordRefresher.SOURCE_RID = "_sourceRID"`
  - `static void MaterializedViewPerRecordRefresher.addLink(LocalSchema schema, MaterializedViewImpl view)` (property + UNIQUE index, idempotent)
  - `static void MaterializedViewPerRecordRefresher.fullPass(Database db, MaterializedViewImpl view)` (runs inside `BackingTypeWriteScope` + own tx like `writeSnapshot`)
  - `String MaterializedViewImpl.derivedQuery(boolean forRidList)` - cached; `invalidateDerivedQuery()`
  - `RecordIdentities` API above

- [ ] **Step 1: Write the failing tests**

```java
@Test void incrementalPerRecordViewGetsTheLinkAndIndex()
  // type Person(name, age), 3 rows; CREATE INCREMENTAL view "SELECT name FROM Person"
  assertThat(view.isPerRecord()).isTrue();
  assertThat(database.getSchema().getType("PV").getProperty("_sourceRID").getType()).isEqualTo(Type.LINK);
  assertThat(database.getSchema().getType("PV").getAllIndexes(false)).anyMatch(i -> i.isUnique() && i.getPropertyNames().equals(List.of("_sourceRID")));
  // every row's _sourceRID points at a Person whose name equals the row's name
@Test void manualViewGetsNoLink()                 // same query, MANUAL -> isPerRecord() false, no _sourceRID property
@Test void nonPerRecordIncrementalViewGetsNoLink() // "SELECT name FROM Person ORDER BY name" INCREMENTAL -> false
@Test void perRecordFlagSurvivesReopen()          // reopenDatabase(); view.isPerRecord() true
@Test void fullPassKeepsEachRowOnItsSource()      // capture map _sourceRID -> row @rid; REFRESH MATERIALIZED VIEW twice; map unchanged
@Test void fullPassOnUnchangedSourceDoesNotGrowTheIndex() // 2000 rows, 50 manual refreshes; reuse indexBytesOf from MaterializedViewRefreshAtomicityTest (copy the helper); size after == size after first
@Test void fullPassAddsAndRemovesRows()           // delete 1 Person + add 1 with listener-free path (MANUAL refresh), rows == SELECT result
```

- [ ] **Step 2: Run, expect FAIL** (`isPerRecord` missing).

- [ ] **Step 3: Implement.** `addLink`: `backing.createProperty(SOURCE_RID, Type.LINK)` if absent, `schema.buildTypeIndex(name, new String[]{SOURCE_RID}).withUnique(true).withIgnoreIfExists(true).create()`. `create()`: wrap the existing `recordFileChanges` in `try (final AsyncQuiesce q = databaseInternal.quiesceAsync())`; after `typeBuilder.create()`, if `refreshMode == INCREMENTAL && isPerRecord(query)` call `addLink` and `view.setPerRecord(true)` before the first `saveConfiguration()`. `derivedQuery(false)` = defining query with `@rid AS _sourceRID` added to the projection (for a `SELECT FROM X` with no projection use `SELECT *, @rid AS _sourceRID`); render from the parsed `SelectStatement` copy (`statement.copy()`, add a `ProjectionItem`, `toString()`). `fullPass`: run `derivedQuery(false)`; for each row look up `SOURCE_RID` via `database.lookupByKey(backing, SOURCE_RID, rid)`; update in place with `applyRow` (make it package-private in `MaterializedViewRefresher`) or create; collect produced source RIDs in a `RecordIdentities`, `sortAndDeduplicate()`; then scan the backing type and delete rows whose `_sourceRID` is not in it (binary search on the sorted arrays). `applyRow` must keep `_sourceRID` (it comes from the row).

- [ ] **Step 4: Run new test + `MaterializedViewTest`, `MaterializedViewRefreshAtomicityTest`, `MaterializedViewSQLTest` → PASS.** `mvn -o -pl engine -am compile -q` clean.

- [ ] **Step 5: No commit.**

---

### Task 3: Delta capture and the per-record pass on commit

**Files:**
- Modify: `MaterializedViewChangeListener.java` (per-record views buffer RIDs in `tx.putDerived(this, RecordIdentities)`; callback hands them to the view)
- Modify: `MaterializedViewImpl.java` (pending delta)
- Modify: `MaterializedViewRefresher.java` (`runOwnedPasses` drains the delta and dispatches per-record vs full)
- Modify: `MaterializedViewPerRecordRefresher.java` (`deltaPass`)
- Test: `MaterializedViewPerRecordTest.java` (add), `MaterializedViewConcurrentRefreshTest.java` (add one per-record case)

**Interfaces:**
- Consumes: Task 2 `RecordIdentities`, `derivedQuery`, `SOURCE_RID`, `applyRow`.
- Produces:
  - `void MaterializedViewImpl.appendPendingDelta(RecordIdentities rids)` and `void MaterializedViewImpl.requestFullPass(boolean recovery)` - `synchronized` on a private lock object
  - `PendingDelta MaterializedViewImpl.drainPendingDelta()` - record `PendingDelta(RecordIdentities rids, boolean full, boolean recovery)`, atomically takes and clears
  - `static void MaterializedViewPerRecordRefresher.deltaPass(Database db, MaterializedViewImpl view, RecordIdentities rids)`

- [ ] **Step 1: Write the failing tests** (type `Person` with subtype `Employee`, INCREMENTAL view `SELECT name, age FROM Person WHERE age >= 18`; helper `assertViewMatchesQuery()` compares the view rows (name, age) with the defining query's rows as multisets)

```java
@Test void createUpdateDeleteKeepTheViewEqualToTheQuery()
@Test void updateOutOfAndBackIntoWhere()            // age 20 -> 10: row gone; -> 30: row back with same _sourceRID
@Test void subtypeRecordIsMaintained()              // new Employee row appears
@Test void createAndDeleteInSameTransactionLeavesNoRow()
@Test void rolledBackChangesDoNotReachTheView()     // begin; update; rollback; then an unrelated commit; view still equal to query
@Test void readYourWritesAfterCommit()              // commit then immediate query on same thread sees the row
@Test void onePassTouchesOnlyChangedRows()
  // 5000 Person rows; snapshot stats; update ONE person in a tx; diff of getStats() "updateRecord"+"createRecord"+"deleteRecord"
  assertThat(writesDelta).isLessThanOrEqualTo(4); // 1 source update + 1 view row write, small slack
@Test void edgeChangeDoesNotRefreshTraversalColumnUntilItsRowChanges() // vertex User, view "SELECT id, out('FriendOf').size() AS f FROM User"; add edge -> f unchanged; touch the User -> f updated
@Test void perRecordPathAfterReopen()               // reopen, update one row, onePassTouchesOnlyChangedRows-style bound holds
@Test void recordAtDrainedRidOfAnotherTypeProducesNoRow() // call deltaPass directly with a RID of type Other -> no row, no exception
```
And in `MaterializedViewConcurrentRefreshTest`: `concurrentWritersOnAPerRecordViewLoseNoDelta` - copy `concurrentWritersLeaveTheViewReflectingEveryCommit` with a per-record view, then `assertViewMatchesQuery`.

- [ ] **Step 2: Run, expect FAIL** (bound in `onePassTouchesOnlyChangedRows` fails: today's pass rewrites 5000 rows).

- [ ] **Step 3: Implement.**
  - Listener: if `view.isPerRecord()`, get or create the tx buffer via `getDerived/putDerived` (not `setAttachment`: it marks the statement unrestorable), `add(record.getIdentity())`. The after-commit callback (same `callbackKey`) does `view.appendPendingDelta(buffer)` then `MaterializedViewRefresher.refreshAfterCommit(db, view)`. Non-per-record views: unchanged code path.
  - `runOwnedPasses`: at the start of each pass, if `view.isPerRecord()`, `drainPendingDelta()`; `full` → `MaterializedViewPerRecordRefresher.fullPass`, else if rids non-empty → `deltaPass`, else skip the write. Non-per-record → `refreshOnce` as today.
  - `fullPass` drains the pending delta first (spec section 6) - covered by the drain at pass start; manual `REFRESH` and scheduler passes go through the same `runOwnedPasses`, so they drain too.
  - `deltaPass`: `rids.sortAndDeduplicate()`; in own tx under `BackingTypeWriteScope`: run `derivedQuery(true)` = derived projection + `FROM :rids` (pass a `List<RID>`) + WHERE `(<original where>) AND @this INSTANCEOF '<source>'`; for each result row upsert by `_sourceRID` index; collect hit RIDs; for each drained RID not hit, delete the backing row found by the index, if any.

- [ ] **Step 4: Run Task 3 tests + `MaterializedViewIncrementalTest`, `MaterializedViewConcurrentRefreshTest`, `MaterializedViewEdgeCaseTest`, `MaterializedViewRegressionTest.MaterializedViewRefreshLeaderOnly` → PASS.**

- [ ] **Step 5: No commit.**

---

### Task 4: Overflow cap

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (new entry after `INDEX_DEFAULT_PAGE_SIZE` group or near other MV-related entries)
- Modify: `MaterializedViewChangeListener.java`, `MaterializedViewImpl.java`
- Test: `MaterializedViewPerRecordTest.java`

**Interfaces:**
- Consumes: Task 3 `requestFullPass(boolean)`, `appendPendingDelta`.

- [ ] **Step 1: Failing tests**

```java
@Test void transactionPastTheCapTriggersACorrectFullPassInline()
  // set MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA = 10 on the database config; update 50 Persons in one tx;
  // right after commit (same thread): assertViewMatchesQuery(); view.getStatus() == "VALID"
@Test void pendingDeltaPastTheCapCollapsesToFull()
  // appendPendingDelta of 11 RIDs with cap 10 while a pass is held running (tryBeginRefresh() in the test);
  // drainPendingDelta().full() isTrue, rids().size() == 0
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.** Description string: "Maximum number of changed source records a REFRESH INCREMENTAL per-record materialized view re-evaluates in one pass; past it the pass rewrites the whole view instead. Default 10000". Listener: when the tx buffer exceeds the cap, drop it and keep a per-tx full mark (store a sentinel in `putDerived`), callback calls `requestFullPass(false)`. `appendPendingDelta` collapses to full when the merged size exceeds the cap. A `full && !recovery` pass runs inline (committing thread), as any pass does today.

- [ ] **Step 4: Run Task 3+4 tests → PASS.**

- [ ] **Step 5: No commit.**

---

### Task 5: Failure recovery handed to the scheduler

**Files:**
- Modify: `MaterializedViewRefresher.java` (`runOwnedPasses` catch block; pass dispatch)
- Test: `MaterializedViewPerRecordTest.java`

**Interfaces:**
- Consumes: `requestFullPass(true)`, `drainPendingDelta()`, existing `handOffFurtherPasses`.

- [ ] **Step 1: Failing tests** (copy `failRefreshOnRecord` from `MaterializedViewRefreshAtomicityTest`, with an unregister hook)

```java
@Test void failedDeltaPassLeavesViewNonValidAndRecoversOffTheCommittingThread()
  // arm failure on the 1st backing write; update one Person -> view status not VALID
  // disarm; update another Person; record Thread name inside a BeforeRecordUpdateListener on the backing type
  // Awaitility.await().atMost(30s).until(() -> "VALID".equals(view.getStatus()));
  // assertViewMatchesQuery(); recorded full-pass thread name startsWith "ArcadeDB-MV-Scheduler"
@Test void commitsDuringRecoveryAreNotLost()
  // after the failure, 20 commits from the test thread while recovery runs; await VALID; assertViewMatchesQuery()
@Test void perRecordFailureNeverEndsValidWithoutAFullPass()
  // fail the delta pass; disarm; make a per-record-sized commit; await VALID; the full-pass marker listener fired at least once
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.** In the catch of `runOwnedPasses`, when `view.isPerRecord()`, call `view.requestFullPass(true)` before releasing ownership (the drained RIDs are gone). At pass dispatch: if the drained delta is `full && recovery` and `mayHandOffFurtherPasses`, put it back (`requestFullPass(true)`), and hand off through `handOffFurtherPasses`; set status STALE (persisted via `persistStatus`) before handing off; if hand-off returns false, run it inline. The scheduler-side `runOwnedPasses` sees the same flag but with ownership on the scheduler thread runs it (`mayHandOff` true there too: distinguish by checking `Thread.currentThread()` against the scheduler, or pass an explicit `onScheduler` boolean through the hand-off lambda - prefer the explicit parameter).

- [ ] **Step 4: Run all `MaterializedView*Test` in engine → PASS.**

- [ ] **Step 5: No commit.**

---

### Task 6: ALTER upgrade, source rename, views over views

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java` (`alterMaterializedView`, ~line 2904)
- Modify: `MaterializedViewImpl.java` (`retargetSourceType` → `invalidateDerivedQuery()`; `copyWithRefreshMode` carries `perRecord`, fresh cache)
- Test: `engine/src/test/java/com/arcadedb/schema/MaterializedViewPerRecordAlterTest.java`

**Interfaces:**
- Consumes: Task 2 `addLink`, `fullPass`, `setPerRecord`.

- [ ] **Step 1: Failing tests**

```java
@Test void manualViewAlteredToIncrementalIsUpgraded()      // MANUAL per-record-shaped view; ALTER ... REFRESH INCREMENTAL; isPerRecord() true; _sourceRID on every row; one update -> onePass bound holds
@Test void legacyIncrementalViewIsUpgradedByReAlter()      // create INCREMENTAL view, then simulate legacy: setPerRecord(false), drop index + property, saveConfiguration, reopen; ALTER ... REFRESH INCREMENTAL -> upgraded
@Test void alterAwayFromIncrementalKeepsTheLink()          // per-record view; ALTER ... REFRESH MANUAL; isPerRecord() true, index present; REFRESH keeps _sourceRID->row map
@Test void nonPerRecordQueryAlteredToIncrementalStaysFull() // ORDER BY view MANUAL -> INCREMENTAL; isPerRecord() false
@Test void sourceRenameKeepsPerRecordPassWorking()         // ALTER TYPE Person NAME People; insert People row; view has it
@Test void viewOverAPerRecordViewStaysCorrect()            // V2 INCREMENTAL "SELECT name FROM V1" where V1 is per-record over Person; updates to Person -> V2 equals its query; manual REFRESH of V1 -> V2 still equal
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.** `alterMaterializedView`: wrap `recordFileChanges` in `try (final AsyncQuiesce q = database.quiesceAsync())`; inside, after `copyWithRefreshMode`, if `newMode == INCREMENTAL && !newView.isPerRecord() && MaterializedViewQueryClassifier.isPerRecord(query, database)`: `addLink`, `setPerRecord(true)`, then after `saveConfiguration()` run `MaterializedViewRefresher.fullRefresh` (dispatches to `fullPass`) before registering listeners; `saveConfiguration()` again. Views-over-views need no code if Tasks 3-4 hold; the test proves it.

- [ ] **Step 4: Run Task 6 tests + `AlterMaterializedViewRefreshTest`, `MaterializedViewRegressionTest.MaterializedViewLifecycleUnderWriteLock`, `FailedMaterializedViewRestoreIsUndoneTest` → PASS.**

- [ ] **Step 5: No commit.**

---

### Task 7: Cross-module verification

**Files:** none new.

- [ ] **Step 1:** `mvn -o -pl engine -am install -DskipTests -q` then `mvn -o -pl server -Dtest='*MaterializedView*' -Dsurefire.failIfNoSpecifiedTests=false test` → PASS (server-side MV SQL/HTTP tests).
- [ ] **Step 2:** `mvn -o -pl ha-raft -Dtest='*MaterializedView*' -Dsurefire.failIfNoSpecifiedTests=false test` → PASS (leader-only refresh, replication of the link schema). If the module has no matching test, record that in the PR body.
- [ ] **Step 3:** `mvn -o -pl engine test -Dtest='MaterializedView*Test,Issue*MaterializedView*Test'` → PASS; Maven `Results:` line quoted in the PR body.
- [ ] **Step 4:** PR body notes (no tracking .md in the repo): BEHAVIOR CHANGES for the release note - link/edge columns refresh only with their own row; `_sourceRID` column on per-record views; `ALTER ... REFRESH INCREMENTAL` upgrades existing views; new config key. Load Tests nightly MV arm (#11464) is the end-to-end confirmation, owed after merge.
