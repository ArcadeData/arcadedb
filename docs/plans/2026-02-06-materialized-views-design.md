# Materialized Views Design Document

**Goal:** Add materialized views to ArcadeDB — a schema-level abstraction that stores the results of a defining query as a regular document type, with support for full refresh, incremental (event-driven) refresh, and transparent querying via SQL.

**Architecture:** A materialized view is a special `DocumentType` that wraps a defining SQL SELECT query. Its data lives in standard buckets and can be indexed like any other type. Refresh is driven by post-commit callbacks on `TransactionContext` (for incremental) or by re-executing the defining query (for full). Schema persistence uses the existing `schema.json` mechanism with a new `"materializedViews"` top-level section.

**Tech Stack:** Java 21, ArcadeDB engine (schema, events, transactions, query, indexing), SQL parser (ANTLR4)

---

## 1. Concepts

| Term | Definition |
|------|-----------|
| **Materialized View** | A named, persisted result set derived from a SQL SELECT query. Stored as documents in buckets. |
| **Defining Query** | The SQL SELECT statement whose result set populates the view. |
| **Source Types** | The types referenced in the defining query's FROM clause. Changes to records of these types can trigger refresh. |
| **Full Refresh** | Truncate the view's buckets and re-execute the defining query, inserting all results. |
| **Incremental Refresh** | Use `AfterRecordCreate/Update/Delete` listeners on source types to detect changes, then apply updates after transaction commit via post-commit callbacks. |
| **Staleness** | A view is stale when source data has changed since the last refresh. |
| **Eventual Consistency** | In `INCREMENTAL` mode, the view is updated in a separate transaction after the source transaction commits. The view may briefly lag by one transaction. |

---

## 2. Data Model

### 2.1 MaterializedView Interface

New interface in `com.arcadedb.schema`:

```java
public interface MaterializedView {
  String getName();
  String getQuery();
  DocumentType getBackingType();
  List<String> getSourceTypeNames();
  MaterializedViewRefreshMode getRefreshMode();
  long getLastRefreshTime();
  String getStatus();   // VALID, STALE, BUILDING, ERROR

  void refresh();       // Full refresh
  void drop();
}
```

### 2.2 Refresh Modes

```java
public enum MaterializedViewRefreshMode {
  MANUAL,         // Only refreshed on explicit REFRESH command
  INCREMENTAL,    // Automatic: post-commit callback refreshes after source record mutations
  PERIODIC        // Scheduled: background thread refreshes at configurable interval
}
```

### 2.3 Schema Persistence (schema.json)

Add a new top-level `"materializedViews"` section alongside existing `"types"` and `"triggers"`:

```json
{
  "schemaVersion": 42,
  "settings": { ... },
  "types": { ... },
  "triggers": { ... },
  "materializedViews": {
    "SalesSummary": {
      "query": "SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer",
      "backingType": "SalesSummary",
      "sourceTypes": ["Order"],
      "refreshMode": "MANUAL",
      "lastRefreshTime": 1738800000000,
      "status": "VALID"
    }
  }
}
```

This mirrors the pattern used for triggers: a dedicated section in `schema.json`, loaded/saved in `LocalSchema.readConfiguration()` / `toJSON()`.

---

## 3. Schema API

### 3.1 Schema Interface Additions

Add to `Schema.java`:

```java
// Materialized View management
boolean existsMaterializedView(String viewName);
MaterializedView getMaterializedView(String viewName);
MaterializedView[] getMaterializedViews();
void dropMaterializedView(String viewName);

// Builder
MaterializedViewBuilder buildMaterializedView();
```

### 3.2 MaterializedViewBuilder

Follows the same builder pattern as `TypeBuilder`:

```java
public class MaterializedViewBuilder {
  private final DatabaseInternal database;
  private String name;
  private String query;
  private MaterializedViewRefreshMode refreshMode = MaterializedViewRefreshMode.MANUAL;
  private int buckets;         // defaults from GlobalConfiguration
  private int pageSize;        // defaults from GlobalConfiguration
  private long refreshInterval; // for PERIODIC mode, in ms
  private boolean ifNotExists = false;

  public MaterializedViewBuilder withName(String name);
  public MaterializedViewBuilder withQuery(String query);
  public MaterializedViewBuilder withRefreshMode(MaterializedViewRefreshMode mode);
  public MaterializedViewBuilder withTotalBuckets(int buckets);
  public MaterializedViewBuilder withPageSize(int pageSize);
  public MaterializedViewBuilder withRefreshInterval(long intervalMs);
  public MaterializedViewBuilder withIgnoreIfExists(boolean ignore);
  public MaterializedView create();
}
```

The `create()` method:
1. Validates the defining query by parsing it (does not execute)
2. Extracts source type names from the FROM clause
3. Creates a schema-less backing `DocumentType` (with buckets) — properties are created implicitly when documents are inserted during the first refresh
4. Persists the view definition in schema.json
5. Registers event listeners if refresh mode is `INCREMENTAL`
6. Executes an initial full refresh
7. Returns the `MaterializedView`

### 3.3 Java API Usage

```java
database.getSchema().buildMaterializedView()
    .withName("SalesSummary")
    .withQuery("SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer")
    .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
    .create();

// Query the view like any other type
ResultSet rs = database.query("sql", "SELECT * FROM SalesSummary WHERE total > 1000");

// Manual refresh
database.getSchema().getMaterializedView("SalesSummary").refresh();
```

---

## 4. SQL Syntax

### 4.1 CREATE MATERIALIZED VIEW

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name>
  AS <select-statement>
  [REFRESH <MANUAL | INCREMENTAL | EVERY <interval> <SECOND|MINUTE|HOUR>>]
  [BUCKETS <n>]
```

Examples:

```sql
-- Manual refresh (default)
CREATE MATERIALIZED VIEW SalesSummary
  AS SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer;

-- Incremental refresh on source type changes
CREATE MATERIALIZED VIEW ActiveUsers
  AS SELECT name, email FROM User WHERE active = true
  REFRESH INCREMENTAL;

-- Periodic refresh every 5 minutes
CREATE MATERIALIZED VIEW DailyStats
  AS SELECT date, COUNT(*) AS cnt FROM Event GROUP BY date
  REFRESH EVERY 5 MINUTE;
```

### 4.2 REFRESH MATERIALIZED VIEW

```sql
REFRESH MATERIALIZED VIEW <name>;
```

Forces a full refresh regardless of refresh mode.

### 4.3 DROP MATERIALIZED VIEW

```sql
DROP MATERIALIZED VIEW [IF EXISTS] <name>;
```

Drops the view definition, backing type, buckets, and unregisters listeners.

### 4.4 ALTER MATERIALIZED VIEW

```sql
ALTER MATERIALIZED VIEW <name> REFRESH <MANUAL | INCREMENTAL | EVERY <interval> <SECOND|MINUTE|HOUR>>;
```

Changes the refresh mode. Registers/unregisters listeners as needed.

### 4.5 Querying

Materialized views are queried like normal types — no special syntax:

```sql
SELECT * FROM SalesSummary WHERE total > 1000 ORDER BY total DESC;
```

The query planner treats the view's backing type identically to any other `DocumentType`, using its buckets and indexes.

---

## 5. Implementation Plan

### Phase 1: Core Infrastructure

#### Task 1.1: MaterializedView Interface & MaterializedViewImpl

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedView.java`
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewImpl.java`
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefreshMode.java`

**MaterializedViewImpl** stores:
- `name` — view name
- `query` — defining SQL string
- `backingTypeName` — name of the DocumentType holding data
- `sourceTypeNames` — list of type names referenced in FROM
- `refreshMode` — MANUAL, INCREMENTAL, or PERIODIC
- `lastRefreshTime` — epoch millis of last successful refresh
- `status` — VALID, STALE, BUILDING, ERROR
- `refreshInterval` — for PERIODIC mode

Serialization to/from JSON follows the `TriggerImpl.fromJSON()` / `toJSON()` pattern.

#### Task 1.2: MaterializedViewBuilder

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java`

Builder pattern matching `TypeBuilder`. The `create()` method:
1. Parses the defining query to validate syntax
2. Extracts source types from the parsed `SelectStatement`'s FROM clause
3. Creates a schema-less backing `DocumentType` via `schema.buildDocumentType().withName(name)...create()`
4. Stores the `MaterializedViewImpl` in `LocalSchema.materializedViews` map
5. Saves schema configuration
6. Registers event listeners if refresh mode is `INCREMENTAL`
7. Performs initial full refresh

#### Task 1.3: LocalSchema Extensions

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/Schema.java`
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`

Add to `LocalSchema`:
- `Map<String, MaterializedViewImpl> materializedViews` field
- `createMaterializedView()`, `getMaterializedView()`, `dropMaterializedView()`, etc.
- Extend `toJSON()` to serialize the `materializedViews` map
- Extend `readConfiguration()` to load materialized views from JSON and re-register listeners for `INCREMENTAL` views
- Extend `dropType()` to prevent dropping a type that is a backing type for a view
- Extend `close()` to shut down periodic refresh schedulers

#### Task 1.4: Full Refresh Logic

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java`

The refresher:
1. Sets view status to `BUILDING`
2. Begins a transaction
3. Truncates the backing type via Java API (iterates buckets and deletes records, or uses `type.truncate()`)
4. Executes the defining query via `database.query("sql", definingQuery)`
5. Iterates the result set and inserts each result as a document in the backing type
6. Commits
7. Updates `lastRefreshTime` and sets status to `VALID`
8. On error: rolls back, sets status to `ERROR`, logs the exception

#### Task 1.5: Crash Recovery

On database open, in `LocalSchema.readConfiguration()`, after loading materialized views:
- If any view has status `BUILDING`, set it to `STALE` (interrupted refresh)
- Schedule an automatic full refresh for all `STALE` views

This ensures that if the system crashes between a source commit and the post-commit view refresh, the view is automatically recovered on restart.

---

### Phase 2: SQL Parser Integration

#### Task 2.1: CREATE MATERIALIZED VIEW Statement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/CreateMaterializedViewStatement.java`
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4` (add new keywords)
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4` (add grammar rules)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java` (add visitor methods)

The statement class extends `DDLStatement` and implements `executeDDL()` following the same pattern as `CreateTriggerStatement`:
1. Validate inputs
2. Delegate to `database.getSchema().buildMaterializedView()...create()`
3. Return result set with `{ "operation": "create materialized view", "name": viewName }`

ANTLR4 grammar rules (added to `SQLParser.g4`):

```antlr
createMaterializedViewBody
    : (IF NOT EXISTS)? identifier
      AS selectStatement
      materializedViewRefreshClause?
      (BUCKETS POSITIVE_INTEGER)?
    ;

materializedViewRefreshClause
    : REFRESH MANUAL
    | REFRESH INCREMENTAL
    | REFRESH EVERY POSITIVE_INTEGER materializedViewTimeUnit
    ;

materializedViewTimeUnit
    : SECOND | MINUTE | HOUR
    ;
```

New keywords added to `SQLLexer.g4`: `MATERIALIZED`, `VIEW`, `REFRESH`, `EVERY`, `SECOND`, `MINUTE`, `HOUR`, `MANUAL`, `INCREMENTAL` (only those not already present in the lexer).

Visitor methods added to `SQLASTBuilder.java` following the existing `visitCreateTriggerStmt` pattern.

#### Task 2.2: DROP MATERIALIZED VIEW Statement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/DropMaterializedViewStatement.java`

Extends `DDLStatement`. Calls `schema.dropMaterializedView(name)`.

#### Task 2.3: REFRESH MATERIALIZED VIEW Statement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/RefreshMaterializedViewStatement.java`

Extends `DDLStatement`. Calls `schema.getMaterializedView(name).refresh()`.

#### Task 2.4: ALTER MATERIALIZED VIEW Statement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/AlterMaterializedViewStatement.java`

Extends `DDLStatement`. Modifies refresh mode and re-registers/unregisters listeners.

---

### Phase 3: Transaction Callback Infrastructure

#### Task 3.1: Post-Commit Callbacks on TransactionContext

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/database/TransactionContext.java`

Add a general-purpose post-commit callback mechanism:

```java
// In TransactionContext
private List<Runnable> afterCommitCallbacks;

public void addAfterCommitCallback(final Runnable callback) {
  if (afterCommitCallbacks == null)
    afterCommitCallbacks = new ArrayList<>();
  afterCommitCallbacks.add(callback);
}
```

In `commit2ndPhase()`, after the commit succeeds but before `reset()`, fire all callbacks:

```java
// After successful commit, before reset()
if (afterCommitCallbacks != null) {
  for (final Runnable callback : afterCommitCallbacks) {
    try {
      callback.run();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error in post-commit callback: %s", e, e.getMessage());
    }
  }
}
```

Callbacks are cleared on both `commit()` (after firing) and `rollback()` (without firing). This ensures:
- On successful commit: callbacks fire exactly once
- On rollback: no callbacks fire, view stays consistent
- Each callback failure is logged but doesn't affect other callbacks or the commit

This mechanism is general-purpose — other ArcadeDB features can use it in the future.

---

### Phase 4: Incremental Refresh

#### Task 4.1: MaterializedViewChangeListener

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewChangeListener.java`

Implements `AfterRecordCreateListener`, `AfterRecordUpdateListener`, `AfterRecordDeleteListener`. Registered on each source type's `RecordEventsRegistry`.

The listener does NOT modify the view directly. Instead, it:
1. Marks the view as dirty in the current `TransactionContext` (via a `Set<String> dirtyMaterializedViews`)
2. For simple queries: buffers the individual change (insert/update/delete + source record RID) in a thread-local or transaction-scoped buffer
3. Registers a post-commit callback on the `TransactionContext` (only once per transaction per view)

The post-commit callback then applies the changes in a **new transaction**:

**Simple queries** (no aggregation, no JOIN, no GROUP BY, no TRAVERSE):
- Applies buffered per-record changes:
  - `AfterRecordCreate`: If the record passes the WHERE filter, insert a corresponding document in the view with `_sourceRID` set to the source record's RID
  - `AfterRecordUpdate`: Look up the view document by `_sourceRID`, re-evaluate the WHERE filter, update or delete accordingly
  - `AfterRecordDelete`: Look up by `_sourceRID`, delete from view

**Complex queries** (aggregation, GROUP BY, JOIN, TRAVERSE, subqueries):
- Triggers a single full refresh of the view

The listener is designed to fail gracefully — if an error occurs during post-commit refresh, the view is marked `STALE` rather than corrupting data.

#### Task 4.2: Source RID Tracking

View documents for simple-query views include a special internal property `_sourceRID` that stores the RID of the source record that produced the view row. This enables efficient lookup for updates and deletes.

- `_sourceRID` is stored as a `String` property (e.g., `"#12:45"`) on each view document
- An automatic index on `_sourceRID` is created on the backing type for O(log n) lookups
- `_sourceRID` is only used for simple-query views; complex-query views do full refresh and don't need it

#### Task 4.3: Query Complexity Classification

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewQueryClassifier.java`

Analyzes the parsed `SelectStatement` to determine if the defining query is "simple" or "complex":

**Simple** (eligible for per-record incremental):
- Single type in FROM clause (no JOINs)
- No GROUP BY
- No aggregate functions (SUM, COUNT, AVG, MIN, MAX)
- No subqueries
- No TRAVERSE
- Projection is field references and/or aliases only

**Complex** (falls back to full refresh on change):
- Everything else

The classification is determined at view creation time and stored in `MaterializedViewImpl` (e.g., `boolean simpleQuery`).

#### Task 4.4: Listener Registration/Unregistration

Handled in `MaterializedViewBuilder.create()` and `LocalSchema.dropMaterializedView()`:
- On create with `INCREMENTAL`: instantiate `MaterializedViewChangeListener` and register on each source type's events.
- On drop: unregister the listener from all source types.
- On schema reload: re-register listeners for all `INCREMENTAL` views (in `readConfiguration()`).

Follows the same adapter pattern as `TriggerListenerAdapter` in `LocalSchema.registerTriggerListener()`.

---

### Phase 5: Periodic Refresh

#### Task 5.1: Periodic Refresh Scheduler

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewScheduler.java`

Uses a `ScheduledExecutorService` (single daemon thread) to periodically invoke full refresh.

- Started when a view with `PERIODIC` mode is created or loaded from schema.
- Stopped when the view is dropped or the database is closed.
- The scheduler holds a weak reference to the database to avoid preventing GC.
- Errors during refresh are logged and the view is marked `ERROR`.

Lifecycle:
- `LocalSchema.readConfiguration()` → start schedulers for PERIODIC views
- `LocalSchema.close()` → shut down the executor service
- `MaterializedViewBuilder.create()` with PERIODIC → start a new scheduled task
- `LocalSchema.dropMaterializedView()` → cancel the scheduled task

---

### Phase 6: HTTP/REST API

#### Task 6.1: REST Endpoints

No new handler classes needed. The existing `PostCommandHandler` dispatches SQL commands, so all operations work via:

```
POST /api/v1/command/{database}
{ "language": "sql", "command": "CREATE MATERIALIZED VIEW ..." }
```

```
POST /api/v1/command/{database}
{ "language": "sql", "command": "REFRESH MATERIALIZED VIEW SalesSummary" }
```

Querying works identically to querying any type:

```
POST /api/v1/query/{database}
{ "language": "sql", "command": "SELECT * FROM SalesSummary" }
```

#### Task 6.2: Schema Introspection Endpoint

The existing `GET /api/v1/schema/{database}` returns the schema JSON. The `materializedViews` section will be included automatically since `LocalSchema.toJSON()` is extended in Phase 1.

---

### Phase 7: Testing

#### Task 7.1: Unit Tests

**File:** `engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java`

| Test | Description |
|------|-------------|
| `createAndQueryView` | Create view, verify data is populated, query it |
| `fullRefresh` | Modify source data, refresh, verify view is updated |
| `dropView` | Drop view, verify backing type and buckets are removed |
| `viewSurvivesReopen` | Create view, reopen database, verify view still exists and is queryable |
| `createViewIfNotExists` | Verify IF NOT EXISTS semantics |
| `dropViewIfExists` | Verify IF EXISTS semantics |
| `cannotDropBackingType` | Verify dropping the backing type directly throws an error |
| `viewWithWhereClause` | View with a filter only includes matching records |
| `viewWithAggregation` | View with GROUP BY and SUM produces correct aggregated data |
| `viewWithJoin` | View joining two types produces correct joined data |
| `createViewOnNonExistentType` | Verify error for invalid defining query |
| `schemaLessBackingType` | Verify backing type has no predefined properties; properties emerge from data |
| `emptySourceType` | View on an empty source type produces zero records |

#### Task 7.2: Incremental Refresh Tests

**File:** `engine/src/test/java/com/arcadedb/schema/MaterializedViewIncrementalTest.java`

| Test | Description |
|------|-------------|
| `insertPropagatesAfterCommit` | Insert into source type, commit → view updated in post-commit callback |
| `updatePropagatesAfterCommit` | Update source record, commit → view updated |
| `deletePropagatesAfterCommit` | Delete source record, commit → removed from view |
| `rollbackDoesNotAffectView` | Insert into source type, rollback → view unchanged |
| `filterRespected` | Insert non-matching record, commit → not added to view |
| `bulkInsertSingleRefresh` | Insert 100 records in one transaction → single post-commit refresh, not 100 |
| `complexQueryFullRefreshAfterCommit` | Aggregation view triggers full refresh after source commit |
| `sourceRidTracking` | Verify `_sourceRID` is stored and indexed on simple-query views |
| `crashRecoveryStaleView` | Simulate crash during refresh → view marked STALE on reopen → auto-refreshed |

#### Task 7.3: Transaction Callback Tests

**File:** `engine/src/test/java/com/arcadedb/database/TransactionCallbackTest.java`

| Test | Description |
|------|-------------|
| `callbackFiresAfterCommit` | Register callback, commit → callback fires |
| `callbackNotFiredOnRollback` | Register callback, rollback → callback does not fire |
| `multipleCallbacksFire` | Register multiple callbacks → all fire in order |
| `callbackErrorDoesNotAffectCommit` | Callback throws exception → commit still succeeds, error logged |

#### Task 7.4: SQL Syntax Tests

**File:** `engine/src/test/java/com/arcadedb/schema/MaterializedViewSQLTest.java`

| Test | Description |
|------|-------------|
| `createViaSql` | `CREATE MATERIALIZED VIEW ... AS SELECT ...` |
| `createWithRefreshMode` | `... REFRESH INCREMENTAL` |
| `createWithBuckets` | `... BUCKETS 4` |
| `createWithPeriodic` | `... REFRESH EVERY 5 MINUTE` |
| `refreshViaSql` | `REFRESH MATERIALIZED VIEW ...` |
| `dropViaSql` | `DROP MATERIALIZED VIEW ...` |
| `alterRefreshMode` | `ALTER MATERIALIZED VIEW ... REFRESH MANUAL` |

#### Task 7.5: Server Integration Tests

**File:** `server/src/test/java/com/arcadedb/server/MaterializedViewServerIT.java`

| Test | Description |
|------|-------------|
| `createViewViaRest` | POST command to create view, then query it |
| `refreshViewViaRest` | POST command to refresh view |
| `schemaEndpointIncludesViews` | GET schema contains materializedViews section |

---

## 6. Key Design Decisions

### 6.1 Backing Type vs. Virtual View

**Decision:** Materialized views store data in a real `DocumentType` with buckets.

**Rationale:**
- Queries on the view use the exact same execution pipeline as any other type (no query rewriting needed).
- Indexes can be created on the view's backing type for fast lookups.
- No changes needed in the query planner or executor.
- The `FetchFromTypeExecutionStep` works as-is.

**Trade-off:** Storage overhead. Each materialized view consumes disk space proportional to its result set. This is the fundamental trade-off of materialized views (compute <-> storage).

### 6.2 Schema-Less Backing Type

**Decision:** The backing type is created without predefined properties. Properties emerge implicitly when documents are inserted during the first full refresh.

**Rationale:**
- Inferring property types from a SELECT projection without executing it is non-trivial (requires resolving aggregate return types, expression types, etc.).
- Schema-less documents are a first-class feature in ArcadeDB.
- Properties are inferred naturally when the first refresh inserts data.
- Users can still manually add property constraints or indexes after creation.

### 6.3 Incremental Refresh Strategy

**Decision:** `INCREMENTAL` mode uses a two-tier strategy based on query complexity, with all changes applied post-commit via `TransactionContext` callbacks.

**Tier 1 — Simple queries** (single type, no aggregation, no JOIN, no TRAVERSE):
- Per-record changes are buffered during the transaction
- After commit, buffered changes are applied in a new transaction using `_sourceRID` for correlation

**Tier 2 — Complex queries** (aggregation, GROUP BY, JOIN, TRAVERSE, subqueries):
- The view is marked dirty during the transaction
- After commit, a single full refresh is triggered in a new transaction

**Rationale:**
- Post-commit callbacks ensure changes are only applied after successful commit (rollback = no view update)
- Batching within a transaction avoids thrashing (1,000 inserts = 1 refresh, not 1,000)
- `_sourceRID` enables efficient per-record correlation for simple queries
- Full refresh fallback for complex queries is correct and avoids the complexity of partial aggregation recomputation

**Trade-off:** Eventual consistency — the view may briefly lag by one transaction. This is acceptable for a materialized view (which is inherently a cached snapshot). Crash recovery handles the edge case where the system fails between source commit and view update.

### 6.4 Post-Commit Callback Mechanism

**Decision:** Add a general-purpose `addAfterCommitCallback(Runnable)` method to `TransactionContext`.

**Rationale:**
- ArcadeDB currently has no transaction-level event hooks — only record-level listeners
- Record-level listeners (`AfterRecord*`) fire per-record within the transaction, before commit
- A post-commit callback allows batching changes and applying them only after successful commit
- The mechanism is general-purpose — other features can use it in the future
- Callbacks are cleared on rollback (never fire) and after commit (fire exactly once)

### 6.5 View Metadata in schema.json

**Decision:** Store view definitions in a new `"materializedViews"` section in schema.json, separate from `"types"`.

**Rationale:**
- The backing type is a regular type in the `"types"` section (with buckets, properties, indexes).
- The view metadata (query, source types, refresh mode, status) is orthogonal to the type definition.
- This mirrors how `"triggers"` are stored separately from `"types"`.
- On schema load, the view metadata is linked to its backing type by name.

### 6.6 Thread Safety

**Decision:** Use the existing `RecordEventsRegistry` (which uses `CopyOnWriteArrayList`) for listener registration. Refresh operations run in their own transaction.

**Rationale:**
- The event system is already thread-safe.
- Full refresh runs in a new transaction, providing ACID guarantees.
- Periodic refresh uses a single-thread `ScheduledExecutorService` to avoid concurrent refreshes.
- Post-commit callbacks run after the committing transaction completes, avoiding transaction nesting issues.

### 6.7 View Identity

**Decision:** Materialized view names must not conflict with existing type names (since the backing type uses the same name).

**Rationale:**
- Allows `SELECT FROM ViewName` to work without any query rewriting.
- The name uniqueness is enforced by the existing `LocalSchema.types` map.

---

## 7. Files to Create/Modify

### New Files (engine module)

| File | Purpose |
|------|---------|
| `schema/MaterializedView.java` | Interface |
| `schema/MaterializedViewImpl.java` | Implementation |
| `schema/MaterializedViewRefreshMode.java` | Enum (MANUAL, INCREMENTAL, PERIODIC) |
| `schema/MaterializedViewBuilder.java` | Builder |
| `schema/MaterializedViewRefresher.java` | Full refresh logic |
| `schema/MaterializedViewChangeListener.java` | Incremental refresh listener (marks dirty + buffers changes + registers post-commit callback) |
| `schema/MaterializedViewQueryClassifier.java` | Determines if a defining query is simple or complex |
| `schema/MaterializedViewScheduler.java` | Periodic refresh scheduler |
| `query/sql/parser/CreateMaterializedViewStatement.java` | SQL CREATE |
| `query/sql/parser/DropMaterializedViewStatement.java` | SQL DROP |
| `query/sql/parser/RefreshMaterializedViewStatement.java` | SQL REFRESH |
| `query/sql/parser/AlterMaterializedViewStatement.java` | SQL ALTER |

### Modified Files (engine module)

| File | Change |
|------|--------|
| `database/TransactionContext.java` | Add `afterCommitCallbacks` list, `addAfterCommitCallback()`, fire in `commit2ndPhase()`, clear on `rollback()` |
| `schema/Schema.java` | Add materialized view methods |
| `schema/LocalSchema.java` | Add `materializedViews` map, extend `toJSON()`, `readConfiguration()`, `close()`, `dropType()`; crash recovery for STALE views |
| `query/sql/grammar/SQLLexer.g4` | Add new keywords (MATERIALIZED, VIEW, REFRESH, EVERY, SECOND, MINUTE, HOUR, MANUAL, INCREMENTAL) |
| `query/sql/grammar/SQLParser.g4` | Add grammar rules for MATERIALIZED VIEW statements (CREATE, DROP, REFRESH, ALTER) |
| `query/sql/antlr/SQLASTBuilder.java` | Add ANTLR4 visitor methods for materialized view statement contexts |

### Test Files

| File | Purpose |
|------|---------|
| `engine/.../schema/MaterializedViewTest.java` | Core unit tests |
| `engine/.../schema/MaterializedViewIncrementalTest.java` | Incremental refresh tests |
| `engine/.../database/TransactionCallbackTest.java` | Transaction callback mechanism tests |
| `engine/.../schema/MaterializedViewSQLTest.java` | SQL syntax tests |
| `server/.../server/MaterializedViewServerIT.java` | REST API integration tests |

---

## 8. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Post-commit view refresh fails (crash between source commit and view update) | On database reopen, detect STALE/BUILDING views and auto-refresh (crash recovery in `readConfiguration()`) |
| Full refresh on large datasets blocks other operations | Run refresh in a separate transaction; consider batching (similar to `copyType()` pattern) |
| Bulk operations trigger expensive complex-query refreshes | Post-commit batching ensures one refresh per transaction, not per record |
| Periodic refresh thread leak on database close | Use daemon thread; shut down executor in `LocalSchema.close()` |
| Schema migration — old versions can't read `materializedViews` section | Old ArcadeDB versions ignore unknown JSON keys in schema.json |
| Circular view dependencies | Prevent creating a view whose defining query references another view (validation in builder) |
| HA/replication — view refresh on replicas | Only the leader refreshes views; replicas receive the view data via normal replication of the backing type's records |
| `_sourceRID` index overhead on simple-query views | Minor overhead; the index is small (one entry per view row) and only created for simple-query views |

---

## 9. Future Enhancements (Out of Scope)

- **Query rewriting / transparent view substitution:** The query planner could detect that a query matches a materialized view and read from the view instead.
- **Partial/incremental refresh for aggregation queries:** Track affected groups and recompute only those (upgrade complex queries from Tier 2 to Tier 1).
- **View dependencies / cascading refresh:** Materialized view B defined on materialized view A.
- **Cypher/Gremlin view definitions:** Support defining views in query languages other than SQL.
- **Studio UI:** Admin interface for creating, monitoring, and refreshing materialized views.
