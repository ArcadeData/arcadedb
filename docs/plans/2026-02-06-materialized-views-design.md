# Materialized Views Design Document

**Goal:** Add materialized views to ArcadeDB — a schema-level abstraction that stores the results of a defining query as a regular document type, with support for full refresh, incremental (event-driven) refresh, and transparent querying via SQL.

**Architecture:** A materialized view is a special `DocumentType` that wraps a defining SQL SELECT query. Its data lives in standard buckets and can be indexed like any other type. Refresh is driven by the existing `RecordEventsRegistry` listener infrastructure (for incremental) or by re-executing the defining query (for full). Schema persistence uses the existing `schema.json` mechanism with a new `"materializedViews"` top-level section.

**Tech Stack:** Java 21, ArcadeDB engine (schema, events, query, indexing), SQL parser (JavaCC)

---

## 1. Concepts

| Term | Definition |
|------|-----------|
| **Materialized View** | A named, persisted result set derived from a SQL SELECT query. Stored as documents in buckets. |
| **Defining Query** | The SQL SELECT statement whose result set populates the view. |
| **Source Types** | The types referenced in the defining query's FROM clause. Changes to records of these types can trigger refresh. |
| **Full Refresh** | Truncate the view's buckets and re-execute the defining query, inserting all results. |
| **Incremental Refresh** | Use `AfterRecordCreate/Update/Delete` listeners on source types to propagate individual changes. |
| **Staleness** | A view is stale when source data has changed since the last refresh. |

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
  ON_COMMIT,      // Incremental: listeners fire after source record mutations
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
3. Creates a backing `DocumentType` (with buckets) whose properties are inferred from the query's projection
4. Persists the view definition in schema.json
5. Registers event listeners if refresh mode is `ON_COMMIT`
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
  [REFRESH <MANUAL | ON COMMIT | EVERY <interval> <SECOND|MINUTE|HOUR>>]
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
  REFRESH ON COMMIT;

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
ALTER MATERIALIZED VIEW <name> REFRESH <MANUAL | ON COMMIT | EVERY <interval> <SECOND|MINUTE|HOUR>>;
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
- `refreshMode` — MANUAL, ON_COMMIT, or PERIODIC
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
3. Creates a backing `DocumentType` via `schema.buildDocumentType().withName(name)...create()`
4. Creates properties on the backing type inferred from the query projection (or leaves schemaless)
5. Stores the `MaterializedViewImpl` in `LocalSchema.materializedViews` map
6. Saves schema configuration
7. Performs initial full refresh

#### Task 1.3: LocalSchema Extensions

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/Schema.java`
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`

Add to `LocalSchema`:
- `Map<String, MaterializedViewImpl> materializedViews` field
- `createMaterializedView()`, `getMaterializedView()`, `dropMaterializedView()`, etc.
- Extend `toJSON()` to serialize the `materializedViews` map
- Extend `readConfiguration()` to load materialized views from JSON
- Extend `dropType()` to prevent dropping a type that is a backing type for a view
- Extend `close()` to shut down periodic refresh schedulers

#### Task 1.4: Full Refresh Logic

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java`

The refresher:
1. Sets view status to `BUILDING`
2. Begins a transaction
3. Truncates the backing type's buckets (delete all records)
4. Executes the defining query via `database.query("sql", definingQuery)`
5. Iterates the result set and inserts each result as a document in the backing type
6. Commits
7. Updates `lastRefreshTime` and sets status to `VALID`
8. On error: rolls back, sets status to `ERROR`, logs the exception

Truncation of buckets uses the existing `database.command("sql", "TRUNCATE TYPE " + backingTypeName)` or iterates and deletes, depending on the presence of indexes.

---

### Phase 2: SQL Parser Integration

#### Task 2.1: CREATE MATERIALIZED VIEW Statement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/CreateMaterializedViewStatement.java`
- Modify: `engine/src/main/java/com/arcadedb/query/sql/parser/SqlParser.jj` (grammar file)

The statement class extends `DDLStatement` and implements `executeDDL()` following the same pattern as `CreateTriggerStatement`:
1. Validate inputs
2. Delegate to `database.getSchema().buildMaterializedView()...create()`
3. Return result set with `{ "operation": "create materialized view", "name": viewName }`

Parser grammar addition:

```
CreateMaterializedViewStatement ::= CREATE MATERIALIZED VIEW [IF NOT EXISTS] <Identifier>
    AS <SelectStatement>
    [ REFRESH ( MANUAL | ON COMMIT | EVERY <PInteger> ( SECOND | MINUTE | HOUR ) ) ]
    [ BUCKETS <PInteger> ]
```

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

### Phase 3: Incremental Refresh (ON COMMIT)

#### Task 3.1: MaterializedViewChangeListener

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewChangeListener.java`

Implements `AfterRecordCreateListener`, `AfterRecordUpdateListener`, `AfterRecordDeleteListener`. Registered on each source type's `RecordEventsRegistry`.

Strategy depends on query complexity:

**Simple queries** (no aggregation, no JOIN, no GROUP BY):
- `AfterRecordCreate`: If the new record passes the WHERE filter, insert a corresponding document in the view.
- `AfterRecordUpdate`: If the old record was in the view, delete it. If the new record passes the filter, insert it.
- `AfterRecordDelete`: If the deleted record was in the view, delete it from the view.

**Complex queries** (aggregation, GROUP BY, JOIN):
- Mark the view as `STALE` on any source change.
- Optionally trigger a deferred full refresh (after transaction commit).

The listener is designed to fail gracefully — if an error occurs during incremental propagation, the view is marked `STALE` rather than corrupting data.

#### Task 3.2: Listener Registration/Unregistration

Handled in `MaterializedViewBuilder.create()` and `LocalSchema.dropMaterializedView()`:
- On create with `ON_COMMIT`: instantiate `MaterializedViewChangeListener` and register on each source type's events.
- On drop: unregister the listener from all source types.
- On schema reload: re-register listeners for all `ON_COMMIT` views (in `readConfiguration()`).

Follows the same adapter pattern as `TriggerListenerAdapter` in `LocalSchema.registerTriggerListener()`.

---

### Phase 4: Periodic Refresh

#### Task 4.1: Periodic Refresh Scheduler

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

### Phase 5: HTTP/REST API

#### Task 5.1: REST Endpoints

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

#### Task 5.2: Schema Introspection Endpoint

The existing `GET /api/v1/schema/{database}` returns the schema JSON. The `materializedViews` section will be included automatically since `LocalSchema.toJSON()` is extended in Phase 1.

---

### Phase 6: Testing

#### Task 6.1: Unit Tests

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
| `viewPropertyInference` | Verify backing type properties are inferred from projection |
| `emptySourceType` | View on an empty source type produces zero records |

#### Task 6.2: Incremental Refresh Tests

**File:** `engine/src/test/java/com/arcadedb/schema/MaterializedViewIncrementalTest.java`

| Test | Description |
|------|-------------|
| `insertPropagates` | Insert into source type → view updated (simple query) |
| `updatePropagates` | Update source record → view updated |
| `deletePropagates` | Delete source record → removed from view |
| `filterRespected` | Insert non-matching record → not added to view |
| `complexQueryMarksStale` | Aggregation view marked STALE on source change |
| `refreshAfterStale` | Stale view correctly refreshed |

#### Task 6.3: SQL Syntax Tests

**File:** `engine/src/test/java/com/arcadedb/schema/MaterializedViewSQLTest.java`

| Test | Description |
|------|-------------|
| `createViaSql` | `CREATE MATERIALIZED VIEW ... AS SELECT ...` |
| `createWithRefreshMode` | `... REFRESH ON COMMIT` |
| `createWithBuckets` | `... BUCKETS 4` |
| `createWithPeriodic` | `... REFRESH EVERY 5 MINUTE` |
| `refreshViaSql` | `REFRESH MATERIALIZED VIEW ...` |
| `dropViaSql` | `DROP MATERIALIZED VIEW ...` |
| `alterRefreshMode` | `ALTER MATERIALIZED VIEW ... REFRESH MANUAL` |

#### Task 6.4: Server Integration Tests

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

**Trade-off:** Storage overhead. Each materialized view consumes disk space proportional to its result set. This is the fundamental trade-off of materialized views (compute ↔ storage).

### 6.2 Incremental Refresh Scope

**Decision:** Incremental (ON_COMMIT) refresh is only supported for simple queries (no aggregation, no GROUP BY, no subqueries). Complex queries fall back to marking the view as STALE.

**Rationale:**
- Correctly maintaining aggregated views incrementally requires tracking which groups are affected and recomputing them, which is complex and error-prone.
- Simple filter+projection views can be maintained incrementally by applying the WHERE clause to individual record changes.
- Marking as STALE is a safe fallback — users can trigger a full refresh explicitly or rely on PERIODIC mode.

### 6.3 View Metadata in schema.json

**Decision:** Store view definitions in a new `"materializedViews"` section in schema.json, separate from `"types"`.

**Rationale:**
- The backing type is a regular type in the `"types"` section (with buckets, properties, indexes).
- The view metadata (query, source types, refresh mode, status) is orthogonal to the type definition.
- This mirrors how `"triggers"` are stored separately from `"types"`.
- On schema load, the view metadata is linked to its backing type by name.

### 6.4 Thread Safety

**Decision:** Use the existing `RecordEventsRegistry` (which uses `CopyOnWriteArrayList`) for listener registration. Refresh operations acquire an implicit transaction.

**Rationale:**
- The event system is already thread-safe.
- Full refresh runs in a transaction, providing ACID guarantees.
- Periodic refresh uses a single-thread `ScheduledExecutorService` to avoid concurrent refreshes.

### 6.5 View Identity

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
| `schema/MaterializedViewRefreshMode.java` | Enum (MANUAL, ON_COMMIT, PERIODIC) |
| `schema/MaterializedViewBuilder.java` | Builder |
| `schema/MaterializedViewRefresher.java` | Full refresh logic |
| `schema/MaterializedViewChangeListener.java` | Incremental refresh listener |
| `schema/MaterializedViewScheduler.java` | Periodic refresh scheduler |
| `query/sql/parser/CreateMaterializedViewStatement.java` | SQL CREATE |
| `query/sql/parser/DropMaterializedViewStatement.java` | SQL DROP |
| `query/sql/parser/RefreshMaterializedViewStatement.java` | SQL REFRESH |
| `query/sql/parser/AlterMaterializedViewStatement.java` | SQL ALTER |

### Modified Files (engine module)

| File | Change |
|------|--------|
| `schema/Schema.java` | Add materialized view methods |
| `schema/LocalSchema.java` | Add `materializedViews` map, extend `toJSON()`, `readConfiguration()`, `close()`, `dropType()` |
| `query/sql/parser/SqlParser.jj` | Add grammar rules for MATERIALIZED VIEW statements |

### Test Files

| File | Purpose |
|------|---------|
| `engine/.../schema/MaterializedViewTest.java` | Core unit tests |
| `engine/.../schema/MaterializedViewIncrementalTest.java` | Incremental refresh tests |
| `engine/.../schema/MaterializedViewSQLTest.java` | SQL syntax tests |
| `server/.../server/MaterializedViewServerIT.java` | REST API integration tests |

---

## 8. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Incremental refresh produces incorrect data for complex queries | Only support incremental for simple queries; mark others as STALE |
| Full refresh on large datasets blocks transactions | Run refresh in a separate transaction; consider batching (similar to `copyType()` pattern) |
| Periodic refresh thread leak on database close | Use daemon thread; shut down executor in `LocalSchema.close()` |
| Schema migration — old versions can't read `materializedViews` section | Old ArcadeDB versions ignore unknown JSON keys in schema.json |
| Circular view dependencies | Prevent creating a view whose defining query references another view (validation in builder) |
| HA/replication — view refresh on replicas | Only the leader refreshes views; replicas receive the view data via normal replication of the backing type's records |

---

## 9. Future Enhancements (Out of Scope)

- **Query rewriting / transparent view substitution:** The query planner could detect that a query matches a materialized view and read from the view instead.
- **Partial/incremental refresh for aggregation queries:** Track affected groups and recompute only those.
- **View dependencies / cascading refresh:** Materialized view B defined on materialized view A.
- **Cypher/Gremlin view definitions:** Support defining views in query languages other than SQL.
- **Studio UI:** Admin interface for creating, monitoring, and refreshing materialized views.
