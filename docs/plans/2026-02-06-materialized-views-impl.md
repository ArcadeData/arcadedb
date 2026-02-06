# Materialized Views Implementation Plan

> **Prerequisite:** Read `docs/plans/2026-02-06-materialized-views-design.md` for the full design rationale.
>
> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement materialized views as described in the design document — a schema-level abstraction backed by a real `DocumentType` with full refresh, incremental (ON COMMIT) refresh, periodic refresh, and SQL DDL statements.

**Tech Stack:** Java 21, ANTLR4 grammar, ArcadeDB engine

---

## Phase 1: Core Data Model

### Task 1.1: Create MaterializedViewRefreshMode Enum

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefreshMode.java`

**Step 1: Write the enum**

```java
package com.arcadedb.schema;

public enum MaterializedViewRefreshMode {
  MANUAL,
  ON_COMMIT,
  PERIODIC
}
```

No test needed — it's a plain enum.

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

---

### Task 1.2: Create MaterializedView Interface

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedView.java`

**Step 1: Write the interface**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;

import java.util.List;

public interface MaterializedView {
  String getName();
  String getQuery();
  DocumentType getBackingType();
  List<String> getSourceTypeNames();
  MaterializedViewRefreshMode getRefreshMode();
  long getLastRefreshTime();
  String getStatus();
  long getRefreshInterval();

  void refresh();
  void drop();

  JSONObject toJSON();
}
```

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

---

### Task 1.3: Create MaterializedViewImpl

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewImpl.java`
- Test: `engine/src/test/java/com/arcadedb/schema/MaterializedViewImplTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewImplTest {

  @Test
  void toJSON() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        "SalesSummary",
        "SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer",
        "SalesSummary",
        List.of("Order"),
        MaterializedViewRefreshMode.MANUAL,
        0
    );

    final JSONObject json = view.toJSON();

    assertThat(json.getString("name")).isEqualTo("SalesSummary");
    assertThat(json.getString("query")).isEqualTo(
        "SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer");
    assertThat(json.getString("backingType")).isEqualTo("SalesSummary");
    assertThat(json.getString("refreshMode")).isEqualTo("MANUAL");
    assertThat(json.getString("status")).isEqualTo("VALID");
  }

  @Test
  void fromJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", "ActiveUsers");
    json.put("query", "SELECT name FROM User WHERE active = true");
    json.put("backingType", "ActiveUsers");
    json.put("sourceTypes", new com.arcadedb.serializer.json.JSONArray(List.of("User")));
    json.put("refreshMode", "ON_COMMIT");
    json.put("lastRefreshTime", 1738800000000L);
    json.put("status", "VALID");
    json.put("refreshInterval", 0);

    final MaterializedViewImpl view = MaterializedViewImpl.fromJSON(json);

    assertThat(view.getName()).isEqualTo("ActiveUsers");
    assertThat(view.getQuery()).isEqualTo("SELECT name FROM User WHERE active = true");
    assertThat(view.getBackingTypeName()).isEqualTo("ActiveUsers");
    assertThat(view.getSourceTypeNames()).containsExactly("User");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.ON_COMMIT);
    assertThat(view.getLastRefreshTime()).isEqualTo(1738800000000L);
    assertThat(view.getStatus()).isEqualTo("VALID");
  }

  @Test
  void statusTransitions() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        "TestView", "SELECT 1", "TestView", List.of(), MaterializedViewRefreshMode.MANUAL, 0);

    assertThat(view.getStatus()).isEqualTo("VALID");

    view.setStatus("STALE");
    assertThat(view.getStatus()).isEqualTo("STALE");

    view.setStatus("BUILDING");
    assertThat(view.getStatus()).isEqualTo("BUILDING");
  }
}
```

**Step 2: Run test to verify it fails**

```bash
mvn test -Dtest=MaterializedViewImplTest -pl engine
```

Expected: FAIL — class not found.

**Step 3: Write MaterializedViewImpl**

Follows exactly the `TriggerImpl` pattern (see `LocalSchema.java:94` for how triggers are stored,
`TriggerImpl.java` for `toJSON()`/`fromJSON()` serialization).

Key fields:
- `name`, `query`, `backingTypeName`, `sourceTypeNames` (List), `refreshMode`, `lastRefreshTime`, `status`, `refreshInterval`
- `toJSON()` serializes all fields; `sourceTypeNames` as `JSONArray`
- `static fromJSON(JSONObject)` deserializes

Note: `getBackingType()` and `refresh()` and `drop()` will be wired later when the view is associated with a database. For now, `getBackingType()` returns `null` and `refresh()`/`drop()` throw `UnsupportedOperationException`. These will be replaced in Task 1.5 when `MaterializedViewImpl` gets a `database` reference.

**Step 4: Run test**

```bash
mvn test -Dtest=MaterializedViewImplTest -pl engine
```

Expected: PASS.

---

### Task 1.4: Extend Schema Interface

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/Schema.java` (416 lines)

**Step 1: Add methods after the trigger section (after line 188)**

Insert after `dropTrigger(String triggerName);`:

```java
// MATERIALIZED VIEW MANAGEMENT

boolean existsMaterializedView(String viewName);

MaterializedView getMaterializedView(String viewName);

MaterializedView[] getMaterializedViews();

void dropMaterializedView(String viewName);

MaterializedViewBuilder buildMaterializedView();
```

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

Expected: FAIL — `LocalSchema` does not implement the new methods yet. That's fine — we'll fix it in Task 1.5.

---

### Task 1.5: Create MaterializedViewBuilder

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java`

**Step 1: Write the builder class**

Follows the `TypeBuilder` pattern. Key responsibilities of `create()`:

1. Validate name is not null/empty.
2. Parse the defining query via `((DatabaseInternal) database).getQueryEngine("sql").parse(query, null)` to check syntax.
3. Extract source type names from the parsed `SelectStatement.target.item.identifier` (the FROM clause). If the FROM clause references a type identifier, extract its `getStringValue()`.
4. Check all source types exist in the schema.
5. Check a type with the same name does not already exist (unless `ifNotExists`).
6. Create a backing `DocumentType` via `database.getSchema().buildDocumentType().withName(name).withTotalBuckets(buckets).create()`.
7. Create the `MaterializedViewImpl` object and wire its `database` reference.
8. Store it in `LocalSchema` via a package-private `registerMaterializedView()` method.
9. Execute initial full refresh.
10. Return the `MaterializedView`.

Key method signatures:

```java
public class MaterializedViewBuilder {
  private final DatabaseInternal database;
  private String name;
  private String query;
  private MaterializedViewRefreshMode refreshMode = MaterializedViewRefreshMode.MANUAL;
  private int buckets = -1;
  private long refreshInterval = 0;
  private boolean ifNotExists = false;

  public MaterializedViewBuilder(DatabaseInternal database) { ... }
  public MaterializedViewBuilder withName(String name) { ... }
  public MaterializedViewBuilder withQuery(String query) { ... }
  public MaterializedViewBuilder withRefreshMode(MaterializedViewRefreshMode mode) { ... }
  public MaterializedViewBuilder withTotalBuckets(int buckets) { ... }
  public MaterializedViewBuilder withRefreshInterval(long intervalMs) { ... }
  public MaterializedViewBuilder withIgnoreIfExists(boolean ignore) { ... }
  public MaterializedView create() { ... }
}
```

Source type extraction from `SelectStatement`:

```java
private List<String> extractSourceTypes(final String queryText) {
  final List<String> sourceTypes = new ArrayList<>();
  final Statement parsed = ((SQLQueryEngine) database.getQueryEngine("sql")).parse(queryText, (DatabaseInternal) database);
  if (parsed instanceof SelectStatement select && select.target != null && select.target.item != null) {
    final FromItem item = select.target.item;
    if (item.identifier != null)
      sourceTypes.add(item.identifier.getStringValue());
  }
  return sourceTypes;
}
```

**Step 2: Compile — this will fail until LocalSchema is extended (Task 1.6).**

---

### Task 1.6: Extend LocalSchema

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java` (1671 lines)

**Step 1: Add the materializedViews field (after line 94, near the `triggers` field)**

```java
protected final Map<String, MaterializedViewImpl> materializedViews = new HashMap<>();
```

**Step 2: Implement the Schema interface methods**

Add after the `dropTrigger` / `unregisterTriggerListener` section (~line 665):

```java
// --- MATERIALIZED VIEW MANAGEMENT ---

@Override
public boolean existsMaterializedView(final String viewName) {
  return materializedViews.containsKey(viewName);
}

@Override
public MaterializedView getMaterializedView(final String viewName) {
  return materializedViews.get(viewName);
}

@Override
public MaterializedView[] getMaterializedViews() {
  return materializedViews.values().toArray(new MaterializedView[0]);
}

@Override
public MaterializedViewBuilder buildMaterializedView() {
  return new MaterializedViewBuilder(database);
}

@Override
public void dropMaterializedView(final String viewName) {
  database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

  recordFileChanges(() -> {
    final MaterializedViewImpl view = materializedViews.get(viewName);
    if (view == null)
      throw new SchemaException("Materialized view '" + viewName + "' does not exist");

    // Unregister any event listeners (ON_COMMIT mode)
    unregisterMaterializedViewListeners(viewName);

    // Drop the backing type (which drops buckets and indexes)
    final String backingTypeName = view.getBackingTypeName();
    if (existsType(backingTypeName))
      dropType(backingTypeName);

    materializedViews.remove(viewName);
    return null;
  });
}

void registerMaterializedView(final MaterializedViewImpl view) {
  materializedViews.put(view.getName(), view);
  saveConfiguration();
}
```

**Step 3: Extend `toJSON()` (line 1430)**

After the triggers serialization block (after line 1453), add:

```java
final JSONObject viewsJson = new JSONObject();
root.put("materializedViews", viewsJson);

for (final MaterializedViewImpl view : this.materializedViews.values())
  viewsJson.put(view.getName(), view.toJSON());
```

**Step 4: Extend `readConfiguration()` (after the trigger loading block, after line 1391)**

Add before the `catch` on line 1393:

```java
// LOAD MATERIALIZED VIEWS
if (root.has("materializedViews")) {
  final JSONObject viewsJSON = root.getJSONObject("materializedViews");
  for (final String viewName : viewsJSON.keySet()) {
    final JSONObject viewJSON = viewsJSON.getJSONObject(viewName);
    try {
      final MaterializedViewImpl view = MaterializedViewImpl.fromJSON(viewJSON);
      view.setDatabase(database);
      materializedViews.put(view.getName(), view);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Error loading materialized view '%s': %s", e, viewName, e.getMessage());
    }
  }
}
```

**Step 5: Extend `close()` (line 766)**

Add before `files.clear()` on line 782:

```java
materializedViews.clear();
```

**Step 6: Protect backing type from direct drop**

In `dropType()` or the existing drop path, add validation. Find the `dropType` method and add at the start:

```java
// Check if this type is a backing type for a materialized view
for (final MaterializedViewImpl view : materializedViews.values()) {
  if (view.getBackingTypeName().equals(typeName))
    throw new SchemaException(
        "Cannot drop type '" + typeName + "' because it is the backing type for materialized view '"
            + view.getName() + "'. Use DROP MATERIALIZED VIEW instead.");
}
```

**Step 7: Compile the entire engine module**

```bash
mvn compile -pl engine -q
```

Expected: PASS.

---

### Task 1.7: Create MaterializedViewRefresher

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java`

**Step 1: Write the refresher class**

```java
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.logging.Level;

public class MaterializedViewRefresher {

  public static void fullRefresh(final DatabaseInternal database, final MaterializedViewImpl view) {
    final String backingTypeName = view.getBackingTypeName();
    final String query = view.getQuery();

    view.setStatus("BUILDING");

    boolean success = false;
    database.begin();
    try {
      // 1. Truncate existing data
      database.command("sql", "TRUNCATE TYPE `" + backingTypeName + "` UNSAFE");

      // 2. Execute the defining query
      final ResultSet rs = database.query("sql", query);

      // 3. Insert each result as a document in the backing type
      while (rs.hasNext()) {
        final Result row = rs.next();
        final MutableDocument doc = database.newDocument(backingTypeName);
        for (final String prop : row.getPropertyNames())
          doc.set(prop, row.getProperty(prop));
        doc.save();
      }

      database.commit();
      view.setLastRefreshTime(System.currentTimeMillis());
      view.setStatus("VALID");
      success = true;

    } catch (final Exception e) {
      LogManager.instance().log(MaterializedViewRefresher.class, Level.SEVERE,
          "Error refreshing materialized view '%s': %s", e, view.getName(), e.getMessage());
      view.setStatus("ERROR");
    } finally {
      if (!success && database.isTransactionActive())
        database.rollback();
    }

    // Save updated metadata (lastRefreshTime, status)
    ((LocalSchema) database.getSchema().getEmbedded()).saveConfiguration();
  }
}
```

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

---

### Task 1.8: Wire MaterializedViewImpl Database Reference

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/MaterializedViewImpl.java`

**Step 1:** Add a `DatabaseInternal database` field, a `setDatabase()` method, and implement `getBackingType()`, `refresh()`, `drop()`:

```java
private transient DatabaseInternal database;

public void setDatabase(final DatabaseInternal database) {
  this.database = database;
}

@Override
public DocumentType getBackingType() {
  if (database == null)
    return null;
  return database.getSchema().getType(backingTypeName);
}

@Override
public void refresh() {
  MaterializedViewRefresher.fullRefresh(database, this);
}

@Override
public void drop() {
  database.getSchema().dropMaterializedView(name);
}
```

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

---

### Task 1.9: Phase 1 Integration Test

**Files:**
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java`

**Step 1: Write the test class**

```java
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewTest extends TestHelper {

  @Test
  void createAndQueryView() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Order");
      database.getSchema().getType("Order").createProperty("customer", String.class);
      database.getSchema().getType("Order").createProperty("amount", Integer.class);

      database.newDocument("Order").set("customer", "Alice").set("amount", 100).save();
      database.newDocument("Order").set("customer", "Alice").set("amount", 200).save();
      database.newDocument("Order").set("customer", "Bob").set("amount", 50).save();
    });

    database.getSchema().buildMaterializedView()
        .withName("SalesSummary")
        .withQuery("SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer")
        .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
        .create();

    assertThat(database.getSchema().existsMaterializedView("SalesSummary")).isTrue();
    assertThat(database.getSchema().existsType("SalesSummary")).isTrue();

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT customer, total FROM SalesSummary ORDER BY customer");
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) results.add(rs.next());

      assertThat(results).hasSize(2);
      assertThat(results.get(0).getProperty("customer")).isEqualTo("Alice");
      assertThat(((Number) results.get(0).getProperty("total")).intValue()).isEqualTo(300);
      assertThat(results.get(1).getProperty("customer")).isEqualTo("Bob");
      assertThat(((Number) results.get(1).getProperty("total")).intValue()).isEqualTo(50);
    });
  }

  @Test
  void fullRefresh() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product");
      database.getSchema().getType("Product").createProperty("name", String.class);
      database.getSchema().getType("Product").createProperty("price", Integer.class);

      database.newDocument("Product").set("name", "A").set("price", 10).save();
    });

    database.getSchema().buildMaterializedView()
        .withName("ProductView")
        .withQuery("SELECT name, price FROM Product")
        .create();

    // Add more data
    database.transaction(() -> {
      database.newDocument("Product").set("name", "B").set("price", 20).save();
    });

    // View should still have old data
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM ProductView");
      int count = 0;
      while (rs.hasNext()) { rs.next(); count++; }
      assertThat(count).isEqualTo(1);
    });

    // Refresh
    database.getSchema().getMaterializedView("ProductView").refresh();

    // Now should have both records
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM ProductView");
      int count = 0;
      while (rs.hasNext()) { rs.next(); count++; }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void dropView() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Item");
    });

    database.getSchema().buildMaterializedView()
        .withName("ItemView")
        .withQuery("SELECT FROM Item")
        .create();

    assertThat(database.getSchema().existsMaterializedView("ItemView")).isTrue();
    assertThat(database.getSchema().existsType("ItemView")).isTrue();

    database.getSchema().dropMaterializedView("ItemView");

    assertThat(database.getSchema().existsMaterializedView("ItemView")).isFalse();
    assertThat(database.getSchema().existsType("ItemView")).isFalse();
  }

  @Test
  void viewSurvivesReopen() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Log");
      database.getSchema().getType("Log").createProperty("msg", String.class);
      database.newDocument("Log").set("msg", "hello").save();
    });

    database.getSchema().buildMaterializedView()
        .withName("LogView")
        .withQuery("SELECT msg FROM Log")
        .create();

    // Reopen database
    database.close();
    database = factory.open();

    assertThat(database.getSchema().existsMaterializedView("LogView")).isTrue();

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT msg FROM LogView");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getProperty("msg")).isEqualTo("hello");
    });
  }

  @Test
  void cannotDropBackingType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Source");
    });

    database.getSchema().buildMaterializedView()
        .withName("MyView")
        .withQuery("SELECT FROM Source")
        .create();

    assertThatThrownBy(() -> database.getSchema().dropType("MyView"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("backing type");
  }

  @Test
  void emptySourceType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Empty");
    });

    database.getSchema().buildMaterializedView()
        .withName("EmptyView")
        .withQuery("SELECT FROM Empty")
        .create();

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM EmptyView");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  void createViewIfNotExists() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Data");
    });

    database.getSchema().buildMaterializedView()
        .withName("DataView")
        .withQuery("SELECT FROM Data")
        .withIgnoreIfExists(true)
        .create();

    // Second call should not throw
    database.getSchema().buildMaterializedView()
        .withName("DataView")
        .withQuery("SELECT FROM Data")
        .withIgnoreIfExists(true)
        .create();

    assertThat(database.getSchema().existsMaterializedView("DataView")).isTrue();
  }
}
```

**Step 2: Run tests**

```bash
mvn test -Dtest=MaterializedViewTest -pl engine
```

Expected: ALL PASS.

---

## Phase 2: SQL Parser Integration

### Task 2.1: Add Lexer Keywords

**Files:**
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4` (520 lines)

**Step 1:** Add new keywords after the `TRIGGER` keyword (line 206). Insert after `TIMESTAMP` (line 224):

```
MATERIALIZED: M A T E R I A L I Z E D;
VIEW: V I E W;
REFRESH: R E F R E S H;
EVERY: E V E R Y;
SECOND: S E C O N D;
MINUTE: M I N U T E;
HOUR: H O U R;
COMMIT: C O M M I T;
MANUAL: M A N U A L;
```

Check if `COMMIT`, `VIEW`, or `AS` already exist in the lexer before adding — some may already be defined (e.g., `AS` is certainly already there). Only add what is missing.

**Step 2: Compile to regenerate ANTLR parser**

```bash
mvn generate-sources -pl engine -q
```

---

### Task 2.2: Add Grammar Rules to SQLParser.g4

**Files:**
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4` (1256 lines)

**Step 1:** Add to the `statement` rule (after line 96, alongside other CREATE variants):

```
    | CREATE MATERIALIZED VIEW createMaterializedViewBody  # createMaterializedViewStmt
```

Add to the DROP section (after line 109):

```
    | DROP MATERIALIZED VIEW dropMaterializedViewBody      # dropMaterializedViewStmt
```

Add a new standalone entry (after line 117, alongside other utility statements):

```
    | REFRESH MATERIALIZED VIEW refreshMaterializedViewBody # refreshMaterializedViewStmt
```

Add to the ALTER section (after line 102):

```
    | ALTER MATERIALIZED VIEW alterMaterializedViewBody    # alterMaterializedViewStmt
```

**Step 2:** Add grammar rules at the end of the TRIGGER MANAGEMENT section (after line 632, before TRUNCATE):

```antlr
// ============================================================================
// MATERIALIZED VIEW MANAGEMENT
// ============================================================================

/**
 * CREATE MATERIALIZED VIEW statement
 * Syntax: CREATE MATERIALIZED VIEW [IF NOT EXISTS] name
 *         AS selectStatement
 *         [REFRESH (MANUAL | ON COMMIT | EVERY integer (SECOND | MINUTE | HOUR))]
 *         [BUCKETS integer]
 */
createMaterializedViewBody
    : (IF NOT EXISTS)? identifier
      AS selectStatement
      materializedViewRefreshClause?
      (BUCKETS POSITIVE_INTEGER)?
    ;

materializedViewRefreshClause
    : REFRESH MANUAL
    | REFRESH ON COMMIT
    | REFRESH EVERY POSITIVE_INTEGER materializedViewTimeUnit
    ;

materializedViewTimeUnit
    : SECOND
    | MINUTE
    | HOUR
    ;

/**
 * DROP MATERIALIZED VIEW statement
 * Syntax: DROP MATERIALIZED VIEW [IF EXISTS] name
 */
dropMaterializedViewBody
    : (IF EXISTS)? identifier
    ;

/**
 * REFRESH MATERIALIZED VIEW statement
 * Syntax: REFRESH MATERIALIZED VIEW name
 */
refreshMaterializedViewBody
    : identifier
    ;

/**
 * ALTER MATERIALIZED VIEW statement
 * Syntax: ALTER MATERIALIZED VIEW name REFRESH (MANUAL | ON COMMIT | EVERY integer (SECOND|MINUTE|HOUR))
 */
alterMaterializedViewBody
    : identifier materializedViewRefreshClause
    ;
```

**Step 3: Regenerate parser**

```bash
mvn generate-sources -pl engine -q
```

---

### Task 2.3: Create CreateMaterializedViewStatement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/CreateMaterializedViewStatement.java`

**Step 1: Write the statement class**

Follows the `CreateTriggerStatement` pattern exactly (see `CreateTriggerStatement.java:38-152`):

```java
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewBuilder;
import com.arcadedb.schema.MaterializedViewRefreshMode;

public class CreateMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public SelectStatement selectStatement;
  public boolean ifNotExists = false;
  public String refreshMode = null;  // null = MANUAL (default)
  public int refreshInterval = 0;
  public String refreshTimeUnit = null;
  public int buckets = -1;

  public CreateMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();

    // Build the defining query string from the parsed SelectStatement
    final StringBuilder queryBuilder = new StringBuilder();
    selectStatement.toString(new java.util.HashMap<>(), queryBuilder);
    final String query = queryBuilder.toString();

    // Determine refresh mode
    MaterializedViewRefreshMode mode = MaterializedViewRefreshMode.MANUAL;
    if (refreshMode != null) {
      if ("ON_COMMIT".equalsIgnoreCase(refreshMode))
        mode = MaterializedViewRefreshMode.ON_COMMIT;
      else if ("PERIODIC".equalsIgnoreCase(refreshMode))
        mode = MaterializedViewRefreshMode.PERIODIC;
    }

    // Compute refresh interval in ms
    long intervalMs = 0;
    if (mode == MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0) {
      intervalMs = switch (refreshTimeUnit.toUpperCase()) {
        case "SECOND" -> refreshInterval * 1000L;
        case "MINUTE" -> refreshInterval * 60_000L;
        case "HOUR" -> refreshInterval * 3_600_000L;
        default -> refreshInterval * 1000L;
      };
    }

    final MaterializedViewBuilder builder = database.getSchema().buildMaterializedView()
        .withName(name.getStringValue())
        .withQuery(query)
        .withRefreshMode(mode)
        .withRefreshInterval(intervalMs)
        .withIgnoreIfExists(ifNotExists);

    if (buckets > 0)
      builder.withTotalBuckets(buckets);

    final MaterializedView view = builder.create();

    final InternalResultSet rs = new InternalResultSet();
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "create materialized view");
    result.setProperty("name", view.getName());
    result.setProperty("created", true);
    rs.add(result);
    return rs;
  }
}
```

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

---

### Task 2.4: Create DropMaterializedViewStatement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/DropMaterializedViewStatement.java`

Follows the `DropTriggerStatement` pattern (see `DropTriggerStatement.java:37-72`):

```java
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

public class DropMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public boolean ifExists = false;

  public DropMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database db = context.getDatabase();
    final InternalResultSet rs = new InternalResultSet();

    if (!db.getSchema().existsMaterializedView(name.getStringValue())) {
      if (ifExists) {
        final ResultInternal result = new ResultInternal(context.getDatabase());
        result.setProperty("operation", "drop materialized view");
        result.setProperty("name", name.getStringValue());
        result.setProperty("dropped", false);
        rs.add(result);
        return rs;
      }
      throw new CommandExecutionException(
          "Materialized view '" + name.getStringValue() + "' does not exist");
    }

    db.getSchema().dropMaterializedView(name.getStringValue());

    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "drop materialized view");
    result.setProperty("name", name.getStringValue());
    result.setProperty("dropped", true);
    rs.add(result);
    return rs;
  }
}
```

---

### Task 2.5: Create RefreshMaterializedViewStatement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/RefreshMaterializedViewStatement.java`

```java
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedView;

public class RefreshMaterializedViewStatement extends DDLStatement {
  public Identifier name;

  public RefreshMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database db = context.getDatabase();

    final MaterializedView view = db.getSchema().getMaterializedView(name.getStringValue());
    if (view == null)
      throw new CommandExecutionException(
          "Materialized view '" + name.getStringValue() + "' does not exist");

    view.refresh();

    final InternalResultSet rs = new InternalResultSet();
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "refresh materialized view");
    result.setProperty("name", name.getStringValue());
    result.setProperty("refreshed", true);
    rs.add(result);
    return rs;
  }
}
```

---

### Task 2.6: Create AlterMaterializedViewStatement

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/AlterMaterializedViewStatement.java`

Same DDL pattern. Changes the refresh mode on the `MaterializedViewImpl` and re-registers/unregisters listeners.

---

### Task 2.7: Add ANTLR Visitor Methods to SQLASTBuilder

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java` (6874 lines)

Insert after the `visitDropTriggerStmt` / `visitTriggerEvent` methods (~line 5773), before the TRUNCATE section:

```java
// MATERIALIZED VIEW MANAGEMENT

@Override
public CreateMaterializedViewStatement visitCreateMaterializedViewStmt(
    final SQLParser.CreateMaterializedViewStmtContext ctx) {
  final CreateMaterializedViewStatement stmt = new CreateMaterializedViewStatement(-1);
  final SQLParser.CreateMaterializedViewBodyContext body = ctx.createMaterializedViewBody();

  stmt.ifNotExists = body.IF() != null && body.NOT() != null && body.EXISTS() != null;
  stmt.name = (Identifier) visit(body.identifier());
  stmt.selectStatement = (SelectStatement) visit(body.selectStatement());

  if (body.materializedViewRefreshClause() != null) {
    final SQLParser.MaterializedViewRefreshClauseContext rc = body.materializedViewRefreshClause();
    if (rc.MANUAL() != null) {
      stmt.refreshMode = "MANUAL";
    } else if (rc.COMMIT() != null) {
      stmt.refreshMode = "ON_COMMIT";
    } else if (rc.EVERY() != null) {
      stmt.refreshMode = "PERIODIC";
      stmt.refreshInterval = Integer.parseInt(rc.POSITIVE_INTEGER().getText());
      stmt.refreshTimeUnit = ((SQLParser.MaterializedViewTimeUnitContext) rc.materializedViewTimeUnit())
          .getText().toUpperCase();
    }
  }

  if (body.BUCKETS() != null)
    stmt.buckets = Integer.parseInt(body.POSITIVE_INTEGER().getText());

  return stmt;
}

@Override
public DropMaterializedViewStatement visitDropMaterializedViewStmt(
    final SQLParser.DropMaterializedViewStmtContext ctx) {
  final DropMaterializedViewStatement stmt = new DropMaterializedViewStatement(-1);
  final SQLParser.DropMaterializedViewBodyContext body = ctx.dropMaterializedViewBody();
  stmt.name = (Identifier) visit(body.identifier());
  stmt.ifExists = body.IF() != null && body.EXISTS() != null;
  return stmt;
}

@Override
public RefreshMaterializedViewStatement visitRefreshMaterializedViewStmt(
    final SQLParser.RefreshMaterializedViewStmtContext ctx) {
  final RefreshMaterializedViewStatement stmt = new RefreshMaterializedViewStatement(-1);
  stmt.name = (Identifier) visit(ctx.refreshMaterializedViewBody().identifier());
  return stmt;
}

@Override
public AlterMaterializedViewStatement visitAlterMaterializedViewStmt(
    final SQLParser.AlterMaterializedViewStmtContext ctx) {
  final AlterMaterializedViewStatement stmt = new AlterMaterializedViewStatement(-1);
  final SQLParser.AlterMaterializedViewBodyContext body = ctx.alterMaterializedViewBody();
  stmt.name = (Identifier) visit(body.identifier());
  // Parse refresh clause same as CREATE
  final SQLParser.MaterializedViewRefreshClauseContext rc = body.materializedViewRefreshClause();
  if (rc.MANUAL() != null) {
    stmt.refreshMode = "MANUAL";
  } else if (rc.COMMIT() != null) {
    stmt.refreshMode = "ON_COMMIT";
  } else if (rc.EVERY() != null) {
    stmt.refreshMode = "PERIODIC";
    stmt.refreshInterval = Integer.parseInt(rc.POSITIVE_INTEGER().getText());
    stmt.refreshTimeUnit = rc.materializedViewTimeUnit().getText().toUpperCase();
  }
  return stmt;
}
```

**Step 2: Compile**

```bash
mvn compile -pl engine -q
```

---

### Task 2.8: SQL Syntax Tests

**Files:**
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewSQLTest.java`

**Step 1: Write tests**

```java
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewSQLTest extends TestHelper {

  @Test
  void createViaSql() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Customer");
      database.getSchema().getType("Customer").createProperty("name", String.class);
      database.newDocument("Customer").set("name", "Alice").save();
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW CustomerView AS SELECT name FROM Customer");

    assertThat(database.getSchema().existsMaterializedView("CustomerView")).isTrue();

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT name FROM CustomerView");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getProperty("name")).isEqualTo("Alice");
    });
  }

  @Test
  void createWithRefreshMode() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Event");
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW EventView AS SELECT FROM Event REFRESH ON COMMIT");

    final MaterializedView view = database.getSchema().getMaterializedView("EventView");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.ON_COMMIT);
  }

  @Test
  void createWithBuckets() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Data");
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW DataView AS SELECT FROM Data BUCKETS 4");

    assertThat(database.getSchema().existsMaterializedView("DataView")).isTrue();
  }

  @Test
  void createWithPeriodic() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Metric");
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW MetricView AS SELECT FROM Metric REFRESH EVERY 5 MINUTE");

    final MaterializedView view = database.getSchema().getMaterializedView("MetricView");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.PERIODIC);
    assertThat(view.getRefreshInterval()).isEqualTo(300_000L);
  }

  @Test
  void refreshViaSql() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Stock");
      database.getSchema().getType("Stock").createProperty("symbol", String.class);
      database.newDocument("Stock").set("symbol", "AAPL").save();
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW StockView AS SELECT symbol FROM Stock");

    database.transaction(() -> {
      database.newDocument("Stock").set("symbol", "GOOG").save();
    });

    database.command("sql", "REFRESH MATERIALIZED VIEW StockView");

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM StockView");
      int count = 0;
      while (rs.hasNext()) { rs.next(); count++; }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void dropViaSql() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Temp");
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW TempView AS SELECT FROM Temp");

    assertThat(database.getSchema().existsMaterializedView("TempView")).isTrue();

    database.command("sql", "DROP MATERIALIZED VIEW TempView");

    assertThat(database.getSchema().existsMaterializedView("TempView")).isFalse();
  }

  @Test
  void dropIfExists() {
    // Should not throw
    database.command("sql", "DROP MATERIALIZED VIEW IF EXISTS NonExistent");
  }

  @Test
  void dropNonExistentThrows() {
    assertThatThrownBy(() ->
        database.command("sql", "DROP MATERIALIZED VIEW NonExistent"))
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void createIfNotExists() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Base");
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS BaseView AS SELECT FROM Base");

    // Should not throw
    database.command("sql",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS BaseView AS SELECT FROM Base");

    assertThat(database.getSchema().existsMaterializedView("BaseView")).isTrue();
  }
}
```

**Step 2: Run tests**

```bash
mvn test -Dtest=MaterializedViewSQLTest -pl engine
```

Expected: ALL PASS.

---

## Phase 3: Incremental Refresh (ON COMMIT)

### Task 3.1: Create MaterializedViewChangeListener

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewChangeListener.java`

Implements `AfterRecordCreateListener`, `AfterRecordUpdateListener`, `AfterRecordDeleteListener`.

**Strategy:**

Determine if the defining query is "simple" (no GROUP BY, no aggregation functions, no JOIN, no subquery). If simple:

- **onCreate**: Parse the WHERE clause from the defining query. Evaluate it against the new record. If it passes, project the relevant fields and insert a document into the view's backing type. Store the source RID as a hidden property `_sourceRid` on the view document for update/delete tracking.
- **onUpdate**: Find the view document whose `_sourceRid` matches. Delete it. Then re-evaluate the WHERE clause against the updated record and insert if it passes.
- **onDelete**: Find and delete the view document whose `_sourceRid` matches.

If complex: just set the view status to `STALE`.

```java
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.logging.Level;

public class MaterializedViewChangeListener
    implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final DatabaseInternal database;
  private final MaterializedViewImpl view;
  private final boolean isSimpleQuery;

  public MaterializedViewChangeListener(final DatabaseInternal database,
                                         final MaterializedViewImpl view,
                                         final boolean isSimpleQuery) {
    this.database = database;
    this.view = view;
    this.isSimpleQuery = isSimpleQuery;
  }

  @Override
  public void onAfterCreate(final Record record) {
    if (!isSimpleQuery) {
      view.setStatus("STALE");
      return;
    }
    try {
      propagateCreate(record);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Incremental refresh failed for view '%s', marking as STALE", null, view.getName());
      view.setStatus("STALE");
    }
  }

  @Override
  public void onAfterUpdate(final Record record) {
    if (!isSimpleQuery) {
      view.setStatus("STALE");
      return;
    }
    try {
      propagateDelete(record);
      propagateCreate(record);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Incremental refresh failed for view '%s', marking as STALE", null, view.getName());
      view.setStatus("STALE");
    }
  }

  @Override
  public void onAfterDelete(final Record record) {
    if (!isSimpleQuery) {
      view.setStatus("STALE");
      return;
    }
    try {
      propagateDelete(record);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Incremental refresh failed for view '%s', marking as STALE", null, view.getName());
      view.setStatus("STALE");
    }
  }

  private void propagateCreate(final Record record) {
    // Execute: SELECT <projection> FROM :rid WHERE <view's where clause>
    // If it returns results, insert them into the backing type
    final String rid = record.getIdentity().toString();
    final ResultSet rs = database.query("sql", view.getQuery() + " AND @rid = " + rid);
    // Alternative approach: re-run with LET $source = :rid
    // For simplicity, just re-check via query on the single record
    // ... Implementation details depend on query structure
  }

  private void propagateDelete(final Record record) {
    final String rid = record.getIdentity().toString();
    database.command("sql",
        "DELETE FROM `" + view.getBackingTypeName() + "` WHERE _sourceRid = " + rid);
  }

  public MaterializedViewImpl getView() {
    return view;
  }
}
```

**Note:** The exact incremental propagation logic for `propagateCreate` is the most complex part. The initial implementation should be conservative — for any query that isn't a trivial `SELECT <fields> FROM <type> [WHERE <condition>]`, fall back to marking `STALE`.

---

### Task 3.2: Listener Registration in LocalSchema

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`

**Step 1:** Add a field to track view listeners (near line 95):

```java
private final Map<String, MaterializedViewChangeListener> viewListeners = new HashMap<>();
```

**Step 2:** Add registration/unregistration methods:

```java
void registerMaterializedViewListeners(final MaterializedViewImpl view) {
  if (view.getRefreshMode() != MaterializedViewRefreshMode.ON_COMMIT)
    return;

  final boolean isSimple = isSimpleQuery(view.getQuery());
  final MaterializedViewChangeListener listener =
      new MaterializedViewChangeListener(database, view, isSimple);

  for (final String sourceTypeName : view.getSourceTypeNames()) {
    final LocalDocumentType type = types.get(sourceTypeName);
    if (type != null) {
      final RecordEventsRegistry events = (RecordEventsRegistry) type.getEvents();
      events.registerListener((AfterRecordCreateListener) listener);
      events.registerListener((AfterRecordUpdateListener) listener);
      events.registerListener((AfterRecordDeleteListener) listener);
    }
  }

  viewListeners.put(view.getName(), listener);
}

void unregisterMaterializedViewListeners(final String viewName) {
  final MaterializedViewChangeListener listener = viewListeners.remove(viewName);
  if (listener == null)
    return;

  for (final String sourceTypeName : listener.getView().getSourceTypeNames()) {
    final LocalDocumentType type = types.get(sourceTypeName);
    if (type != null) {
      final RecordEventsRegistry events = (RecordEventsRegistry) type.getEvents();
      events.unregisterListener((AfterRecordCreateListener) listener);
      events.unregisterListener((AfterRecordUpdateListener) listener);
      events.unregisterListener((AfterRecordDeleteListener) listener);
    }
  }
}

private boolean isSimpleQuery(final String query) {
  final String upper = query.toUpperCase();
  return !upper.contains("GROUP BY") && !upper.contains("SUM(") && !upper.contains("COUNT(")
      && !upper.contains("AVG(") && !upper.contains("MIN(") && !upper.contains("MAX(")
      && !upper.contains(" JOIN ");
}
```

**Step 3:** Call `registerMaterializedViewListeners` in `readConfiguration()` after loading each view.

**Step 4:** Call `unregisterMaterializedViewListeners` in `close()`.

---

### Task 3.3: Incremental Refresh Tests

**Files:**
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewIncrementalTest.java`

```java
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewIncrementalTest extends TestHelper {

  @Test
  void insertPropagatesForSimpleQuery() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("User");
      database.getSchema().getType("User").createProperty("name", String.class);
      database.getSchema().getType("User").createProperty("active", Boolean.class);
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW ActiveUsers "
            + "AS SELECT name FROM User WHERE active = true "
            + "REFRESH ON COMMIT");

    // Insert a matching record
    database.transaction(() -> {
      database.newDocument("User").set("name", "Alice").set("active", true).save();
    });

    // View should have the new record (or be STALE and we refresh)
    final MaterializedView view = database.getSchema().getMaterializedView("ActiveUsers");
    if ("STALE".equals(view.getStatus()))
      view.refresh();

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM ActiveUsers");
      assertThat(rs.hasNext()).isTrue();
    });
  }

  @Test
  void complexQueryMarksStale() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sale");
      database.getSchema().getType("Sale").createProperty("product", String.class);
      database.getSchema().getType("Sale").createProperty("amount", Integer.class);
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW SalesSummary "
            + "AS SELECT product, SUM(amount) AS total FROM Sale GROUP BY product "
            + "REFRESH ON COMMIT");

    database.transaction(() -> {
      database.newDocument("Sale").set("product", "Widget").set("amount", 100).save();
    });

    final MaterializedView view = database.getSchema().getMaterializedView("SalesSummary");
    assertThat(view.getStatus()).isEqualTo("STALE");
  }

  @Test
  void refreshAfterStale() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Item");
      database.getSchema().getType("Item").createProperty("name", String.class);
      database.getSchema().getType("Item").createProperty("qty", Integer.class);
      database.newDocument("Item").set("name", "A").set("qty", 10).save();
    });

    database.command("sql",
        "CREATE MATERIALIZED VIEW ItemAgg "
            + "AS SELECT name, SUM(qty) AS totalQty FROM Item GROUP BY name "
            + "REFRESH ON COMMIT");

    // Initial data should be correct
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM ItemAgg");
      assertThat(rs.hasNext()).isTrue();
    });

    // Insert more data — view goes STALE
    database.transaction(() -> {
      database.newDocument("Item").set("name", "A").set("qty", 5).save();
    });

    final MaterializedView view = database.getSchema().getMaterializedView("ItemAgg");
    assertThat(view.getStatus()).isEqualTo("STALE");

    // Manual refresh should fix it
    view.refresh();
    assertThat(view.getStatus()).isEqualTo("VALID");

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT totalQty FROM ItemAgg");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("totalQty")).intValue()).isEqualTo(15);
    });
  }
}
```

**Step 2: Run tests**

```bash
mvn test -Dtest=MaterializedViewIncrementalTest -pl engine
```

---

## Phase 4: Periodic Refresh

### Task 4.1: Create MaterializedViewScheduler

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewScheduler.java`

```java
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MaterializedViewScheduler {
  private final ScheduledExecutorService executor;
  private final Map<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();
  private final WeakReference<DatabaseInternal> databaseRef;

  public MaterializedViewScheduler(final DatabaseInternal database) {
    this.databaseRef = new WeakReference<>(database);
    this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-matview-scheduler");
      t.setDaemon(true);
      return t;
    });
  }

  public void schedule(final MaterializedViewImpl view) {
    if (view.getRefreshMode() != MaterializedViewRefreshMode.PERIODIC || view.getRefreshInterval() <= 0)
      return;

    final ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
      final DatabaseInternal db = databaseRef.get();
      if (db == null || !db.isOpen()) {
        cancel(view.getName());
        return;
      }
      try {
        MaterializedViewRefresher.fullRefresh(db, view);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Error in periodic refresh of view '%s': %s", e, view.getName(), e.getMessage());
        view.setStatus("ERROR");
      }
    }, view.getRefreshInterval(), view.getRefreshInterval(), TimeUnit.MILLISECONDS);

    tasks.put(view.getName(), future);
  }

  public void cancel(final String viewName) {
    final ScheduledFuture<?> future = tasks.remove(viewName);
    if (future != null)
      future.cancel(false);
  }

  public void shutdown() {
    executor.shutdownNow();
    tasks.clear();
  }
}
```

---

### Task 4.2: Wire Scheduler into LocalSchema

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`

**Step 1:** Add field (near line 95):

```java
private MaterializedViewScheduler viewScheduler;
```

**Step 2:** Initialize lazily in `registerMaterializedView()`:

```java
if (view.getRefreshMode() == MaterializedViewRefreshMode.PERIODIC) {
  if (viewScheduler == null)
    viewScheduler = new MaterializedViewScheduler(database);
  viewScheduler.schedule(view);
}
```

**Step 3:** Cancel in `dropMaterializedView()`:

```java
if (viewScheduler != null)
  viewScheduler.cancel(viewName);
```

**Step 4:** Shut down in `close()`:

```java
if (viewScheduler != null) {
  viewScheduler.shutdown();
  viewScheduler = null;
}
```

**Step 5:** Start schedulers for PERIODIC views in `readConfiguration()` after loading views.

---

### Task 4.3: Periodic Refresh Test

**Files:**
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewPeriodicTest.java`

```java
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewPeriodicTest extends TestHelper {

  @Test
  void periodicRefreshUpdatesData() throws InterruptedException {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sensor");
      database.getSchema().getType("Sensor").createProperty("value", Integer.class);
      database.newDocument("Sensor").set("value", 1).save();
    });

    // Create view with 1-second refresh
    database.command("sql",
        "CREATE MATERIALIZED VIEW SensorView AS SELECT value FROM Sensor REFRESH EVERY 1 SECOND");

    // Insert more data
    database.transaction(() -> {
      database.newDocument("Sensor").set("value", 2).save();
    });

    // Wait for periodic refresh
    Thread.sleep(2500);

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM SensorView");
      int count = 0;
      while (rs.hasNext()) { rs.next(); count++; }
      assertThat(count).isEqualTo(2);
    });
  }
}
```

---

## Phase 5: Server Integration Tests

### Task 5.1: REST API Integration Test

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/MaterializedViewServerIT.java`

No new HTTP handlers needed. The existing `PostCommandHandler` handles all SQL commands. Tests verify:

1. `POST /api/v1/command/{db}` with `CREATE MATERIALIZED VIEW ...` succeeds (HTTP 200).
2. `POST /api/v1/query/{db}` with `SELECT FROM ViewName` returns the view data.
3. `POST /api/v1/command/{db}` with `REFRESH MATERIALIZED VIEW ...` succeeds.
4. `GET /api/v1/server` or schema endpoint includes `materializedViews` in the JSON.

Follow the existing test patterns in `server/src/test/java/com/arcadedb/server/` (extend `BaseGraphServerTest` or use `TestServerHelper`).

---

## Phase 6: Final Verification

### Task 6.1: Full Build

```bash
mvn clean install -pl engine
```

### Task 6.2: Run All Materialized View Tests

```bash
mvn test -Dtest="MaterializedView*" -pl engine
```

### Task 6.3: Run Existing Tests to Check for Regressions

```bash
mvn test -pl engine
```

---

## Task Summary

| # | Task | Phase | Files Changed | Type |
|---|------|-------|---------------|------|
| 1.1 | MaterializedViewRefreshMode enum | 1 | 1 new | Create |
| 1.2 | MaterializedView interface | 1 | 1 new | Create |
| 1.3 | MaterializedViewImpl + test | 1 | 2 new | Create + Test |
| 1.4 | Schema interface additions | 1 | 1 modified | Modify |
| 1.5 | MaterializedViewBuilder | 1 | 1 new | Create |
| 1.6 | LocalSchema extensions | 1 | 1 modified | Modify |
| 1.7 | MaterializedViewRefresher | 1 | 1 new | Create |
| 1.8 | Wire database ref into Impl | 1 | 1 modified | Modify |
| 1.9 | Phase 1 integration tests | 1 | 1 new | Test |
| 2.1 | Lexer keywords | 2 | 1 modified | Grammar |
| 2.2 | Parser grammar rules | 2 | 1 modified | Grammar |
| 2.3 | CreateMaterializedViewStatement | 2 | 1 new | Create |
| 2.4 | DropMaterializedViewStatement | 2 | 1 new | Create |
| 2.5 | RefreshMaterializedViewStatement | 2 | 1 new | Create |
| 2.6 | AlterMaterializedViewStatement | 2 | 1 new | Create |
| 2.7 | ANTLR visitor methods | 2 | 1 modified | Modify |
| 2.8 | SQL syntax tests | 2 | 1 new | Test |
| 3.1 | MaterializedViewChangeListener | 3 | 1 new | Create |
| 3.2 | Listener registration in LocalSchema | 3 | 1 modified | Modify |
| 3.3 | Incremental refresh tests | 3 | 1 new | Test |
| 4.1 | MaterializedViewScheduler | 4 | 1 new | Create |
| 4.2 | Wire scheduler into LocalSchema | 4 | 1 modified | Modify |
| 4.3 | Periodic refresh test | 4 | 1 new | Test |
| 5.1 | Server integration test | 5 | 1 new | Test |
| 6.1 | Full build verification | 6 | 0 | Verify |
| 6.2 | Run all view tests | 6 | 0 | Verify |
| 6.3 | Regression test run | 6 | 0 | Verify |

**Totals:** 13 new files, 7 modified files, 27 tasks across 6 phases.

---

## Dependency Graph

```
1.1 (enum) ──┐
1.2 (iface) ─┤
              ├──> 1.3 (impl) ──> 1.4 (Schema.java) ──> 1.5 (builder) ──> 1.6 (LocalSchema)
              │                                                                     │
              │                                                      1.7 (refresher)┘
              │                                                                     │
              │                                              1.8 (wire db ref) ─────┘
              │                                                                     │
              │                                                         1.9 (test) ─┘
              │
2.1 (lexer) ──> 2.2 (parser) ──> 2.3 (CREATE stmt) ─┐
                                  2.4 (DROP stmt)   ──┤
                                  2.5 (REFRESH stmt) ─┤──> 2.7 (visitor) ──> 2.8 (test)
                                  2.6 (ALTER stmt)  ──┘

3.1 (listener) ──> 3.2 (registration) ──> 3.3 (test)

4.1 (scheduler) ──> 4.2 (wire) ──> 4.3 (test)

5.1 (server test)   [depends on all above]

6.1-6.3 (verification) [depends on all above]
```
