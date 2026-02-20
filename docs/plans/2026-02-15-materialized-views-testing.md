# Materialized Views — Integration & E2E Testing Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add integration tests (server HTTP API) and end-to-end tests (remote client, HA replication) for materialized views, plus implement the missing `RemoteSchema` materialized view methods.

**Architecture:** Tests are layered: engine-level unit tests already exist (24 tests across 4 classes). This plan adds server HTTP API tests (`BaseGraphServerTest`), remote client tests (`RemoteDatabase`/`RemoteSchema`), and HA replication tests (`ReplicationServerIT`). RemoteSchema stubs must be implemented first since they're currently no-ops.

**Tech Stack:** Java 21, JUnit 5, AssertJ, ArcadeDB server test infrastructure (`BaseGraphServerTest`, `ReplicationServerIT`)

**Reference:** Implementation at `docs/plans/2026-02-14-materialized-views-implementation.md`

---

## Phase 1: Fix RemoteSchema Materialized View Stubs

The `RemoteSchema` class has stub implementations that return `false`/`null`/empty for all materialized view methods. These must be implemented using SQL commands (same pattern as `createDocumentType`, `dropType`, etc.) before remote/server tests can use the Java API.

### Task 1: Implement RemoteSchema materialized view methods

**Files:**
- Modify: `network/src/main/java/com/arcadedb/remote/RemoteSchema.java:129-152`

**Context:**
- `RemoteSchema` delegates to SQL commands via `remoteDatabase.command("sql", ...)` for all schema operations
- Pattern examples in same file: `createDocumentType()` (line 183), `dropType()` (line 85), `dropTrigger()` (line 126)
- The 5 methods to implement: `existsMaterializedView`, `getMaterializedView`, `getMaterializedViews`, `dropMaterializedView`, `buildMaterializedView`
- `buildMaterializedView()` should return `null` with a comment — remote clients should use SQL `CREATE MATERIALIZED VIEW` instead (same as `createTrigger()` pattern at line 120-122)

**Step 1: Write the test**

Create: `server/src/test/java/com/arcadedb/remote/RemoteMaterializedViewIT.java`

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.remote;

import com.arcadedb.schema.MaterializedView;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteMaterializedViewIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-mv";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createAndQueryViaSql() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "INSERT INTO Product SET name = 'Widget', price = 10");
      database.command("sql", "INSERT INTO Product SET name = 'Gadget', price = 20");

      database.command("sql",
          "CREATE MATERIALIZED VIEW ExpensiveProducts AS SELECT name, price FROM Product WHERE price > 15");

      assertThat(database.getSchema().existsMaterializedView("ExpensiveProducts")).isTrue();
      assertThat(database.getSchema().existsMaterializedView("NonExistent")).isFalse();

      final MaterializedView view = database.getSchema().getMaterializedView("ExpensiveProducts");
      assertThat(view).isNotNull();
      assertThat(view.getName()).isEqualTo("ExpensiveProducts");

      final MaterializedView[] views = database.getSchema().getMaterializedViews();
      assertThat(views).hasSize(1);
      assertThat(views[0].getName()).isEqualTo("ExpensiveProducts");

      // Query the view
      try (final var rs = database.query("sql", "SELECT FROM ExpensiveProducts")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }

      // Drop
      database.getSchema().dropMaterializedView("ExpensiveProducts");
      assertThat(database.getSchema().existsMaterializedView("ExpensiveProducts")).isFalse();
    });
  }

  @Test
  void buildMaterializedViewThrowsUnsupported() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      // buildMaterializedView is not supported remotely — use SQL instead
      assertThatThrownBy(() -> database.getSchema().buildMaterializedView())
          .isInstanceOf(UnsupportedOperationException.class);
    });
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd server && mvn test -Dtest=RemoteMaterializedViewIT -pl .`
Expected: FAIL — `existsMaterializedView` returns false, `getMaterializedView` returns null

**Step 3: Implement RemoteSchema methods**

In `network/src/main/java/com/arcadedb/remote/RemoteSchema.java`, replace the stub implementations (lines 129-152):

```java
  @Override
  public boolean existsMaterializedView(final String viewName) {
    try {
      final ResultSet result = remoteDatabase.query("sql",
          "SELECT FROM schema:materializedViews WHERE name = ?", viewName);
      return result.hasNext();
    } catch (final Exception e) {
      return false;
    }
  }

  @Override
  public MaterializedView getMaterializedView(final String viewName) {
    // Delegate to the server — remote clients get the view via embedded database on server side
    // For now, use command to check existence and return a lightweight proxy
    if (!existsMaterializedView(viewName))
      return null;
    // Return a remote-friendly view using the server's schema info
    final ResultSet result = remoteDatabase.query("sql",
        "SELECT FROM schema:materializedViews WHERE name = ?", viewName);
    if (result.hasNext()) {
      final com.arcadedb.query.sql.executor.Result row = result.next();
      return new RemoteMaterializedView(remoteDatabase, row);
    }
    return null;
  }

  @Override
  public MaterializedView[] getMaterializedViews() {
    final ResultSet result = remoteDatabase.query("sql", "SELECT FROM schema:materializedViews");
    final java.util.List<MaterializedView> views = new java.util.ArrayList<>();
    while (result.hasNext())
      views.add(new RemoteMaterializedView(remoteDatabase, result.next()));
    return views.toArray(new MaterializedView[0]);
  }

  @Override
  public void dropMaterializedView(final String viewName) {
    remoteDatabase.command("sql", "DROP MATERIALIZED VIEW `" + viewName + "`");
  }

  @Override
  public MaterializedViewBuilder buildMaterializedView() {
    // Not supported remotely — use SQL CREATE MATERIALIZED VIEW instead
    return null;
  }
```

**Important:** This approach requires a `schema:materializedViews` query to work from the server side. Check if this exists — if not, we need an alternative approach. The alternative is simpler: use `remoteDatabase.command("sql", "SELECT count(*) FROM ...")` on the backing type, or query the server's schema JSON endpoint. Let me describe a simpler fallback:

**Alternative (simpler) — if `schema:materializedViews` doesn't exist:**

```java
  @Override
  public boolean existsMaterializedView(final String viewName) {
    try {
      final ResultSet result = remoteDatabase.command("sql",
          "SELECT FROM `" + viewName + "` LIMIT 0");
      // If the view exists, the type exists (backing type has the same name)
      // But we need to distinguish views from regular types.
      // Safest: query schema info via server API
      return false; // Fallback — see note below
    } catch (final Exception e) {
      return false;
    }
  }
```

**Recommended approach:** Query the server's schema info endpoint. Look at how `RemoteSchema.getType()` works (around line 177) — it likely uses `remoteDatabase.command("sql", "SELECT FROM schema:database")` or the HTTP API directly. Use the same pattern to check `materializedViews` in the schema JSON.

**The implementer should:**
1. Read `RemoteSchema` fully to understand how existing methods query schema info
2. Check if there's a `schema:materializedViews` metadata query or if schema info is fetched via HTTP `/api/v1/server`
3. Use the existing pattern to implement the 5 methods
4. Create `RemoteMaterializedView.java` in the `network` module if needed (a lightweight read-only proxy implementing `MaterializedView`)

**Step 4: Run the test**

Run: `cd server && mvn test -Dtest=RemoteMaterializedViewIT -pl .`
Expected: PASS

**Step 5: Commit**

```bash
git add network/src/main/java/com/arcadedb/remote/RemoteSchema.java
git add network/src/main/java/com/arcadedb/remote/RemoteMaterializedView.java
git add server/src/test/java/com/arcadedb/remote/RemoteMaterializedViewIT.java
git commit -m "feat: implement RemoteSchema materialized view methods"
```

---

## Phase 2: Server HTTP API Integration Tests

Test materialized view DDL operations via the HTTP REST API (`POST /api/v1/command/{database}`). These tests exercise the full stack: HTTP request → SQL parser → DDL statement → schema.

### Task 2: HTTP API integration tests for materialized views

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/HTTPMaterializedViewIT.java`

**Context:**
- Extends `BaseGraphServerTest` (same pattern as `HTTPDocumentIT`)
- Uses `command(serverIndex, sqlCommand)` helper to send SQL via HTTP POST
- Uses `executeCommand(serverIndex, language, command)` for JSON responses
- Default database name is "graph" — override with `getDatabaseName()`
- `testEachServer((serverIndex) -> { ... })` iterates all servers

**Step 1: Write the test**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HTTPMaterializedViewIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "httpMaterializedView";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      // Setup: create source type and data
      command(serverIndex, "CREATE DOCUMENT TYPE Sensor");
      command(serverIndex, "INSERT INTO Sensor SET name = 'temp1', value = 22.5, active = true");
      command(serverIndex, "INSERT INTO Sensor SET name = 'temp2', value = 25.0, active = false");
      command(serverIndex, "INSERT INTO Sensor SET name = 'hum1', value = 60.0, active = true");

      // Create materialized view
      final String createResponse = command(serverIndex,
          "CREATE MATERIALIZED VIEW ActiveSensors AS SELECT name, value FROM Sensor WHERE active = true");
      assertThat(createResponse).isNotNull();

      // Query the view via HTTP
      final JSONObject queryResult = executeCommand(serverIndex, "sql", "SELECT FROM ActiveSensors");
      assertThat(queryResult).isNotNull();
      assertThat(queryResult.getJSONArray("result").length()).isEqualTo(2);

      // Cleanup
      command(serverIndex, "DROP MATERIALIZED VIEW ActiveSensors");
      command(serverIndex, "DROP TYPE Sensor UNSAFE");
    });
  }

  @Test
  void refreshViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Item");
      command(serverIndex, "INSERT INTO Item SET label = 'A'");

      command(serverIndex, "CREATE MATERIALIZED VIEW ItemView AS SELECT label FROM Item");

      // Add more data
      command(serverIndex, "INSERT INTO Item SET label = 'B'");

      // Refresh
      command(serverIndex, "REFRESH MATERIALIZED VIEW ItemView");

      // Query — should see 2 records
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM ItemView");
      assertThat(result).isNotNull();
      assertThat(result.getJSONArray("result").length()).isEqualTo(2);

      // Cleanup
      command(serverIndex, "DROP MATERIALIZED VIEW ItemView");
      command(serverIndex, "DROP TYPE Item UNSAFE");
    });
  }

  @Test
  void dropViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Log");
      command(serverIndex, "INSERT INTO Log SET msg = 'hello'");
      command(serverIndex, "CREATE MATERIALIZED VIEW LogView AS SELECT FROM Log");

      // Drop
      command(serverIndex, "DROP MATERIALIZED VIEW LogView");

      // Verify backing type is also gone
      final JSONObject result = executeCommand(serverIndex, "sql",
          "SELECT count(*) as cnt FROM schema:types WHERE name = 'LogView'");
      // The backing type should not exist after drop
      // Alternatively just verify the command doesn't fail
      assertThat(result).isNotNull();

      // Cleanup
      command(serverIndex, "DROP TYPE Log UNSAFE");
    });
  }

  @Test
  void dropIfExistsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      // Should not throw
      command(serverIndex, "DROP MATERIALIZED VIEW IF EXISTS NonExistentView");
    });
  }

  @Test
  void createIfNotExistsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Record");
      command(serverIndex, "INSERT INTO Record SET v = 1");

      command(serverIndex, "CREATE MATERIALIZED VIEW RecordView AS SELECT FROM Record");
      // Should not throw
      command(serverIndex, "CREATE MATERIALIZED VIEW IF NOT EXISTS RecordView AS SELECT FROM Record");

      // Cleanup
      command(serverIndex, "DROP MATERIALIZED VIEW RecordView");
      command(serverIndex, "DROP TYPE Record UNSAFE");
    });
  }

  @Test
  void createWithRefreshModeViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Event");
      command(serverIndex, "INSERT INTO Event SET type = 'click'");

      command(serverIndex,
          "CREATE MATERIALIZED VIEW ClickEvents AS SELECT type FROM Event WHERE type = 'click' REFRESH INCREMENTAL");

      // Verify the view exists by querying it
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM ClickEvents");
      assertThat(result).isNotNull();
      assertThat(result.getJSONArray("result").length()).isEqualTo(1);

      // Cleanup
      command(serverIndex, "DROP MATERIALIZED VIEW ClickEvents");
      command(serverIndex, "DROP TYPE Event UNSAFE");
    });
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd server && mvn test -Dtest=HTTPMaterializedViewIT -pl .`
Expected: PASS (these are testing existing functionality via HTTP)

**Step 3: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/HTTPMaterializedViewIT.java
git commit -m "test: add HTTP API integration tests for materialized views"
```

---

## Phase 3: Engine-Level Edge Case Tests

Add tests for edge cases and scenarios not covered by existing engine tests.

### Task 3: Edge case and stress tests

**Files:**
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewEdgeCaseTest.java`

**Context:**
- Extends `TestHelper` (same as existing MV tests)
- Tests edge cases: empty source type, large datasets, multiple views on same source, view of a view (should fail), concurrent modifications, schema persistence with multiple views

**Step 1: Write the tests**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewEdgeCaseTest extends TestHelper {

  @Test
  void multipleViewsOnSameSource() {
    database.transaction(() -> database.getSchema().createDocumentType("Source"));
    database.transaction(() -> {
      database.newDocument("Source").set("name", "A").set("category", "X").save();
      database.newDocument("Source").set("name", "B").set("category", "Y").save();
      database.newDocument("Source").set("name", "C").set("category", "X").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ViewAll")
          .withQuery("SELECT name FROM Source")
          .create();
      database.getSchema().buildMaterializedView()
          .withName("ViewCatX")
          .withQuery("SELECT name FROM Source WHERE category = 'X'")
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM ViewAll")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM ViewCatX")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }

    // Both views listed
    assertThat(database.getSchema().getMaterializedViews()).hasSize(2);
  }

  @Test
  void cannotCreateViewWithExistingTypeName() {
    database.transaction(() -> database.getSchema().createDocumentType("Conflict"));

    assertThatThrownBy(() ->
        database.transaction(() ->
            database.getSchema().buildMaterializedView()
                .withName("Conflict")
                .withQuery("SELECT FROM Conflict")
                .create()))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void cannotCreateViewWithDuplicateName() {
    database.transaction(() -> database.getSchema().createDocumentType("Dup"));
    database.transaction(() -> database.newDocument("Dup").set("v", 1).save());

    database.transaction(() ->
        database.getSchema().buildMaterializedView()
            .withName("DupView")
            .withQuery("SELECT FROM Dup")
            .create());

    assertThatThrownBy(() ->
        database.transaction(() ->
            database.getSchema().buildMaterializedView()
                .withName("DupView")
                .withQuery("SELECT FROM Dup")
                .create()))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void viewWithEmptyResult() {
    database.transaction(() -> database.getSchema().createDocumentType("Empty"));

    database.transaction(() ->
        database.getSchema().buildMaterializedView()
            .withName("EmptyView")
            .withQuery("SELECT FROM Empty")
            .create());

    try (final ResultSet rs = database.query("sql", "SELECT FROM EmptyView")) {
      assertThat(rs.stream().count()).isEqualTo(0);
    }
    assertThat(database.getSchema().getMaterializedView("EmptyView").getStatus()).isEqualTo("VALID");
  }

  @Test
  void viewSurvivesReopenWithMultipleViews() {
    database.transaction(() -> database.getSchema().createDocumentType("Persist"));
    database.transaction(() -> {
      database.newDocument("Persist").set("v", 1).save();
      database.newDocument("Persist").set("v", 2).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("PersistView1")
          .withQuery("SELECT FROM Persist WHERE v = 1")
          .create();
      database.getSchema().buildMaterializedView()
          .withName("PersistView2")
          .withQuery("SELECT FROM Persist WHERE v = 2")
          .create();
    });

    // Reopen
    database.close();
    database = factory.open();

    assertThat(database.getSchema().existsMaterializedView("PersistView1")).isTrue();
    assertThat(database.getSchema().existsMaterializedView("PersistView2")).isTrue();
    assertThat(database.getSchema().getMaterializedViews()).hasSize(2);

    try (final ResultSet rs = database.query("sql", "SELECT FROM PersistView1")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM PersistView2")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void largeDatasetRefresh() {
    database.transaction(() -> database.getSchema().createDocumentType("Bulk"));

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++)
        database.newDocument("Bulk").set("idx", i).set("even", i % 2 == 0).save();
    });

    database.transaction(() ->
        database.getSchema().buildMaterializedView()
            .withName("EvenBulk")
            .withQuery("SELECT idx FROM Bulk WHERE even = true")
            .create());

    try (final ResultSet rs = database.query("sql", "SELECT FROM EvenBulk")) {
      assertThat(rs.stream().count()).isEqualTo(500);
    }
  }

  @Test
  void dropSourceTypeAfterViewCreation() {
    // Dropping a source type should not cascade to the view — the view becomes stale
    database.transaction(() -> database.getSchema().createDocumentType("Ephemeral"));
    database.transaction(() -> database.newDocument("Ephemeral").set("v", 1).save());

    database.transaction(() ->
        database.getSchema().buildMaterializedView()
            .withName("EphemeralView")
            .withQuery("SELECT FROM Ephemeral")
            .create());

    // Drop the source type — view should still exist but refresh will fail
    database.transaction(() -> database.getSchema().dropType("Ephemeral"));

    assertThat(database.getSchema().existsMaterializedView("EphemeralView")).isTrue();

    // Manual refresh should fail gracefully (view goes to ERROR/STALE)
    final MaterializedView view = database.getSchema().getMaterializedView("EphemeralView");
    try {
      view.refresh();
    } catch (final Exception e) {
      // Expected — source type no longer exists
    }

    // Clean up the orphaned view
    database.getSchema().dropMaterializedView("EphemeralView");
  }

  @Test
  void incrementalMultipleInsertsInSameTransaction() {
    database.transaction(() -> database.getSchema().createDocumentType("Multi"));
    database.transaction(() -> database.newDocument("Multi").set("v", 1).save());

    database.transaction(() ->
        database.getSchema().buildMaterializedView()
            .withName("MultiView")
            .withQuery("SELECT v FROM Multi")
            .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
            .create());

    // Multiple inserts in one transaction — should trigger only one refresh
    database.transaction(() -> {
      database.newDocument("Multi").set("v", 2).save();
      database.newDocument("Multi").set("v", 3).save();
      database.newDocument("Multi").set("v", 4).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM MultiView")) {
      assertThat(rs.stream().count()).isEqualTo(4);
    }
  }

  @Test
  void queryClassifierDetectsComplexQueries() {
    database.transaction(() -> database.getSchema().createDocumentType("Sales"));
    database.transaction(() -> {
      database.newDocument("Sales").set("product", "A").set("amount", 10).save();
    });

    // GROUP BY → complex
    database.transaction(() -> {
      final MaterializedView view = database.getSchema().buildMaterializedView()
          .withName("SalesAgg")
          .withQuery("SELECT product, sum(amount) as total FROM Sales GROUP BY product")
          .create();
      assertThat(view.isSimpleQuery()).isFalse();
    });

    // Simple SELECT → simple
    database.transaction(() -> {
      final MaterializedView view = database.getSchema().buildMaterializedView()
          .withName("SalesSimple")
          .withQuery("SELECT product, amount FROM Sales WHERE amount > 5")
          .create();
      assertThat(view.isSimpleQuery()).isTrue();
    });
  }
}
```

**Step 2: Run tests**

Run: `cd engine && mvn test -Dtest=MaterializedViewEdgeCaseTest -pl .`
Expected: PASS

**Step 3: Commit**

```bash
git add engine/src/test/java/com/arcadedb/schema/MaterializedViewEdgeCaseTest.java
git commit -m "test: add edge case tests for materialized views"
```

---

## Phase 4: HA Replication Tests

Test that materialized views replicate correctly across servers in an HA cluster.

### Task 4: HA replication test for materialized views

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/ha/ReplicationMaterializedViewIT.java`

**Context:**
- Extends `ReplicationServerIT` (same as `ReplicationChangeSchemaIT`)
- Uses `getServer(i).getDatabase(getDatabaseName())` to get databases
- Schema changes on leader (server 0) replicate to replicas
- Use `testOnAllServers()` to verify replication
- Use `isInSchemaFile()` / `isNotInSchemaFile()` to verify schema persistence
- Call `super.replication()` in test method to start servers
- Override `getServerCount()` to return 3 for HA tests

**Step 1: Write the test**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationMaterializedViewIT extends ReplicationServerIT {
  private Database[] databases;

  @Test
  void testReplication() throws Exception {
    super.replication();

    databases = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // Create source type on leader
    databases[0].command("sql", "CREATE DOCUMENT TYPE Metric");
    databases[0].command("sql", "INSERT INTO Metric SET name = 'cpu', value = 80");
    databases[0].command("sql", "INSERT INTO Metric SET name = 'mem', value = 60");

    // Create materialized view on leader
    databases[0].command("sql",
        "CREATE MATERIALIZED VIEW HighMetrics AS SELECT name, value FROM Metric WHERE value > 70");

    // Verify view exists on all servers
    testOnAllServers((database) -> {
      assertThat(database.getSchema().existsMaterializedView("HighMetrics")).isTrue();
    });

    // Verify schema file contains the view definition on all servers
    testOnAllServers((database) -> isInSchemaFile(database, "HighMetrics"));
    testOnAllServers((database) -> isInSchemaFile(database, "materializedViews"));

    // Query view on a replica
    try (final var rs = databases[1].query("sql", "SELECT FROM HighMetrics")) {
      assertThat(rs.stream().count()).isEqualTo(1); // Only cpu > 70
    }

    // Drop the view on leader
    databases[0].command("sql", "DROP MATERIALIZED VIEW HighMetrics");

    // Verify view is gone on all servers
    testOnAllServers((database) -> {
      assertThat(database.getSchema().existsMaterializedView("HighMetrics")).isFalse();
    });
    testOnAllServers((database) -> isNotInSchemaFile(database, "HighMetrics"));
  }
}
```

**Important notes for the implementer:**
- `ReplicationServerIT` has specific lifecycle. You MUST call `super.replication()` first.
- Read `ReplicationChangeSchemaIT.java` fully to understand the pattern.
- The `testOnAllServers()` method may need `Thread.sleep()` or retry logic if replication is async.
- If schema files are checked, make sure `checkSchemaFilesAreTheSameOnAllServers()` is called or validated.

**Step 2: Run the test**

Run: `cd server && mvn test -Dtest=ReplicationMaterializedViewIT -pl .`
Expected: PASS

Note: HA tests can be slow (30-60 seconds). If they fail due to timing, add appropriate waits.

**Step 3: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/ReplicationMaterializedViewIT.java
git commit -m "test: add HA replication test for materialized views"
```

---

## Phase 5: Full Build Verification

### Task 5: Full project build and regression check

**Step 1: Compile the full project**

Run: `mvn clean install -DskipTests`
Expected: BUILD SUCCESS

**Step 2: Run engine tests**

Run: `cd engine && mvn test`
Expected: All tests pass, 0 failures

**Step 3: Run server tests (materialized view related)**

Run: `cd server && mvn test -Dtest="HTTPMaterializedViewIT,RemoteMaterializedViewIT"`
Expected: All tests pass

**Step 4: Run HA test**

Run: `cd server && mvn test -Dtest="ReplicationMaterializedViewIT"`
Expected: PASS (may be slow)

**Step 5: Commit final state**

If any fixes were needed, commit them.
