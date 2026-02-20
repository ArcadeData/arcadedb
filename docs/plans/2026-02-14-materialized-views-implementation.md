# Materialized Views Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add materialized views to ArcadeDB with full refresh, incremental (post-commit) refresh, and periodic refresh, driven by a new transaction callback mechanism.

**Architecture:** A materialized view is a schema-level object that wraps a SQL SELECT query. Its data lives in a backing `DocumentType` with standard buckets. Refresh uses post-commit callbacks on `TransactionContext` (new general-purpose mechanism). Schema persistence uses a `"materializedViews"` section in `schema.json`. ANTLR4 grammar extensions add `CREATE/DROP/REFRESH/ALTER MATERIALIZED VIEW` SQL syntax.

**Tech Stack:** Java 21, ArcadeDB engine, ANTLR4 SQL parser, JUnit 5, AssertJ

**Reference:** Design document at `docs/plans/2026-02-06-materialized-views-design.md`

---

## Phase 1: Transaction Callback Infrastructure

This phase adds general-purpose post-commit callbacks to `TransactionContext`. This is a prerequisite for incremental refresh but is useful independently.

### Task 1: TransactionContext post-commit callbacks — test

**Files:**
- Create: `engine/src/test/java/com/arcadedb/database/TransactionCallbackTest.java`

**Step 1: Write the failing tests**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionCallbackTest extends TestHelper {

  @Test
  void callbackFiresAfterCommit() {
    final AtomicBoolean fired = new AtomicBoolean(false);

    database.begin();
    database.getSchema().createDocumentType("CallbackTest1");
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> fired.set(true));
    database.commit();

    assertThat(fired.get()).isTrue();
  }

  @Test
  void callbackNotFiredOnRollback() {
    final AtomicBoolean fired = new AtomicBoolean(false);

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> fired.set(true));
    database.rollback();

    assertThat(fired.get()).isFalse();
  }

  @Test
  void multipleCallbacksFireInOrder() {
    final List<Integer> order = new ArrayList<>();

    database.begin();
    database.getSchema().createDocumentType("CallbackTest2");
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> order.add(1));
    tx.addAfterCommitCallback(() -> order.add(2));
    tx.addAfterCommitCallback(() -> order.add(3));
    database.commit();

    assertThat(order).containsExactly(1, 2, 3);
  }

  @Test
  void callbackErrorDoesNotAffectCommit() {
    final AtomicInteger counter = new AtomicInteger(0);

    database.begin();
    database.getSchema().createDocumentType("CallbackTest3");
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> counter.incrementAndGet());
    tx.addAfterCommitCallback(() -> { throw new RuntimeException("test error"); });
    tx.addAfterCommitCallback(() -> counter.incrementAndGet());
    database.commit();

    // All callbacks should have run despite the error in the second one
    assertThat(counter.get()).isEqualTo(2);
    // And the type should exist (commit succeeded)
    assertThat(database.getSchema().existsType("CallbackTest3")).isTrue();
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd engine && mvn test -Dtest=TransactionCallbackTest -pl . -DfailIfNoTests=false`
Expected: FAIL — `addAfterCommitCallback` method does not exist

**Step 3: Commit test**

```
git add engine/src/test/java/com/arcadedb/database/TransactionCallbackTest.java
git commit -m "test: add TransactionContext post-commit callback tests"
```

---

### Task 2: TransactionContext post-commit callbacks — implementation

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/database/TransactionContext.java`

The fields are at lines 59-83. The `commit2ndPhase()` method is at lines 659-715 with `reset()` at line 713 in the finally block. The `rollback()` method is at lines 221-257 with `reset()` at line 256.

**Step 1: Add the `afterCommitCallbacks` field and `addAfterCommitCallback()` method**

Add after line 83 (after `private Object requester;`):

```java
  private       List<Runnable>                         afterCommitCallbacks  = null;
```

Add a new public method (anywhere in the class, e.g., after `rollback()`):

```java
  /**
   * Registers a callback to be executed after a successful commit. Callbacks fire in registration order.
   * If the transaction is rolled back, callbacks are discarded without firing.
   * Callback exceptions are logged but do not affect the commit or other callbacks.
   */
  public void addAfterCommitCallback(final Runnable callback) {
    if (afterCommitCallbacks == null)
      afterCommitCallbacks = new ArrayList<>();
    afterCommitCallbacks.add(callback);
  }
```

**Step 2: Fire callbacks in `commit2ndPhase()` after commit succeeds**

In `commit2ndPhase()` (line 659), the try block runs commit logic and the finally block calls `reset()` at line 713. We need to fire callbacks **after** the commit succeeds but **before** `reset()` clears state. Restructure the finally block:

Replace the finally block at lines 712-714:
```java
    } finally {
      reset();
    }
```

With:
```java
    } finally {
      final List<Runnable> callbacks = afterCommitCallbacks;
      reset();
      if (callbacks != null) {
        for (final Runnable callback : callbacks) {
          try {
            callback.run();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING,
                "Error in post-commit callback: %s", e, e.getMessage());
          }
        }
      }
    }
```

Note: We capture the callbacks list before `reset()` clears it, then fire after `reset()`. This way the transaction state is clean when callbacks run (they can start new transactions).

**Step 3: Clear callbacks in `reset()`**

Find the `reset()` method and add `afterCommitCallbacks = null;` to it. The reset method clears all transaction state.

**Step 4: Ensure `rollback()` clears callbacks**

The `rollback()` method at line 221 already calls `reset()` at line 256, which will clear `afterCommitCallbacks`. No additional changes needed.

**Step 5: Add the import for `ArrayList`**

Check if `java.util.ArrayList` is already imported. If not, add it to the imports.

**Step 6: Run the tests**

Run: `cd engine && mvn test -Dtest=TransactionCallbackTest -pl .`
Expected: All 4 tests PASS

**Step 7: Commit**

```
git add engine/src/main/java/com/arcadedb/database/TransactionContext.java
git commit -m "feat: add post-commit callbacks to TransactionContext"
```

---

## Phase 2: Core Schema Infrastructure

### Task 3: MaterializedViewRefreshMode enum

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefreshMode.java`

**Step 1: Create the enum**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

public enum MaterializedViewRefreshMode {
  MANUAL,
  INCREMENTAL,
  PERIODIC
}
```

**Step 2: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewRefreshMode.java
git commit -m "feat: add MaterializedViewRefreshMode enum"
```

---

### Task 4: MaterializedView interface

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedView.java`

**Step 1: Create the interface**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

  boolean isSimpleQuery();

  void refresh();

  void drop();

  JSONObject toJSON();
}
```

**Step 2: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/MaterializedView.java
git commit -m "feat: add MaterializedView interface"
```

---

### Task 5: MaterializedViewImpl — test and implementation

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewImpl.java`
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java` (first tests)

**Step 1: Write a test for JSON round-trip serialization**

Create `engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewTest extends TestHelper {

  @Test
  void jsonRoundTrip() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "SalesSummary",
        "SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer",
        "SalesSummary", List.of("Order"),
        MaterializedViewRefreshMode.MANUAL, false, 0);

    final JSONObject json = view.toJSON();
    final MaterializedViewImpl restored = MaterializedViewImpl.fromJSON(database, json);

    assertThat(restored.getName()).isEqualTo("SalesSummary");
    assertThat(restored.getQuery()).isEqualTo("SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer");
    assertThat(restored.getSourceTypeNames()).containsExactly("Order");
    assertThat(restored.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
    assertThat(restored.getStatus()).isEqualTo("VALID");
    assertThat(restored.isSimpleQuery()).isFalse();
  }

  @Test
  void statusTransitions() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "TestView",
        "SELECT name FROM User WHERE active = true",
        "TestView", List.of("User"),
        MaterializedViewRefreshMode.MANUAL, true, 0);

    assertThat(view.getStatus()).isEqualTo("VALID");

    view.setStatus("STALE");
    assertThat(view.getStatus()).isEqualTo("STALE");

    view.setStatus("BUILDING");
    assertThat(view.getStatus()).isEqualTo("BUILDING");

    view.setStatus("ERROR");
    assertThat(view.getStatus()).isEqualTo("ERROR");

    view.setStatus("VALID");
    view.updateLastRefreshTime();
    assertThat(view.getLastRefreshTime()).isGreaterThan(0);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd engine && mvn test -Dtest=MaterializedViewTest -pl . -DfailIfNoTests=false`
Expected: FAIL — `MaterializedViewImpl` class does not exist

**Step 3: Implement MaterializedViewImpl**

Create `engine/src/main/java/com/arcadedb/schema/MaterializedViewImpl.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MaterializedViewImpl implements MaterializedView {
  private final Database database;
  private final String name;
  private final String query;
  private final String backingTypeName;
  private final List<String> sourceTypeNames;
  private final MaterializedViewRefreshMode refreshMode;
  private final boolean simpleQuery;
  private final long refreshInterval;
  private long lastRefreshTime;
  private String status;

  public MaterializedViewImpl(final Database database, final String name, final String query,
      final String backingTypeName, final List<String> sourceTypeNames,
      final MaterializedViewRefreshMode refreshMode, final boolean simpleQuery,
      final long refreshInterval) {
    this.database = database;
    this.name = name;
    this.query = query;
    this.backingTypeName = backingTypeName;
    this.sourceTypeNames = List.copyOf(sourceTypeNames);
    this.refreshMode = refreshMode;
    this.simpleQuery = simpleQuery;
    this.refreshInterval = refreshInterval;
    this.lastRefreshTime = 0;
    this.status = "VALID";
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public DocumentType getBackingType() {
    return database.getSchema().getType(backingTypeName);
  }

  public String getBackingTypeName() {
    return backingTypeName;
  }

  @Override
  public List<String> getSourceTypeNames() {
    return sourceTypeNames;
  }

  @Override
  public MaterializedViewRefreshMode getRefreshMode() {
    return refreshMode;
  }

  @Override
  public long getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public boolean isSimpleQuery() {
    return simpleQuery;
  }

  public long getRefreshInterval() {
    return refreshInterval;
  }

  public void setStatus(final String status) {
    this.status = status;
  }

  public void updateLastRefreshTime() {
    this.lastRefreshTime = System.currentTimeMillis();
  }

  public void setLastRefreshTime(final long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  @Override
  public void refresh() {
    MaterializedViewRefresher.fullRefresh(database, this);
  }

  @Override
  public void drop() {
    database.getSchema().dropMaterializedView(name);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("query", query);
    json.put("backingType", backingTypeName);
    json.put("refreshMode", refreshMode.name());
    json.put("simpleQuery", simpleQuery);
    json.put("refreshInterval", refreshInterval);
    json.put("lastRefreshTime", lastRefreshTime);
    json.put("status", status);
    final JSONArray srcTypes = new JSONArray();
    for (final String src : sourceTypeNames)
      srcTypes.put(src);
    json.put("sourceTypes", srcTypes);
    return json;
  }

  public static MaterializedViewImpl fromJSON(final Database database, final JSONObject json) {
    final List<String> sourceTypes = new ArrayList<>();
    final JSONArray srcArray = json.getJSONArray("sourceTypes");
    for (int i = 0; i < srcArray.length(); i++)
      sourceTypes.add(srcArray.getString(i));

    final MaterializedViewImpl view = new MaterializedViewImpl(
        database,
        json.getString("name"),
        json.getString("query"),
        json.getString("backingType"),
        sourceTypes,
        MaterializedViewRefreshMode.valueOf(json.getString("refreshMode")),
        json.optBoolean("simpleQuery", false),
        json.optLong("refreshInterval", 0));
    view.lastRefreshTime = json.optLong("lastRefreshTime", 0);
    view.status = json.optString("status", "VALID");
    return view;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final MaterializedViewImpl that = (MaterializedViewImpl) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "MaterializedView{name='" + name + "', refreshMode=" + refreshMode +
        ", status=" + status + ", simpleQuery=" + simpleQuery + '}';
  }
}
```

Note: The `refresh()` method delegates to `MaterializedViewRefresher.fullRefresh()` which doesn't exist yet. The `drop()` method calls `schema.dropMaterializedView()` which also doesn't exist yet. These will compile but fail at runtime until later tasks implement them. For now, create a stub `MaterializedViewRefresher`:

Create `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;

public class MaterializedViewRefresher {

  public static void fullRefresh(final Database database, final MaterializedViewImpl view) {
    // TODO: implement in Task 8
    throw new UnsupportedOperationException("Full refresh not yet implemented");
  }
}
```

**Step 4: Run tests**

Run: `cd engine && mvn test -Dtest=MaterializedViewTest -pl .`
Expected: All tests PASS

**Step 5: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewImpl.java
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java
git add engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java
git commit -m "feat: add MaterializedViewImpl with JSON serialization"
```

---

### Task 6: Schema interface — add materialized view methods

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/Schema.java` (add methods near line 188, after trigger methods)

**Step 1: Add materialized view methods to Schema.java**

After the trigger methods (around line 188, after `void dropTrigger(String triggerName);`), add:

```java
  // -- Materialized View management --

  boolean existsMaterializedView(String viewName);

  MaterializedView getMaterializedView(String viewName);

  MaterializedView[] getMaterializedViews();

  void dropMaterializedView(String viewName);

  MaterializedViewBuilder buildMaterializedView();
```

**Step 2: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: FAIL — `LocalSchema` doesn't implement the new methods yet. That's expected; we'll fix in the next task.

**Step 3: Commit the interface change**

```
git add engine/src/main/java/com/arcadedb/schema/Schema.java
git commit -m "feat: add materialized view methods to Schema interface"
```

---

### Task 7: LocalSchema — implement materialized view methods + persistence

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java` (stub for now)

This is a larger task. Key integration points in `LocalSchema.java`:
- Field declarations: add `materializedViews` map near the `triggers` field (line 94)
- `toJSON()`: add serialization of materialized views (lines 1430-1456)
- `readConfiguration()`: add loading of materialized views (after triggers loading at lines 1369-1391)
- `close()`: no changes needed yet (periodic scheduler comes in Phase 5)
- `dropType()`: add protection against dropping backing types (lines 892-939)
- New methods: `existsMaterializedView()`, `getMaterializedView()`, `getMaterializedViews()`, `dropMaterializedView()`, `buildMaterializedView()`

**Step 1: Create the `MaterializedViewBuilder` stub**

Create `engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.DatabaseInternal;

public class MaterializedViewBuilder {
  private final DatabaseInternal database;
  private String name;
  private String query;
  private MaterializedViewRefreshMode refreshMode = MaterializedViewRefreshMode.MANUAL;
  private int buckets = 0;
  private int pageSize = 0;
  private long refreshInterval = 0;
  private boolean ifNotExists = false;

  public MaterializedViewBuilder(final DatabaseInternal database) {
    this.database = database;
  }

  public MaterializedViewBuilder withName(final String name) {
    this.name = name;
    return this;
  }

  public MaterializedViewBuilder withQuery(final String query) {
    this.query = query;
    return this;
  }

  public MaterializedViewBuilder withRefreshMode(final MaterializedViewRefreshMode mode) {
    this.refreshMode = mode;
    return this;
  }

  public MaterializedViewBuilder withTotalBuckets(final int buckets) {
    this.buckets = buckets;
    return this;
  }

  public MaterializedViewBuilder withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public MaterializedViewBuilder withRefreshInterval(final long intervalMs) {
    this.refreshInterval = intervalMs;
    return this;
  }

  public MaterializedViewBuilder withIgnoreIfExists(final boolean ignore) {
    this.ifNotExists = ignore;
    return this;
  }

  public MaterializedView create() {
    // TODO: full implementation in Task 9
    throw new UnsupportedOperationException("MaterializedViewBuilder.create() not yet implemented");
  }
}
```

**Step 2: Add materialized view fields and methods to `LocalSchema.java`**

Add field near line 94 (after the triggers map):

```java
  protected final Map<String, MaterializedViewImpl> materializedViews = new LinkedHashMap<>();
```

Add the following methods (after the trigger methods section):

```java
  // -- Materialized View management --

  @Override
  public boolean existsMaterializedView(final String viewName) {
    return materializedViews.containsKey(viewName);
  }

  @Override
  public MaterializedView getMaterializedView(final String viewName) {
    final MaterializedViewImpl view = materializedViews.get(viewName);
    if (view == null)
      throw new SchemaException("Materialized view '" + viewName + "' not found");
    return view;
  }

  @Override
  public MaterializedView[] getMaterializedViews() {
    return materializedViews.values().toArray(new MaterializedView[0]);
  }

  @Override
  public void dropMaterializedView(final String viewName) {
    final MaterializedViewImpl view = materializedViews.get(viewName);
    if (view == null)
      throw new SchemaException("Materialized view '" + viewName + "' not found");

    // Remove the view definition
    materializedViews.remove(viewName);

    // Drop the backing type (which drops buckets and indexes)
    if (existsType(view.getBackingTypeName()))
      dropType(view.getBackingTypeName());

    saveConfiguration();
  }

  @Override
  public MaterializedViewBuilder buildMaterializedView() {
    return new MaterializedViewBuilder((DatabaseInternal) database);
  }
```

**Step 3: Extend `toJSON()` to serialize materialized views**

In `toJSON()` (around line 1430-1456), after the triggers serialization section, add:

```java
    // Serialize materialized views
    final JSONObject mvJSON = new JSONObject();
    for (final Map.Entry<String, MaterializedViewImpl> entry : materializedViews.entrySet())
      mvJSON.put(entry.getKey(), entry.getValue().toJSON());
    root.put("materializedViews", mvJSON);
```

**Step 4: Extend `readConfiguration()` to load materialized views**

In `readConfiguration()`, after the triggers loading section (around line 1391), add:

```java
      // Load materialized views
      if (root.has("materializedViews")) {
        final JSONObject mvJSON = root.getJSONObject("materializedViews");
        for (final String viewName : mvJSON.keySet()) {
          final JSONObject viewDef = mvJSON.getJSONObject(viewName);
          final MaterializedViewImpl view = MaterializedViewImpl.fromJSON(database, viewDef);
          materializedViews.put(viewName, view);

          // Crash recovery: if status is BUILDING, it was interrupted
          if ("BUILDING".equals(view.getStatus()))
            view.setStatus("STALE");
        }
      }
```

**Step 5: Protect backing types in `dropType()`**

In `dropType()` (around line 892), add a check at the beginning of the method:

```java
    // Prevent dropping a type that is a backing type for a materialized view
    for (final MaterializedViewImpl view : materializedViews.values()) {
      if (view.getBackingTypeName().equals(typeName))
        throw new SchemaException(
            "Cannot drop type '" + typeName + "' because it is the backing type for materialized view '" + view.getName() + "'. " +
                "Drop the materialized view first with: DROP MATERIALIZED VIEW " + view.getName());
    }
```

**Step 6: Clear materialized views in `close()`**

In `close()` (around line 766-788), add before the existing `files.clear()` line:

```java
    materializedViews.clear();
```

**Step 7: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: BUILD SUCCESS

**Step 8: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/LocalSchema.java
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java
git commit -m "feat: add materialized view management to LocalSchema"
```

---

### Task 8: Full refresh logic — test and implementation

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java`
- Modify: `engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java` (add tests)

**Step 1: Add tests for full refresh**

Add these tests to `MaterializedViewTest.java`:

```java
  @Test
  void createAndQueryView() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Order");
      database.getSchema().getType("Order").createProperty("customer", Type.STRING);
      database.getSchema().getType("Order").createProperty("amount", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("Order").set("customer", "Alice").set("amount", 100).save();
      database.newDocument("Order").set("customer", "Bob").set("amount", 200).save();
      database.newDocument("Order").set("customer", "Alice").set("amount", 150).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("OrderView")
          .withQuery("SELECT customer, amount FROM Order")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    // Query the view
    try (final ResultSet rs = database.query("sql", "SELECT FROM OrderView")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
  }

  @Test
  void fullRefresh() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product");
      database.getSchema().getType("Product").createProperty("name", Type.STRING);
    });

    database.transaction(() -> {
      database.newDocument("Product").set("name", "Widget").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ProductView")
          .withQuery("SELECT name FROM Product")
          .create();
    });

    // Add more data
    database.transaction(() -> {
      database.newDocument("Product").set("name", "Gadget").save();
    });

    // View is stale — still has 1 record
    try (final ResultSet rs = database.query("sql", "SELECT FROM ProductView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Refresh
    database.getSchema().getMaterializedView("ProductView").refresh();

    // Now has 2 records
    try (final ResultSet rs = database.query("sql", "SELECT FROM ProductView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void dropView() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Item");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ItemView")
          .withQuery("SELECT FROM Item")
          .create();
    });

    assertThat(database.getSchema().existsMaterializedView("ItemView")).isTrue();
    assertThat(database.getSchema().existsType("ItemView")).isTrue();

    database.getSchema().dropMaterializedView("ItemView");

    assertThat(database.getSchema().existsMaterializedView("ItemView")).isFalse();
    assertThat(database.getSchema().existsType("ItemView")).isFalse();
  }

  @Test
  void cannotDropBackingType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Thing");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ThingView")
          .withQuery("SELECT FROM Thing")
          .create();
    });

    assertThatThrownBy(() -> database.getSchema().dropType("ThingView"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("backing type");
  }

  @Test
  void viewSurvivesReopen() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Persisted");
      database.newDocument("Persisted").set("value", 42).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("PersistedView")
          .withQuery("SELECT value FROM Persisted")
          .create();
    });

    // Close and reopen database
    database.close();
    database = factory.open();

    assertThat(database.getSchema().existsMaterializedView("PersistedView")).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT FROM PersistedView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void emptySourceType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Empty");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("EmptyView")
          .withQuery("SELECT FROM Empty")
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM EmptyView")) {
      assertThat(rs.stream().count()).isEqualTo(0);
    }
  }
```

Add the necessary imports at the top of the test file:

```java
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
```

**Step 2: Run tests to verify they fail**

Run: `cd engine && mvn test -Dtest=MaterializedViewTest -pl . -DfailIfNoTests=false`
Expected: FAIL — `MaterializedViewBuilder.create()` throws `UnsupportedOperationException`

**Step 3: Implement `MaterializedViewRefresher.fullRefresh()`**

Replace the stub in `MaterializedViewRefresher.java`:

```java
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.logging.Level;

public class MaterializedViewRefresher {

  public static void fullRefresh(final Database database, final MaterializedViewImpl view) {
    view.setStatus("BUILDING");
    try {
      database.transaction(() -> {
        final String backingTypeName = view.getBackingTypeName();

        // Truncate existing data
        database.command("sql", "DELETE FROM " + backingTypeName);

        // Execute the defining query and insert results
        try (final ResultSet rs = database.query("sql", view.getQuery())) {
          while (rs.hasNext()) {
            final Result result = rs.next();
            final MutableDocument doc = database.newDocument(backingTypeName);
            for (final String prop : result.getPropertyNames()) {
              if (!prop.startsWith("@"))
                doc.set(prop, result.getProperty(prop));
            }
            doc.save();
          }
        }
      });

      view.updateLastRefreshTime();
      view.setStatus("VALID");

    } catch (final Exception e) {
      view.setStatus("ERROR");
      LogManager.instance().log(MaterializedViewRefresher.class, Level.SEVERE,
          "Error refreshing materialized view '%s': %s", e, view.getName(), e.getMessage());
      throw e;
    }
  }
}
```

**Step 4: Implement `MaterializedViewBuilder.create()`**

Replace the stub `create()` method in `MaterializedViewBuilder.java`:

```java
  public MaterializedView create() {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Materialized view name is required");
    if (query == null || query.isEmpty())
      throw new IllegalArgumentException("Materialized view query is required");

    final LocalSchema schema = (LocalSchema) database.getSchema();

    // Check if view already exists
    if (schema.existsMaterializedView(name)) {
      if (ifNotExists)
        return schema.getMaterializedView(name);
      throw new SchemaException("Materialized view '" + name + "' already exists");
    }

    // Check if a type with this name already exists (backing type would conflict)
    if (schema.existsType(name))
      throw new SchemaException("Cannot create materialized view '" + name +
          "': a type with the same name already exists");

    // Parse the query to validate syntax and extract source types
    final List<String> sourceTypeNames = extractSourceTypes(query);

    // Validate source types exist
    for (final String srcType : sourceTypeNames)
      if (!schema.existsType(srcType))
        throw new SchemaException("Source type '" + srcType + "' referenced in materialized view query does not exist");

    // Classify query complexity
    final boolean simple = MaterializedViewQueryClassifier.isSimple(query, database);

    // Create the backing document type (schema-less)
    final TypeBuilder<?> typeBuilder = schema.buildDocumentType().withName(name);
    if (buckets > 0)
      typeBuilder.withTotalBuckets(buckets);
    if (pageSize > 0)
      typeBuilder.withPageSize(pageSize);
    typeBuilder.create();

    // Create and register the materialized view
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, name, query, name, sourceTypeNames,
        refreshMode, simple, refreshInterval);
    schema.materializedViews.put(name, view);
    schema.saveConfiguration();

    // Perform initial full refresh
    MaterializedViewRefresher.fullRefresh(database, view);
    schema.saveConfiguration();

    return view;
  }

  private List<String> extractSourceTypes(final String sql) {
    // Parse the SQL and extract type names from the FROM clause
    // Use a simple regex-based approach for now: find "FROM <TypeName>"
    final List<String> types = new ArrayList<>();
    final String upper = sql.toUpperCase();
    int fromIdx = upper.indexOf("FROM ");
    while (fromIdx >= 0) {
      final int start = fromIdx + 5;
      // Skip whitespace
      int i = start;
      while (i < sql.length() && sql.charAt(i) == ' ')
        i++;
      // Read the type name (stops at space, comma, WHERE, GROUP, ORDER, etc.)
      final int nameStart = i;
      while (i < sql.length() && Character.isLetterOrDigit(sql.charAt(i)) || (i < sql.length() && sql.charAt(i) == '_'))
        i++;
      if (i > nameStart) {
        final String typeName = sql.substring(nameStart, i);
        if (!typeName.isEmpty() && !types.contains(typeName))
          types.add(typeName);
      }
      fromIdx = upper.indexOf("FROM ", i);
    }
    return types;
  }
```

Add imports to `MaterializedViewBuilder.java`:

```java
import java.util.ArrayList;
import java.util.List;
```

**Step 5: Create `MaterializedViewQueryClassifier` stub**

Create `engine/src/main/java/com/arcadedb/schema/MaterializedViewQueryClassifier.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;

public class MaterializedViewQueryClassifier {

  /**
   * Determines if a defining query is "simple" (eligible for per-record incremental refresh).
   * Simple queries have: single type in FROM, no GROUP BY, no aggregate functions,
   * no subqueries, no TRAVERSE, no JOINs.
   */
  public static boolean isSimple(final String sql, final Database database) {
    final String upper = sql.toUpperCase().trim();

    // Check for aggregate indicators
    if (upper.contains("GROUP BY"))
      return false;
    if (upper.contains("SUM(") || upper.contains("COUNT(") || upper.contains("AVG(") ||
        upper.contains("MIN(") || upper.contains("MAX("))
      return false;
    if (upper.contains("TRAVERSE"))
      return false;

    // Check for JOINs — multiple FROM keywords or JOIN keyword
    if (upper.contains(" JOIN "))
      return false;

    // Check for subqueries — SELECT inside parentheses
    final int firstSelect = upper.indexOf("SELECT");
    if (firstSelect >= 0) {
      final int secondSelect = upper.indexOf("SELECT", firstSelect + 6);
      if (secondSelect >= 0)
        return false;
    }

    // Count FROM clauses (multiple = join-like)
    int fromCount = 0;
    int searchFrom = 0;
    while ((searchFrom = upper.indexOf("FROM ", searchFrom)) >= 0) {
      fromCount++;
      searchFrom += 5;
    }
    if (fromCount > 1)
      return false;

    return true;
  }
}
```

**Step 6: Run tests**

Run: `cd engine && mvn test -Dtest=MaterializedViewTest -pl .`
Expected: All tests PASS

**Step 7: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewRefresher.java
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewQueryClassifier.java
git add engine/src/test/java/com/arcadedb/schema/MaterializedViewTest.java
git commit -m "feat: implement full refresh and MaterializedViewBuilder.create()"
```

---

## Phase 3: SQL Parser Integration

### Task 9: Add ANTLR4 keywords and grammar rules

**Files:**
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4`
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4`

**Step 1: Add new keywords to SQLLexer.g4**

After the last keyword (line 224, `TIMESTAMP`), add:

```antlr
MATERIALIZED: M A T E R I A L I Z E D;
VIEW: V I E W;
REFRESH: R E F R E S H;
EVERY: E V E R Y;
SECOND: S E C O N D;
MINUTE: M I N U T E;
HOUR: H O U R;
MANUAL: M A N U A L;
INCREMENTAL: I N C R E M E N T A L;
```

**Step 2: Add grammar rules to SQLParser.g4**

In the `statement` rule alternatives, after the CREATE TRIGGER line (line 96), add:

```antlr
    | CREATE MATERIALIZED VIEW createMaterializedViewBody   # createMaterializedViewStmt
```

After the DROP TRIGGER line (line 109), add:

```antlr
    | DROP MATERIALIZED VIEW dropMaterializedViewBody       # dropMaterializedViewStmt
```

After the ALTER DATABASE line (line 102), add:

```antlr
    | ALTER MATERIALIZED VIEW alterMaterializedViewBody     # alterMaterializedViewStmt
```

Add a new REFRESH statement (in the statement alternatives, after the ALTER section):

```antlr
    | REFRESH MATERIALIZED VIEW refreshMaterializedViewBody # refreshMaterializedViewStmt
```

Add the body rules (after the trigger body rules, around line 631):

```antlr
// MATERIALIZED VIEW RULES

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

dropMaterializedViewBody
    : (IF EXISTS)? identifier
    ;

refreshMaterializedViewBody
    : identifier
    ;

alterMaterializedViewBody
    : identifier materializedViewRefreshClause
    ;
```

**Step 3: Regenerate the ANTLR4 parser**

Run: `cd engine && mvn generate-sources -pl .`
Expected: BUILD SUCCESS (generates new SQLParser.java, SQLLexer.java, etc.)

**Step 4: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: BUILD SUCCESS (or warnings about unimplemented visitor methods — those are fine, we'll add them next)

**Step 5: Commit**

```
git add engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4
git add engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4
git commit -m "feat: add MATERIALIZED VIEW grammar rules to ANTLR4 SQL parser"
```

---

### Task 10: SQL DDL statement classes

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/CreateMaterializedViewStatement.java`
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/DropMaterializedViewStatement.java`
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/RefreshMaterializedViewStatement.java`
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/AlterMaterializedViewStatement.java`

**Step 1: Create all four statement classes**

Follow the pattern from `CreateTriggerStatement.java` (line 38) and `DropTriggerStatement.java` (line 37).

`CreateMaterializedViewStatement.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedViewRefreshMode;

public class CreateMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public SelectStatement selectStatement;
  public String refreshMode;  // MANUAL, INCREMENTAL, or null (default=MANUAL)
  public int refreshInterval; // for EVERY N SECOND/MINUTE/HOUR
  public String refreshUnit;  // SECOND, MINUTE, HOUR
  public int buckets;
  public boolean ifNotExists = false;

  public CreateMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    final MaterializedViewRefreshMode mode;
    if (refreshMode == null || "MANUAL".equalsIgnoreCase(refreshMode))
      mode = MaterializedViewRefreshMode.MANUAL;
    else if ("INCREMENTAL".equalsIgnoreCase(refreshMode))
      mode = MaterializedViewRefreshMode.INCREMENTAL;
    else
      mode = MaterializedViewRefreshMode.PERIODIC;

    final var builder = database.getSchema().buildMaterializedView()
        .withName(viewName)
        .withQuery(selectStatement.toString())
        .withRefreshMode(mode)
        .withIgnoreIfExists(ifNotExists);

    if (buckets > 0)
      builder.withTotalBuckets(buckets);

    if (mode == MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0) {
      long intervalMs = refreshInterval * 1000L; // default seconds
      if ("MINUTE".equalsIgnoreCase(refreshUnit))
        intervalMs = refreshInterval * 60_000L;
      else if ("HOUR".equalsIgnoreCase(refreshUnit))
        intervalMs = refreshInterval * 3_600_000L;
      builder.withRefreshInterval(intervalMs);
    }

    builder.create();

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "create materialized view");
    r.setProperty("name", viewName);
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CREATE MATERIALIZED VIEW ");
    if (ifNotExists)
      sb.append("IF NOT EXISTS ");
    sb.append(name);
    sb.append(" AS ").append(selectStatement);
    if (refreshMode != null) {
      sb.append(" REFRESH ").append(refreshMode);
      if ("PERIODIC".equalsIgnoreCase(refreshMode) || refreshInterval > 0)
        sb.append(" EVERY ").append(refreshInterval).append(' ').append(refreshUnit);
    }
    if (buckets > 0)
      sb.append(" BUCKETS ").append(buckets);
    return sb.toString();
  }
}
```

`DropMaterializedViewStatement.java`:

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
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    if (!database.getSchema().existsMaterializedView(viewName)) {
      if (ifExists) {
        final InternalResultSet result = new InternalResultSet();
        final ResultInternal r = new ResultInternal();
        r.setProperty("operation", "drop materialized view");
        r.setProperty("name", viewName);
        r.setProperty("dropped", false);
        result.add(r);
        return result;
      }
      throw new CommandExecutionException("Materialized view '" + viewName + "' does not exist");
    }

    database.getSchema().dropMaterializedView(viewName);

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "drop materialized view");
    r.setProperty("name", viewName);
    r.setProperty("dropped", true);
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    return "DROP MATERIALIZED VIEW " + (ifExists ? "IF EXISTS " : "") + name;
  }
}
```

`RefreshMaterializedViewStatement.java`:

```java
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

public class RefreshMaterializedViewStatement extends DDLStatement {
  public Identifier name;

  public RefreshMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    database.getSchema().getMaterializedView(viewName).refresh();

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "refresh materialized view");
    r.setProperty("name", viewName);
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    return "REFRESH MATERIALIZED VIEW " + name;
  }
}
```

`AlterMaterializedViewStatement.java`:

```java
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

public class AlterMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public String refreshMode;
  public int refreshInterval;
  public String refreshUnit;

  public AlterMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    // TODO: implement alter logic — requires recreating the view with new refresh mode
    throw new UnsupportedOperationException("ALTER MATERIALIZED VIEW not yet implemented");
  }

  @Override
  public String toString() {
    return "ALTER MATERIALIZED VIEW " + name + " REFRESH " + refreshMode;
  }
}
```

Add the Apache 2.0 license header to all files (omitted above for brevity — use the same header as `CreateTriggerStatement.java`).

**Step 2: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```
git add engine/src/main/java/com/arcadedb/query/sql/parser/CreateMaterializedViewStatement.java
git add engine/src/main/java/com/arcadedb/query/sql/parser/DropMaterializedViewStatement.java
git add engine/src/main/java/com/arcadedb/query/sql/parser/RefreshMaterializedViewStatement.java
git add engine/src/main/java/com/arcadedb/query/sql/parser/AlterMaterializedViewStatement.java
git commit -m "feat: add SQL DDL statement classes for materialized views"
```

---

### Task 11: ANTLR4 visitor methods in SQLASTBuilder

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java`

**Step 1: Add visitor methods**

After the trigger visitor methods (around line 5712), add:

```java
  // MATERIALIZED VIEW MANAGEMENT

  @Override
  public CreateMaterializedViewStatement visitCreateMaterializedViewStmt(
      final SQLParser.CreateMaterializedViewStmtContext ctx) {
    final CreateMaterializedViewStatement stmt = new CreateMaterializedViewStatement(-1);
    final SQLParser.CreateMaterializedViewBodyContext bodyCtx = ctx.createMaterializedViewBody();

    stmt.ifNotExists = bodyCtx.IF() != null && bodyCtx.NOT() != null && bodyCtx.EXISTS() != null;
    stmt.name = (Identifier) visit(bodyCtx.identifier());
    stmt.selectStatement = (SelectStatement) visit(bodyCtx.selectStatement());

    if (bodyCtx.materializedViewRefreshClause() != null) {
      final SQLParser.MaterializedViewRefreshClauseContext refreshCtx =
          bodyCtx.materializedViewRefreshClause();
      if (refreshCtx.MANUAL() != null)
        stmt.refreshMode = "MANUAL";
      else if (refreshCtx.INCREMENTAL() != null)
        stmt.refreshMode = "INCREMENTAL";
      else if (refreshCtx.EVERY() != null) {
        stmt.refreshMode = "PERIODIC";
        stmt.refreshInterval = Integer.parseInt(refreshCtx.POSITIVE_INTEGER().getText());
        final SQLParser.MaterializedViewTimeUnitContext unitCtx =
            refreshCtx.materializedViewTimeUnit();
        if (unitCtx.SECOND() != null)
          stmt.refreshUnit = "SECOND";
        else if (unitCtx.MINUTE() != null)
          stmt.refreshUnit = "MINUTE";
        else if (unitCtx.HOUR() != null)
          stmt.refreshUnit = "HOUR";
      }
    }

    if (bodyCtx.BUCKETS() != null)
      stmt.buckets = Integer.parseInt(bodyCtx.POSITIVE_INTEGER().getText());

    return stmt;
  }

  @Override
  public DropMaterializedViewStatement visitDropMaterializedViewStmt(
      final SQLParser.DropMaterializedViewStmtContext ctx) {
    final DropMaterializedViewStatement stmt = new DropMaterializedViewStatement(-1);
    final SQLParser.DropMaterializedViewBodyContext bodyCtx = ctx.dropMaterializedViewBody();
    stmt.name = (Identifier) visit(bodyCtx.identifier());
    stmt.ifExists = bodyCtx.IF() != null && bodyCtx.EXISTS() != null;
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
    final SQLParser.AlterMaterializedViewBodyContext bodyCtx = ctx.alterMaterializedViewBody();
    stmt.name = (Identifier) visit(bodyCtx.identifier());

    final SQLParser.MaterializedViewRefreshClauseContext refreshCtx =
        bodyCtx.materializedViewRefreshClause();
    if (refreshCtx.MANUAL() != null)
      stmt.refreshMode = "MANUAL";
    else if (refreshCtx.INCREMENTAL() != null)
      stmt.refreshMode = "INCREMENTAL";
    else if (refreshCtx.EVERY() != null) {
      stmt.refreshMode = "PERIODIC";
      stmt.refreshInterval = Integer.parseInt(refreshCtx.POSITIVE_INTEGER().getText());
      final SQLParser.MaterializedViewTimeUnitContext unitCtx =
          refreshCtx.materializedViewTimeUnit();
      if (unitCtx.SECOND() != null)
        stmt.refreshUnit = "SECOND";
      else if (unitCtx.MINUTE() != null)
        stmt.refreshUnit = "MINUTE";
      else if (unitCtx.HOUR() != null)
        stmt.refreshUnit = "HOUR";
    }

    return stmt;
  }
```

Add the necessary imports at the top of `SQLASTBuilder.java`:

```java
import com.arcadedb.query.sql.parser.CreateMaterializedViewStatement;
import com.arcadedb.query.sql.parser.DropMaterializedViewStatement;
import com.arcadedb.query.sql.parser.RefreshMaterializedViewStatement;
import com.arcadedb.query.sql.parser.AlterMaterializedViewStatement;
```

**Step 2: Compile**

Run: `cd engine && mvn compile -pl . -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```
git add engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java
git commit -m "feat: add ANTLR4 visitor methods for materialized view SQL statements"
```

---

### Task 12: SQL syntax tests

**Files:**
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewSQLTest.java`

**Step 1: Write SQL syntax tests**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * ... (Apache 2.0 license header)
 */
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewSQLTest extends TestHelper {

  @BeforeEach
  public void setupTypes() {
    if (!database.getSchema().existsType("User"))
      database.transaction(() -> {
        database.getSchema().createDocumentType("User");
        database.newDocument("User").set("name", "Alice").set("active", true).save();
        database.newDocument("User").set("name", "Bob").set("active", false).save();
      });
  }

  @Test
  void createViaSql() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW ActiveUsers AS SELECT name FROM User WHERE active = true");

    assertThat(database.getSchema().existsMaterializedView("ActiveUsers")).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT FROM ActiveUsers")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Cleanup
    database.command("sql", "DROP MATERIALIZED VIEW ActiveUsers");
  }

  @Test
  void createWithRefreshMode() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW ActiveUsersInc AS SELECT name FROM User WHERE active = true REFRESH INCREMENTAL");

    final MaterializedView view = database.getSchema().getMaterializedView("ActiveUsersInc");
    assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);

    database.command("sql", "DROP MATERIALIZED VIEW ActiveUsersInc");
  }

  @Test
  void createWithBuckets() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW UserView AS SELECT name FROM User BUCKETS 4");

    assertThat(database.getSchema().existsMaterializedView("UserView")).isTrue();

    database.command("sql", "DROP MATERIALIZED VIEW UserView");
  }

  @Test
  void createIfNotExists() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW TestView AS SELECT FROM User");
    // Should not throw
    database.command("sql",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS TestView AS SELECT FROM User");

    database.command("sql", "DROP MATERIALIZED VIEW TestView");
  }

  @Test
  void refreshViaSql() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW RefreshView AS SELECT name FROM User");

    // Add more data
    database.transaction(() ->
        database.newDocument("User").set("name", "Charlie").set("active", true).save());

    database.command("sql", "REFRESH MATERIALIZED VIEW RefreshView");

    try (final ResultSet rs = database.query("sql", "SELECT FROM RefreshView")) {
      assertThat(rs.stream().count()).isEqualTo(3); // Alice, Bob, Charlie
    }

    database.command("sql", "DROP MATERIALIZED VIEW RefreshView");
  }

  @Test
  void dropViaSql() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW DropMe AS SELECT FROM User");
    assertThat(database.getSchema().existsMaterializedView("DropMe")).isTrue();

    database.command("sql", "DROP MATERIALIZED VIEW DropMe");
    assertThat(database.getSchema().existsMaterializedView("DropMe")).isFalse();
  }

  @Test
  void dropIfExists() {
    // Should not throw
    database.command("sql", "DROP MATERIALIZED VIEW IF EXISTS NonExistent");
  }
}
```

**Step 2: Run tests**

Run: `cd engine && mvn test -Dtest=MaterializedViewSQLTest -pl .`
Expected: All tests PASS

**Step 3: Commit**

```
git add engine/src/test/java/com/arcadedb/schema/MaterializedViewSQLTest.java
git commit -m "test: add SQL syntax tests for materialized views"
```

---

## Phase 4: Incremental Refresh

### Task 13: MaterializedViewChangeListener — test and implementation

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewChangeListener.java`
- Create: `engine/src/test/java/com/arcadedb/schema/MaterializedViewIncrementalTest.java`

**Step 1: Write incremental refresh tests**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * ... (Apache 2.0 license header)
 */
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewIncrementalTest extends TestHelper {

  @Test
  void insertPropagatesAfterCommit() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Employee");
    });

    database.transaction(() -> {
      database.newDocument("Employee").set("name", "Alice").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("EmployeeView")
          .withQuery("SELECT name FROM Employee")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Initial refresh should have 1 record
    try (final ResultSet rs = database.query("sql", "SELECT FROM EmployeeView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Insert in a new transaction — should trigger post-commit refresh
    database.transaction(() -> {
      database.newDocument("Employee").set("name", "Bob").save();
    });

    // View should now have 2 records
    try (final ResultSet rs = database.query("sql", "SELECT FROM EmployeeView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void rollbackDoesNotAffectView() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("RollbackTest");
    });

    database.transaction(() -> {
      database.newDocument("RollbackTest").set("value", 1).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("RollbackView")
          .withQuery("SELECT value FROM RollbackTest")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Insert and rollback
    database.begin();
    database.newDocument("RollbackTest").set("value", 2).save();
    database.rollback();

    // View should still have 1 record
    try (final ResultSet rs = database.query("sql", "SELECT FROM RollbackView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void complexQueryFullRefreshAfterCommit() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sale");
    });

    database.transaction(() -> {
      database.newDocument("Sale").set("product", "A").set("amount", 10).save();
      database.newDocument("Sale").set("product", "A").set("amount", 20).save();
      database.newDocument("Sale").set("product", "B").set("amount", 30).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("SaleSummary")
          .withQuery("SELECT product, sum(amount) as total FROM Sale GROUP BY product")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Initial refresh should have 2 groups
    try (final ResultSet rs = database.query("sql", "SELECT FROM SaleSummary")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }

    // Add more data — complex query triggers full refresh
    database.transaction(() -> {
      database.newDocument("Sale").set("product", "C").set("amount", 50).save();
    });

    // Should have 3 groups now
    try (final ResultSet rs = database.query("sql", "SELECT FROM SaleSummary")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
  }
}
```

**Step 2: Run tests to verify they fail**

Run: `cd engine && mvn test -Dtest=MaterializedViewIncrementalTest -pl . -DfailIfNoTests=false`
Expected: FAIL — no listener registration, no post-commit behavior

**Step 3: Implement MaterializedViewChangeListener**

Create `engine/src/main/java/com/arcadedb/schema/MaterializedViewChangeListener.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * ... (Apache 2.0 license header)
 */
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

public class MaterializedViewChangeListener
    implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final MaterializedViewImpl view;
  private final Database database;
  // Track which transactions already have a callback registered (thread-local)
  private final ThreadLocal<Set<Long>> registeredTxIds = ThreadLocal.withInitial(HashSet::new);

  public MaterializedViewChangeListener(final Database database, final MaterializedViewImpl view) {
    this.database = database;
    this.view = view;
  }

  @Override
  public void onAfterCreate(final Record record) {
    schedulePostCommitRefresh();
  }

  @Override
  public void onAfterUpdate(final Record record) {
    schedulePostCommitRefresh();
  }

  @Override
  public void onAfterDelete(final Record record) {
    schedulePostCommitRefresh();
  }

  private void schedulePostCommitRefresh() {
    final DatabaseInternal db = (DatabaseInternal) database;
    if (!db.isTransactionActive())
      return;

    final TransactionContext tx = db.getTransaction();
    final long txId = Thread.currentThread().threadId();

    // Only register one callback per transaction per view
    if (registeredTxIds.get().contains(txId))
      return;
    registeredTxIds.get().add(txId);

    tx.addAfterCommitCallback(() -> {
      try {
        registeredTxIds.get().remove(txId);
        MaterializedViewRefresher.fullRefresh(database, view);
      } catch (final Exception e) {
        view.setStatus("STALE");
        LogManager.instance().log(this, Level.WARNING,
            "Error in incremental refresh for view '%s', marking as STALE: %s",
            e, view.getName(), e.getMessage());
      }
    });
  }

  public MaterializedViewImpl getView() {
    return view;
  }
}
```

Note: For the initial implementation, both simple and complex queries use full refresh in the post-commit callback. The `_sourceRID` per-record optimization for simple queries can be added as a future enhancement. This keeps the implementation simpler while still achieving the key behavior: batched, post-commit, transaction-safe refresh.

**Step 4: Wire up listener registration in MaterializedViewBuilder.create()**

In `MaterializedViewBuilder.java`, after `schema.saveConfiguration();` in the `create()` method, add listener registration:

```java
    // Register event listeners for INCREMENTAL mode
    if (refreshMode == MaterializedViewRefreshMode.INCREMENTAL)
      registerListeners(schema, view, sourceTypeNames);
```

Add the helper method:

```java
  static void registerListeners(final LocalSchema schema, final MaterializedViewImpl view,
      final List<String> sourceTypeNames) {
    final MaterializedViewChangeListener listener =
        new MaterializedViewChangeListener(schema.getDatabase(), view);
    for (final String srcType : sourceTypeNames) {
      final DocumentType type = schema.getType(srcType);
      type.getEvents().registerListener((AfterRecordCreateListener) listener);
      type.getEvents().registerListener((AfterRecordUpdateListener) listener);
      type.getEvents().registerListener((AfterRecordDeleteListener) listener);
    }
  }
```

Add imports:

```java
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
```

**Step 5: Re-register listeners on schema reload**

In `LocalSchema.java`, in the `readConfiguration()` method, after loading materialized views (the code we added in Task 7), add:

```java
        // Re-register listeners for INCREMENTAL views
        if (view.getRefreshMode() == MaterializedViewRefreshMode.INCREMENTAL)
          MaterializedViewBuilder.registerListeners(this, view, view.getSourceTypeNames());
```

**Step 6: Run tests**

Run: `cd engine && mvn test -Dtest=MaterializedViewIncrementalTest -pl .`
Expected: All tests PASS

**Step 7: Run all materialized view tests together**

Run: `cd engine && mvn test -Dtest="MaterializedView*" -pl .`
Expected: All tests PASS

**Step 8: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewChangeListener.java
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java
git add engine/src/main/java/com/arcadedb/schema/LocalSchema.java
git add engine/src/test/java/com/arcadedb/schema/MaterializedViewIncrementalTest.java
git commit -m "feat: add incremental refresh with post-commit callbacks"
```

---

## Phase 5: Periodic Refresh

### Task 14: MaterializedViewScheduler

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/MaterializedViewScheduler.java`
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java` (close() and readConfiguration())
- Modify: `engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java` (start scheduler on create)

**Step 1: Implement the scheduler**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * ... (Apache 2.0 license header)
 */
package com.arcadedb.schema;

import com.arcadedb.database.Database;
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

  public MaterializedViewScheduler() {
    this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "ArcadeDB-MV-Scheduler");
      t.setDaemon(true);
      return t;
    });
  }

  public void schedule(final Database database, final MaterializedViewImpl view) {
    final long interval = view.getRefreshInterval();
    if (interval <= 0)
      return;

    final WeakReference<Database> dbRef = new WeakReference<>(database);
    final ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
      final Database db = dbRef.get();
      if (db == null || !db.isOpen()) {
        cancel(view.getName());
        return;
      }
      try {
        MaterializedViewRefresher.fullRefresh(db, view);
      } catch (final Exception e) {
        view.setStatus("ERROR");
        LogManager.instance().log(this, Level.SEVERE,
            "Error in periodic refresh for view '%s': %s", e, view.getName(), e.getMessage());
      }
    }, interval, interval, TimeUnit.MILLISECONDS);

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

**Step 2: Add scheduler to LocalSchema**

Add a field to `LocalSchema.java`:

```java
  private MaterializedViewScheduler materializedViewScheduler;
```

In `close()`, before `materializedViews.clear()`:

```java
    if (materializedViewScheduler != null) {
      materializedViewScheduler.shutdown();
      materializedViewScheduler = null;
    }
```

Add a helper method:

```java
  public MaterializedViewScheduler getMaterializedViewScheduler() {
    if (materializedViewScheduler == null)
      materializedViewScheduler = new MaterializedViewScheduler();
    return materializedViewScheduler;
  }
```

In `readConfiguration()`, after the INCREMENTAL listener registration, add for PERIODIC views:

```java
        if (view.getRefreshMode() == MaterializedViewRefreshMode.PERIODIC)
          getMaterializedViewScheduler().schedule(database, view);
```

**Step 3: Start scheduler in MaterializedViewBuilder.create()**

After the listener registration in `create()`, add:

```java
    if (refreshMode == MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0)
      schema.getMaterializedViewScheduler().schedule(database, view);
```

In `dropMaterializedView()` in `LocalSchema.java`, before removing the view:

```java
    // Cancel periodic scheduler if active
    if (materializedViewScheduler != null)
      materializedViewScheduler.cancel(viewName);
```

**Step 4: Compile and run all tests**

Run: `cd engine && mvn test -Dtest="MaterializedView*,TransactionCallbackTest" -pl .`
Expected: All tests PASS

**Step 5: Commit**

```
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewScheduler.java
git add engine/src/main/java/com/arcadedb/schema/LocalSchema.java
git add engine/src/main/java/com/arcadedb/schema/MaterializedViewBuilder.java
git commit -m "feat: add periodic refresh scheduler for materialized views"
```

---

## Phase 6: Integration Testing

### Task 15: Run full test suite and fix issues

**Step 1: Run the full engine test suite to check for regressions**

Run: `cd engine && mvn test -pl .`
Expected: All existing tests PASS. Fix any regressions.

**Step 2: Run the full project build**

Run: `mvn clean install -DskipTests && mvn test -pl engine`
Expected: BUILD SUCCESS + all tests PASS

**Step 3: Final commit if any fixes needed**

```
git add -A
git commit -m "fix: resolve integration issues from materialized views implementation"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 1-2 | Transaction post-commit callbacks |
| 2 | 3-8 | Core schema infrastructure (enum, interface, impl, builder, full refresh) |
| 3 | 9-12 | SQL parser integration (ANTLR4 grammar, DDL statements, visitors, tests) |
| 4 | 13 | Incremental refresh with post-commit callbacks |
| 5 | 14 | Periodic refresh scheduler |
| 6 | 15 | Integration testing |

**Future tasks (not in this plan):**
- `_sourceRID` per-record tracking for simple-query Tier 1 optimization
- Server integration tests (`MaterializedViewServerIT.java`)
- ALTER MATERIALIZED VIEW full implementation
- Studio UI integration
