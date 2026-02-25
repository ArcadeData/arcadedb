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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

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

    view.setStatus(MaterializedViewStatus.STALE);
    assertThat(view.getStatus()).isEqualTo("STALE");

    view.setStatus(MaterializedViewStatus.BUILDING);
    assertThat(view.getStatus()).isEqualTo("BUILDING");

    view.setStatus(MaterializedViewStatus.ERROR);
    assertThat(view.getStatus()).isEqualTo("ERROR");

    view.setStatus(MaterializedViewStatus.VALID);
    view.updateLastRefreshTime();
    assertThat(view.getLastRefreshTime()).isGreaterThan(0);
  }

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
      database.getSchema().createDocumentType("Inventory");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("InventoryView")
          .withQuery("SELECT FROM Inventory")
          .create();
    });

    assertThat(database.getSchema().existsMaterializedView("InventoryView")).isTrue();
    assertThat(database.getSchema().existsType("InventoryView")).isTrue();

    database.getSchema().dropMaterializedView("InventoryView");

    assertThat(database.getSchema().existsMaterializedView("InventoryView")).isFalse();
    assertThat(database.getSchema().existsType("InventoryView")).isFalse();
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
  void copyWithRefreshMode() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "CopyView",
        "SELECT name FROM Foo",
        "CopyView", List.of("Foo"),
        MaterializedViewRefreshMode.MANUAL, true, 0);
    view.setStatus(MaterializedViewStatus.STALE);
    view.setLastRefreshTime(12345L);

    final MaterializedViewImpl copy = view.copyWithRefreshMode(MaterializedViewRefreshMode.PERIODIC, 60_000L);
    assertThat(copy.getName()).isEqualTo("CopyView");
    assertThat(copy.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.PERIODIC);
    assertThat(copy.getRefreshInterval()).isEqualTo(60_000L);
    assertThat(copy.getLastRefreshTime()).isEqualTo(12345L);
    assertThat(copy.getStatus()).isEqualTo("STALE");
  }

  @Test
  void tryBeginRefreshGuardsConcurrentAccess() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "GuardView",
        "SELECT name FROM Foo",
        "GuardView", List.of("Foo"),
        MaterializedViewRefreshMode.MANUAL, true, 0);

    assertThat(view.tryBeginRefresh()).isTrue();
    // Second attempt while locked must fail
    assertThat(view.tryBeginRefresh()).isFalse();
    view.endRefresh();
    // Now the lock is released — next attempt must succeed again
    assertThat(view.tryBeginRefresh()).isTrue();
    view.endRefresh();
  }

  @Test
  void setLastRefreshTimeAndGetRefreshInterval() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "TimingView",
        "SELECT x FROM T",
        "TimingView", List.of("T"),
        MaterializedViewRefreshMode.PERIODIC, false, 30_000L);

    assertThat(view.getRefreshInterval()).isEqualTo(30_000L);
    assertThat(view.getLastRefreshTime()).isEqualTo(0L);

    view.setLastRefreshTime(99999L);
    assertThat(view.getLastRefreshTime()).isEqualTo(99999L);
  }

  @Test
  void getChangeListenerAndSetChangeListener() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "ListenerView",
        "SELECT x FROM T",
        "ListenerView", List.of("T"),
        MaterializedViewRefreshMode.INCREMENTAL, true, 0);

    assertThat(view.getChangeListener()).isNull();

    final MaterializedViewChangeListener listener = new MaterializedViewChangeListener(
        (DatabaseInternal) database, view);
    view.setChangeListener(listener);
    assertThat(view.getChangeListener()).isSameAs(listener);
    assertThat(view.getChangeListener().getView()).isSameAs(view);

    view.setChangeListener(null);
    assertThat(view.getChangeListener()).isNull();
  }

  @Test
  void getBackingTypeReturnsSchemaType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("BackingSource");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("BackingView")
          .withQuery("SELECT FROM BackingSource")
          .create();
    });

    final MaterializedView view = database.getSchema().getMaterializedView("BackingView");
    assertThat(view.getBackingType()).isNotNull();
    assertThat(view.getBackingType().getName()).isEqualTo("BackingView");
  }

  @Test
  void concurrentRefreshSkipped() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("ConcSource");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ConcView")
          .withQuery("SELECT FROM ConcSource")
          .create();
    });

    final MaterializedViewImpl view = (MaterializedViewImpl)
        database.getSchema().getMaterializedView("ConcView");

    // Simulate an in-progress refresh by locking the flag
    assertThat(view.tryBeginRefresh()).isTrue();
    try {
      // fullRefresh should detect the lock and return without changing state
      assertThatCode(() -> MaterializedViewRefresher.fullRefresh(database, view))
          .doesNotThrowAnyException();
      // Status should still be VALID (skipped, not reset to BUILDING)
      assertThat(view.getStatus()).isEqualTo("VALID");
    } finally {
      view.endRefresh();
    }
  }

  @Test
  void builderWithPageSize() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("PagedSource");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("PagedView")
          .withQuery("SELECT FROM PagedSource")
          .withTotalBuckets(2)
          .withPageSize(65536)
          .create();
    });

    assertThat(database.getSchema().existsMaterializedView("PagedView")).isTrue();
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
}
