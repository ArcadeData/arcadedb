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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
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
