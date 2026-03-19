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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewEdgeCaseTest extends TestHelper {

  @Test
  void multipleViewsOnSameSource() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sensor");
      database.getSchema().getType("Sensor").createProperty("name", Type.STRING);
      database.getSchema().getType("Sensor").createProperty("value", Type.INTEGER);
      database.getSchema().getType("Sensor").createProperty("active", Type.BOOLEAN);
    });

    database.transaction(() -> {
      database.newDocument("Sensor").set("name", "temp1").set("value", 25).set("active", true).save();
      database.newDocument("Sensor").set("name", "temp2").set("value", 30).set("active", false).save();
      database.newDocument("Sensor").set("name", "temp3").set("value", 15).set("active", true).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("SensorAll")
          .withQuery("SELECT name, value FROM Sensor")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("SensorActive")
          .withQuery("SELECT name, value FROM Sensor WHERE active = true")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM SensorAll")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }

    try (final ResultSet rs = database.query("sql", "SELECT FROM SensorActive")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }

    final MaterializedView[] views = database.getSchema().getMaterializedViews();
    assertThat(views).hasSize(2);
  }

  @Test
  void cannotCreateViewWithExistingTypeName() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Conflict");
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("Conflict")
          .withQuery("SELECT FROM Conflict")
          .create();
    })).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Conflict");
  }

  @Test
  void cannotCreateViewWithDuplicateName() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ProductView")
          .withQuery("SELECT FROM Product")
          .create();
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ProductView")
          .withQuery("SELECT FROM Product")
          .create();
    })).isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void viewWithEmptyResult() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("EmptySource");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("EmptySourceView")
          .withQuery("SELECT FROM EmptySource")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM EmptySourceView")) {
      assertThat(rs.stream().count()).isEqualTo(0);
    }

    final MaterializedView view = database.getSchema().getMaterializedView("EmptySourceView");
    assertThat(view.getStatus()).isEqualTo("VALID");
  }

  @Test
  void viewSurvivesReopenWithMultipleViews() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Inventory");
      database.getSchema().getType("Inventory").createProperty("sku", Type.STRING);
      database.getSchema().getType("Inventory").createProperty("qty", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("Inventory").set("sku", "A1").set("qty", 10).save();
      database.newDocument("Inventory").set("sku", "B2").set("qty", 20).save();
      database.newDocument("Inventory").set("sku", "C3").set("qty", 5).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("InvAll")
          .withQuery("SELECT sku, qty FROM Inventory")
          .create();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("InvLowStock")
          .withQuery("SELECT sku, qty FROM Inventory WHERE qty < 15")
          .create();
    });

    // Close and reopen database
    database.close();
    database = factory.open();

    assertThat(database.getSchema().existsMaterializedView("InvAll")).isTrue();
    assertThat(database.getSchema().existsMaterializedView("InvLowStock")).isTrue();
    assertThat(database.getSchema().getMaterializedViews()).hasSize(2);

    try (final ResultSet rs = database.query("sql", "SELECT FROM InvAll")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }

    try (final ResultSet rs = database.query("sql", "SELECT FROM InvLowStock")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void largeDatasetRefresh() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Measurement");
      database.getSchema().getType("Measurement").createProperty("sensorId", Type.INTEGER);
      database.getSchema().getType("Measurement").createProperty("even", Type.BOOLEAN);
    });

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++)
        database.newDocument("Measurement")
            .set("sensorId", i)
            .set("even", i % 2 == 0)
            .save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("EvenMeasurements")
          .withQuery("SELECT sensorId FROM Measurement WHERE even = true")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM EvenMeasurements")) {
      assertThat(rs.stream().count()).isEqualTo(500);
    }
  }

  @Test
  void incrementalMultipleInsertsInSameTransaction() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("LogEntry");
      database.getSchema().getType("LogEntry").createProperty("message", Type.STRING);
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("LogEntryView")
          .withQuery("SELECT message FROM LogEntry")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Insert 3 records in one transaction
    database.transaction(() -> {
      database.newDocument("LogEntry").set("message", "first").save();
      database.newDocument("LogEntry").set("message", "second").save();
      database.newDocument("LogEntry").set("message", "third").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM LogEntryView")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
  }

  @Test
  void periodicRefreshUpdatesView() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("PeriodicSrc");
      database.getSchema().getType("PeriodicSrc").createProperty("val", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("PeriodicSrc").set("val", 1).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("PeriodicView")
          .withQuery("SELECT val FROM PeriodicSrc")
          .withRefreshMode(MaterializedViewRefreshMode.PERIODIC)
          .withRefreshInterval(200)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM PeriodicView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Add data after view creation
    database.transaction(() -> {
      database.newDocument("PeriodicSrc").set("val", 2).save();
    });

    // Wait for the scheduler to trigger a refresh (interval = 200ms, wait 1s)
    Thread.sleep(1_000);

    try (final ResultSet rs = database.query("sql", "SELECT FROM PeriodicView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void queryClassifierDetectsComplexQueries() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Transaction");
      database.getSchema().getType("Transaction").createProperty("category", Type.STRING);
      database.getSchema().getType("Transaction").createProperty("amount", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("Transaction").set("category", "food").set("amount", 50).save();
    });

    // Complex query with GROUP BY
    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("TransactionSummary")
          .withQuery("SELECT category, sum(amount) as total FROM Transaction GROUP BY category")
          .create();
    });

    final MaterializedView complexView = database.getSchema().getMaterializedView("TransactionSummary");
    assertThat(complexView.isSimpleQuery()).isFalse();

    // Simple query with WHERE
    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("TransactionFiltered")
          .withQuery("SELECT category, amount FROM Transaction WHERE amount > 10")
          .create();
    });

    final MaterializedView simpleView = database.getSchema().getMaterializedView("TransactionFiltered");
    assertThat(simpleView.isSimpleQuery()).isTrue();
  }

  @Test
  void cannotDropSourceTypeWhileViewExists() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Order");
      database.getSchema().getType("Order").createProperty("amount", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("Order").set("amount", 100).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("OrderSummary")
          .withQuery("SELECT amount FROM Order")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    // Dropping the source type must be blocked while the materialized view exists
    assertThatThrownBy(() -> database.transaction(() ->
        database.getSchema().dropType("Order")
    )).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Order")
        .hasMessageContaining("OrderSummary");

    // After dropping the view, the source type can be dropped
    database.transaction(() -> database.getSchema().dropMaterializedView("OrderSummary"));
    database.transaction(() -> database.getSchema().dropType("Order"));
    assertThat(database.getSchema().existsType("Order")).isFalse();
  }

  @Test
  void queryClassifierHandlesAllComplexBranches() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("ClassifierTest");
    });

    // GROUP BY → complex
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT category, count(*) FROM ClassifierTest GROUP BY category", database)).isFalse();

    // Aggregate function in projection → complex
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT sum(amount) FROM ClassifierTest", database)).isFalse();
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT count(*) FROM ClassifierTest", database)).isFalse();
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT avg(score) FROM ClassifierTest", database)).isFalse();
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT min(val) FROM ClassifierTest", database)).isFalse();
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT max(val) FROM ClassifierTest", database)).isFalse();

    // Non-SELECT statement → complex
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "UPDATE ClassifierTest SET x = 1", database)).isFalse();
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "INSERT INTO ClassifierTest SET x = 1", database)).isFalse();

    // Simple WHERE → simple
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT name FROM ClassifierTest WHERE active = true", database)).isTrue();

    // No WHERE → simple
    assertThat(MaterializedViewQueryClassifier.isSimple(
        "SELECT name FROM ClassifierTest", database)).isTrue();
  }

  @Test
  void schemaMetadataQuery() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Report");
      database.getSchema().getType("Report").createProperty("title", Type.STRING);
    });

    database.transaction(() -> {
      database.newDocument("Report").set("title", "Q1").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ReportView")
          .withQuery("SELECT title FROM Report")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:materializedViews")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((String) result.getProperty("name")).isEqualTo("ReportView");
      assertThat((String) result.getProperty("query")).isEqualTo("SELECT title FROM Report");
      assertThat((String) result.getProperty("status")).isEqualTo("VALID");
      assertThat((String) result.getProperty("refreshMode")).isEqualTo("MANUAL");
      assertThat((String) result.getProperty("backingType")).isEqualTo("ReportView");
    }
  }
}
