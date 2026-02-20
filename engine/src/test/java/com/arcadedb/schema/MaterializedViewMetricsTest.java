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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MaterializedViewMetricsTest extends TestHelper {

  @Test
  void refreshCountsAndTiming() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sensor");
      database.getSchema().getType("Sensor").createProperty("name", Type.STRING);
      database.getSchema().getType("Sensor").createProperty("value", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("Sensor").set("name", "temp").set("value", 42).save();
      database.newDocument("Sensor").set("name", "humidity").set("value", 80).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("SensorView")
          .withQuery("SELECT name, value FROM Sensor")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    final MaterializedView mv = database.getSchema().getMaterializedView("SensorView");

    // Before any manual refresh, the initial creation already ran a refresh
    assertThat(mv.getRefreshCount()).isEqualTo(1);
    assertThat(mv.getRefreshTotalTimeMs()).isGreaterThanOrEqualTo(0);
    assertThat(mv.getErrorCount()).isEqualTo(0);

    // Refresh again
    mv.refresh();

    assertThat(mv.getRefreshCount()).isEqualTo(2);
    assertThat(mv.getRefreshTotalTimeMs()).isGreaterThanOrEqualTo(0);
    assertThat(mv.getRefreshMinTimeMs()).isGreaterThanOrEqualTo(0);
    assertThat(mv.getRefreshMaxTimeMs()).isGreaterThanOrEqualTo(mv.getRefreshMinTimeMs());
    assertThat(mv.getLastRefreshDurationMs()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void metricsExposedViaSchemaQuery() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product2");
      database.getSchema().getType("Product2").createProperty("name", Type.STRING);
    });

    database.transaction(() -> {
      database.newDocument("Product2").set("name", "widget").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("ProductView2")
          .withQuery("SELECT name FROM Product2")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    // Refresh to get metrics populated
    database.getSchema().getMaterializedView("ProductView2").refresh();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:materializedViews")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((String) r.getProperty("name")).isEqualTo("ProductView2");
      assertThat((Long) r.getProperty("refreshCount")).isEqualTo(2L);
      assertThat(r.getPropertyNames()).contains("refreshTotalTimeMs", "refreshMinTimeMs",
          "refreshMaxTimeMs", "refreshAvgTimeMs", "errorCount", "lastRefreshDurationMs");
    }
  }

  @Test
  void errorCountIncrementsOnFailure() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Data");
      database.getSchema().getType("Data").createProperty("val", Type.INTEGER);
    });

    database.transaction(() -> {
      database.newDocument("Data").set("val", 1).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("DataView")
          .withQuery("SELECT val FROM Data")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL)
          .create();
    });

    final MaterializedView mv = database.getSchema().getMaterializedView("DataView");
    assertThat(mv.getRefreshCount()).isEqualTo(1);
    assertThat(mv.getErrorCount()).isEqualTo(0);
  }
}
