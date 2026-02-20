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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HTTPMaterializedViewIT extends BaseGraphServerTest {

  // Test 1: Create a view, query it, verify results
  @Test
  void createViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Sensor");
      command(serverIndex, "INSERT INTO Sensor SET name = 'temp1', value = 22.5, active = true");
      command(serverIndex, "INSERT INTO Sensor SET name = 'temp2', value = 25.0, active = false");
      command(serverIndex, "INSERT INTO Sensor SET name = 'hum1', value = 60.0, active = true");

      command(serverIndex,
          "CREATE MATERIALIZED VIEW ActiveSensors AS SELECT name, value FROM Sensor WHERE active = true");

      final JSONObject queryResult = executeCommand(serverIndex, "sql", "SELECT FROM ActiveSensors");
      assertThat(queryResult).isNotNull();
      final JSONArray records = queryResult.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(2);

      // Cleanup
      command(serverIndex, "DROP MATERIALIZED VIEW ActiveSensors");
      command(serverIndex, "DROP TYPE Sensor UNSAFE");
    });
  }

  // Test 2: Refresh a view via HTTP after adding data
  @Test
  void refreshViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Inventory");
      command(serverIndex, "INSERT INTO Inventory SET label = 'A'");

      command(serverIndex, "CREATE MATERIALIZED VIEW InventoryView AS SELECT label FROM Inventory");

      command(serverIndex, "INSERT INTO Inventory SET label = 'B'");
      command(serverIndex, "REFRESH MATERIALIZED VIEW InventoryView");

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM InventoryView");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(2);

      command(serverIndex, "DROP MATERIALIZED VIEW InventoryView");
      command(serverIndex, "DROP TYPE Inventory UNSAFE");
    });
  }

  // Test 3: Drop a view via HTTP
  @Test
  void dropViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE LogEntry");
      command(serverIndex, "INSERT INTO LogEntry SET msg = 'hello'");
      command(serverIndex, "CREATE MATERIALIZED VIEW LogView AS SELECT FROM LogEntry");

      command(serverIndex, "DROP MATERIALIZED VIEW LogView");

      // Verify the backing type is gone too
      final JSONObject result = executeCommand(serverIndex, "sql",
          "SELECT FROM schema:types WHERE name = 'LogView'");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(0);

      command(serverIndex, "DROP TYPE LogEntry UNSAFE");
    });
  }

  // Test 4: DROP IF EXISTS doesn't throw
  @Test
  void dropIfExistsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "DROP MATERIALIZED VIEW IF EXISTS NonExistentView");
    });
  }

  // Test 5: CREATE IF NOT EXISTS is idempotent
  @Test
  void createIfNotExistsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE DataRecord");
      command(serverIndex, "INSERT INTO DataRecord SET v = 1");

      command(serverIndex, "CREATE MATERIALIZED VIEW DataRecordView AS SELECT FROM DataRecord");
      // Should not throw
      command(serverIndex, "CREATE MATERIALIZED VIEW IF NOT EXISTS DataRecordView AS SELECT FROM DataRecord");

      command(serverIndex, "DROP MATERIALIZED VIEW DataRecordView");
      command(serverIndex, "DROP TYPE DataRecord UNSAFE");
    });
  }

  // Test 6: Create with REFRESH INCREMENTAL mode
  @Test
  void createWithRefreshModeViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Event");
      command(serverIndex, "INSERT INTO Event SET type = 'click'");

      command(serverIndex,
          "CREATE MATERIALIZED VIEW ClickEvents AS SELECT type FROM Event WHERE type = 'click' REFRESH INCREMENTAL");

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM ClickEvents");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);

      command(serverIndex, "DROP MATERIALIZED VIEW ClickEvents");
      command(serverIndex, "DROP TYPE Event UNSAFE");
    });
  }

  // Test 7: Query schema:materializedViews via HTTP
  @Test
  void querySchemaMetadataViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE Metric");
      command(serverIndex, "INSERT INTO Metric SET name = 'cpu', value = 80");

      command(serverIndex,
          "CREATE MATERIALIZED VIEW MetricView AS SELECT name FROM Metric");

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM schema:materializedViews");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isGreaterThanOrEqualTo(1);

      // Check the view metadata
      final JSONObject viewMeta = records.getJSONObject(0);
      assertThat(viewMeta.getString("name")).isEqualTo("MetricView");
      assertThat(viewMeta.getString("status")).isEqualTo("VALID");

      command(serverIndex, "DROP MATERIALIZED VIEW MetricView");
      command(serverIndex, "DROP TYPE Metric UNSAFE");
    });
  }
}
