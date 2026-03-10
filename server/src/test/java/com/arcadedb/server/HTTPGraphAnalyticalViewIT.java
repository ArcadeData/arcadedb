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

class HTTPGraphAnalyticalViewIT extends BaseGraphServerTest {

  @Test
  void createAndDropGraphAnalyticalViewViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE City");
      command(serverIndex, "CREATE EDGE TYPE ROAD");
      command(serverIndex, "CREATE VERTEX City SET name = 'Rome'");
      command(serverIndex, "CREATE VERTEX City SET name = 'Milan'");

      // Create GAV
      final String createResponse = command(serverIndex,
          "CREATE GRAPH ANALYTICAL VIEW cityRoads VERTEX TYPES (City) EDGE TYPES (ROAD)");
      assertThat(createResponse).contains("\"created\":true");

      // Verify via schema:graphAnalyticalViews
      final JSONObject queryResult = executeCommand(serverIndex, "sql", "SELECT FROM schema:graphAnalyticalViews");
      assertThat(queryResult).isNotNull();
      final JSONArray records = queryResult.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isGreaterThanOrEqualTo(1);

      boolean found = false;
      for (int i = 0; i < records.length(); i++) {
        final JSONObject record = records.getJSONObject(i);
        if ("cityRoads".equals(record.getString("name"))) {
          found = true;
          break;
        }
      }
      assertThat(found).isTrue();

      // Drop
      final String dropResponse = command(serverIndex, "DROP GRAPH ANALYTICAL VIEW cityRoads");
      assertThat(dropResponse).contains("\"dropped\":true");

      // Verify gone
      final JSONObject afterDrop = executeCommand(serverIndex, "sql",
          "SELECT FROM schema:graphAnalyticalViews WHERE name = 'cityRoads'");
      assertThat(afterDrop).isNotNull();
      final JSONArray afterDropRecords = afterDrop.getJSONObject("result").getJSONArray("records");
      assertThat(afterDropRecords.length()).isEqualTo(0);

      // Cleanup
      command(serverIndex, "DROP TYPE ROAD UNSAFE");
      command(serverIndex, "DROP TYPE City UNSAFE");
    });
  }

  @Test
  void createGraphAnalyticalViewWithAutoUpdateViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE Sensor");
      command(serverIndex, "CREATE EDGE TYPE READS");

      final String response = command(serverIndex,
          "CREATE GRAPH ANALYTICAL VIEW sensorNet VERTEX TYPES (Sensor) EDGE TYPES (READS) AUTO UPDATE");
      assertThat(response).contains("\"created\":true");

      // Verify auto-update flag via schema query
      final JSONObject result = executeCommand(serverIndex, "sql",
          "SELECT FROM schema:graphAnalyticalViews WHERE name = 'sensorNet'");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);
      assertThat(records.getJSONObject(0).getBoolean("autoUpdate")).isTrue();

      // Cleanup
      command(serverIndex, "DROP GRAPH ANALYTICAL VIEW sensorNet");
      command(serverIndex, "DROP TYPE READS UNSAFE");
      command(serverIndex, "DROP TYPE Sensor UNSAFE");
    });
  }

  @Test
  void createIfNotExistsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE Node");
      command(serverIndex, "CREATE EDGE TYPE LINK");

      command(serverIndex, "CREATE GRAPH ANALYTICAL VIEW myView VERTEX TYPES (Node) EDGE TYPES (LINK)");
      // Should not throw
      final String response = command(serverIndex,
          "CREATE GRAPH ANALYTICAL VIEW IF NOT EXISTS myView VERTEX TYPES (Node) EDGE TYPES (LINK)");
      assertThat(response).contains("\"created\":false");

      // Cleanup
      command(serverIndex, "DROP GRAPH ANALYTICAL VIEW myView");
      command(serverIndex, "DROP TYPE LINK UNSAFE");
      command(serverIndex, "DROP TYPE Node UNSAFE");
    });
  }

  @Test
  void dropIfExistsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      // Should not throw for non-existent view
      final String response = command(serverIndex, "DROP GRAPH ANALYTICAL VIEW IF EXISTS nonExistentGav");
      assertThat(response).contains("\"dropped\":false");
    });
  }

  @Test
  void createWithPropertiesViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE Device");
      command(serverIndex, "CREATE EDGE TYPE CONNECTS");

      command(serverIndex,
          "CREATE GRAPH ANALYTICAL VIEW deviceNet VERTEX TYPES (Device) EDGE TYPES (CONNECTS) PROPERTIES (name, status)");

      final JSONObject result = executeCommand(serverIndex, "sql",
          "SELECT FROM schema:graphAnalyticalViews WHERE name = 'deviceNet'");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);

      final JSONObject viewDef = records.getJSONObject(0);
      assertThat(viewDef.has("propertyFilter")).isTrue();
      final JSONArray props = viewDef.getJSONArray("propertyFilter");
      assertThat(props.length()).isEqualTo(2);

      // Cleanup
      command(serverIndex, "DROP GRAPH ANALYTICAL VIEW deviceNet");
      command(serverIndex, "DROP TYPE CONNECTS UNSAFE");
      command(serverIndex, "DROP TYPE Device UNSAFE");
    });
  }

  @Test
  void querySchemaGraphAnalyticalViewsViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE Employee");
      command(serverIndex, "CREATE EDGE TYPE MANAGES");

      command(serverIndex,
          "CREATE GRAPH ANALYTICAL VIEW socialGraph VERTEX TYPES (Employee) EDGE TYPES (MANAGES) AUTO UPDATE");

      final JSONObject result = executeCommand(serverIndex, "sql",
          "SELECT FROM schema:graphAnalyticalViews WHERE name = 'socialGraph'");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);

      final JSONObject viewMeta = records.getJSONObject(0);
      assertThat(viewMeta.getString("name")).isEqualTo("socialGraph");
      assertThat(viewMeta.getBoolean("autoUpdate")).isTrue();
      assertThat(viewMeta.has("vertexTypes")).isTrue();
      assertThat(viewMeta.has("edgeTypes")).isTrue();

      // Cleanup
      command(serverIndex, "DROP GRAPH ANALYTICAL VIEW socialGraph");
      command(serverIndex, "DROP TYPE MANAGES UNSAFE");
      command(serverIndex, "DROP TYPE Employee UNSAFE");
    });
  }
}
