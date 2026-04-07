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
package com.arcadedb.e2e;

import com.arcadedb.serializer.json.JSONObject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests dynamic database creation in an HA cluster. A new database is created at
 * runtime on the leader via the server command API. The test verifies that the
 * database, its schema, and its data are correctly replicated to all followers.
 *
 * <p>No network partitions are involved - this is a straightforward replication test
 * for dynamically created databases.
 *
 * <p>Requires Docker. Run with: {@code mvn test -pl e2e -Dtest=HADynamicDatabaseE2ETest}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("e2e-ha")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
public class HADynamicDatabaseE2ETest extends ArcadeHAContainerTemplate {

  private static final String DYNAMIC_DB = "dynamicdb";

  @BeforeEach
  void setUp() {
    startCluster(3);
  }

  @AfterEach
  void tearDown() {
    stopCluster();
  }

  @Test
  void testDynamicDatabaseReplication() throws Exception {
    // 1. Verify the default database (testdb) is working
    final GenericContainer<?> leader = findLeader();
    assertThat(leader).isNotNull();

    httpCommand(leader, "SQL", "CREATE VERTEX TYPE Baseline IF NOT EXISTS");
    httpCommand(leader, "SQL", "INSERT INTO Baseline CONTENT {\"name\":\"check\",\"value\":1}");

    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCount(c, "Baseline")).isEqualTo(1);
    });

    // 2. Create a new database dynamically on the leader
    httpServerCommand(leader, "create database " + DYNAMIC_DB);

    // Wait for the database to become available on all nodes
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
      for (final GenericContainer<?> c : containers) {
        try {
          httpCommandOnDb(c, DYNAMIC_DB, "SQL", "SELECT 1");
        } catch (final Exception e) {
          return false;
        }
      }
      return true;
    });

    // 3. Create schema in the dynamic database
    httpCommandOnDb(leader, DYNAMIC_DB, "SQL", "CREATE VERTEX TYPE Product IF NOT EXISTS");
    httpCommandOnDb(leader, DYNAMIC_DB, "SQL", "CREATE PROPERTY Product.sku STRING");
    httpCommandOnDb(leader, DYNAMIC_DB, "SQL", "CREATE PROPERTY Product.name STRING");
    httpCommandOnDb(leader, DYNAMIC_DB, "SQL", "CREATE PROPERTY Product.price DOUBLE");
    httpCommandOnDb(leader, DYNAMIC_DB, "SQL", "CREATE INDEX ON Product (sku) UNIQUE");

    // 4. Write data to the dynamic database
    for (int i = 0; i < 30; i++)
      httpCommandOnDb(leader, DYNAMIC_DB, "SQL",
          "INSERT INTO Product CONTENT {\"sku\":\"SKU-" + String.format("%04d", i) + "\",\"name\":\"Product " + i + "\",\"price\":" + (i * 9.99) + "}");

    // 5. Verify schema and data replicate to all followers
    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCountOnDb(c, DYNAMIC_DB, "Product")).isEqualTo(30);
    });

    // 6. Verify schema replicated correctly by querying with the indexed property
    for (final GenericContainer<?> c : containers) {
      final JSONObject result = httpCommandOnDb(c, DYNAMIC_DB, "SQL", "SELECT FROM Product WHERE sku = 'SKU-0010'");
      assertThat(result.getJSONArray("result").length()).as("Index-based lookup should find 1 record on each node").isEqualTo(1);
    }

    // 7. Write more data and verify continued replication
    for (int i = 30; i < 50; i++)
      httpCommandOnDb(leader, DYNAMIC_DB, "SQL",
          "INSERT INTO Product CONTENT {\"sku\":\"SKU-" + String.format("%04d", i) + "\",\"name\":\"Product " + i + "\",\"price\":" + (i * 9.99) + "}");

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
      for (final GenericContainer<?> c : containers)
        assertThat(httpCountOnDb(c, DYNAMIC_DB, "Product")).isEqualTo(50);
    });

    // 8. Verify the default database was not affected
    for (final GenericContainer<?> c : containers)
      assertThat(httpCount(c, "Baseline")).isEqualTo(1);
  }

  /**
   * Executes a server-level command (e.g., create database) via HTTP POST to /api/v1/server.
   */
  private JSONObject httpServerCommand(final GenericContainer<?> container, final String command) throws Exception {
    final String url = "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT) + "/api/v1/server";

    final JSONObject body = new JSONObject();
    body.put("command", command);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200)
      throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());

    return new JSONObject(response.body());
  }

  /**
   * Executes a SQL command on a specific database via direct HTTP POST.
   */
  private JSONObject httpCommandOnDb(final GenericContainer<?> container, final String dbName,
      final String language, final String command) throws Exception {
    final String url = "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT)
        + "/api/v1/command/" + dbName;

    final JSONObject body = new JSONObject();
    body.put("language", language);
    body.put("command", command);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200)
      throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());

    return new JSONObject(response.body());
  }

  /**
   * Counts records of a given type on a specific database and container.
   * Returns -1 if the type or database doesn't exist yet.
   */
  private long httpCountOnDb(final GenericContainer<?> container, final String dbName, final String typeName) {
    try {
      final JSONObject result = httpCommandOnDb(container, dbName, "SQL", "SELECT count(*) as cnt FROM " + typeName);
      return result.getJSONArray("result").getJSONObject(0).getLong("cnt");
    } catch (final Exception e) {
      return -1;
    }
  }
}
