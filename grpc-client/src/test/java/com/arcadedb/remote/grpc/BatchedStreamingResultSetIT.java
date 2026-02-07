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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for BatchedStreamingResultSet.
 * Tests batch-aware result set functionality through RemoteGrpcDatabase queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BatchedStreamingResultSetIT extends BaseGraphServerTest {

  private RemoteGrpcServer server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(server, "localhost", 50051, 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);

    // Create test data
    database.command("sql", "CREATE VERTEX TYPE BatchVertex");
    for (int i = 0; i < 100; i++) {
      database.command("sql", "CREATE VERTEX BatchVertex SET value = " + i + ", name = 'Vertex " + i + "'");
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    if (database != null) {
      database.close();
    }
    if (server != null) {
      server.close();
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void shouldIterateAllResults() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex ORDER BY value");

    int count = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      assertThat(result).isNotNull();
      assertThat(result.<Integer>getProperty("value")).isEqualTo(count);
      count++;
    }

    assertThat(count).isEqualTo(100);
  }

  @Test
  void shouldHandleSmallResultSet() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex WHERE value < 5 ORDER BY value");

    int count = 0;
    while (resultSet.hasNext()) {
      resultSet.next();
      count++;
    }

    assertThat(count).isEqualTo(5);
  }

  @Test
  void shouldHandleLargeResultSet() {
    // Create more data
    for (int i = 100; i < 500; i++) {
      database.command("sql", "CREATE VERTEX BatchVertex SET value = " + i);
    }

    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex");

    int count = 0;
    while (resultSet.hasNext()) {
      resultSet.next();
      count++;
    }

    assertThat(count).isEqualTo(500);
  }

  @Test
  void shouldHandleEmptyResultSet() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex WHERE value > 10000");

    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shouldStreamResults() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex WHERE value < 20 ORDER BY value");

    final List<Result> results = resultSet.stream().toList();

    assertThat(results).hasSize(20);
    for (int i = 0; i < 20; i++) {
      assertThat(results.get(i).<Integer>getProperty("value")).isEqualTo(i);
    }
  }

  @Test
  void shouldHandleComplexQuery() {
    final ResultSet resultSet = database.query("sql",
        "SELECT value, value * 2 AS doubled FROM BatchVertex WHERE value < 10 ORDER BY value");

    int expectedValue = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final int value = result.getProperty("value");
      final int doubled = result.getProperty("doubled");
      assertThat(value).isEqualTo(expectedValue);
      assertThat(doubled).isEqualTo(expectedValue * 2);
      expectedValue++;
    }

    assertThat(expectedValue).isEqualTo(10);
  }

  @Test
  void shouldHandleProjections() {
    final ResultSet resultSet = database.query("sql", "SELECT value AS val, name FROM BatchVertex WHERE value < 5");

    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      assertThat(result.getPropertyNames()).contains("val", "name");
      assertThat(result.<Integer>getProperty("val")).isLessThan(5);
    }
  }

  @Test
  void shouldHandleAggregations() {
    final ResultSet resultSet = database.query("sql", "SELECT count(*) AS total, sum(value) AS sum FROM BatchVertex");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Number total = result.getProperty("total");
    assertThat(total.longValue()).isEqualTo(100L);
    final Number sum = result.getProperty("sum");
    assertThat(sum.longValue()).isGreaterThan(0L);
  }

  @Test
  void shouldHandleGroupBy() {
    // Create grouped data
    database.command("sql", "CREATE VERTEX TYPE GroupedBatchVertex");
    for (int i = 0; i < 20; i++) {
      database.command("sql", "CREATE VERTEX GroupedBatchVertex SET category = " + (i % 5) + ", value = " + i);
    }

    final ResultSet resultSet = database.query("sql",
        "SELECT category, count(*) AS cnt FROM GroupedBatchVertex GROUP BY category ORDER BY category");

    int groups = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final Number cnt = result.getProperty("cnt");
      assertThat(cnt.longValue()).isEqualTo(4L);
      groups++;
    }

    assertThat(groups).isEqualTo(5);
  }

  @Test
  void shouldHandleMultipleIterations() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex WHERE value < 10");

    // First iteration
    int count1 = 0;
    while (resultSet.hasNext()) {
      resultSet.next();
      count1++;
    }

    assertThat(count1).isEqualTo(10);

    // Second hasNext should return false
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shouldHandleResultSetClose() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex");

    // Iterate partially
    for (int i = 0; i < 10 && resultSet.hasNext(); i++) {
      resultSet.next();
    }

    // Close result set
    resultSet.close();

    // Should be able to close multiple times
    resultSet.close();
  }

  @Test
  void shouldHandleMetadata() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex WHERE value < 5");

    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      // Metadata may or may not be present depending on query type
      if (result.getElement().isPresent()) {
        assertThat(result.getElement().get().getTypeName()).isEqualTo("BatchVertex");
      }
    }
  }

  @Test
  void shouldHandleNullValues() {
    database.command("sql", "CREATE VERTEX BatchVertex SET value = null, name = 'null value test'");

    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex WHERE name = 'null value test'");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Object) result.getProperty("value")).isNull();
    assertThat(result.<String>getProperty("name")).isEqualTo("null value test");
  }

  @Test
  void shouldHandleStreamProcessing() {
    final long count = database.query("sql", "SELECT FROM BatchVertex WHERE value < 50")
        .stream()
        .filter(r -> r.<Integer>getProperty("value") % 2 == 0)
        .count();

    assertThat(count).isEqualTo(25);
  }

  @Test
  void shouldHandleOrderByDescending() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex ORDER BY value DESC LIMIT 5");

    int previousValue = Integer.MAX_VALUE;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final int currentValue = result.getProperty("value");
      assertThat(currentValue).isLessThanOrEqualTo(previousValue);
      previousValue = currentValue;
    }
  }

  @Test
  void shouldHandleLimitClause() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex LIMIT 15");

    int count = 0;
    while (resultSet.hasNext()) {
      resultSet.next();
      count++;
    }

    assertThat(count).isEqualTo(15);
  }

  @Test
  void shouldHandleSkipClause() {
    final ResultSet resultSet = database.query("sql", "SELECT FROM BatchVertex ORDER BY value SKIP 90");

    int count = 0;
    int minValue = Integer.MAX_VALUE;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final int value = result.getProperty("value");
      if (value < minValue) {
        minValue = value;
      }
      count++;
    }

    assertThat(count).isEqualTo(10);
    assertThat(minValue).isGreaterThanOrEqualTo(90);
  }

  @Test
  void shouldHandleWhereClause() {
    final ResultSet resultSet = database.query("sql",
        "SELECT FROM BatchVertex WHERE value >= 30 AND value < 40 ORDER BY value");

    int expectedValue = 30;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      assertThat(result.<Integer>getProperty("value")).isEqualTo(expectedValue);
      expectedValue++;
    }

    assertThat(expectedValue).isEqualTo(40);
  }
}
