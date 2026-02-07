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

import com.arcadedb.ContextConfiguration;
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
 * Integration tests for RemoteGrpcDatabaseWithCompression.
 * Tests database operations with compression enabled.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteGrpcDatabaseWithCompressionIT extends BaseGraphServerTest {

  private RemoteGrpcServer server;
  private RemoteGrpcDatabaseWithCompression database;

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

    final ContextConfiguration config = new ContextConfiguration();
    database = new RemoteGrpcDatabaseWithCompression(server, "localhost", 50051, 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS, config);

    // Create test schema
    database.command("sql", "CREATE VERTEX TYPE CompressedVertex");
    database.command("sql", "CREATE DOCUMENT TYPE CompressedDocument");
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
  void shouldExecuteQueryWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 1, name = 'Test'");

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 1");

    assertThat(result.hasNext()).isTrue();
    final Result record = result.next();
    assertThat(record.<Integer>getProperty("id")).isEqualTo(1);
    assertThat(record.<String>getProperty("name")).isEqualTo("Test");
  }

  @Test
  void shouldExecuteCommandWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 2, data = 'compressed data'");

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 2");

    assertThat(result.hasNext()).isTrue();
  }

  @Test
  void shouldHandleTransactionWithCompression() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX CompressedVertex SET id = 3, name = 'Transaction'");
      database.command("sql", "UPDATE CompressedVertex SET name = 'Updated' WHERE id = 3");
    });

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 3");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("Updated");
  }

  @Test
  void shouldRollbackWithCompression() {
    try {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX CompressedVertex SET id = 4, name = 'Rollback'");
        throw new RuntimeException("Force rollback");
      });
    } catch (final RuntimeException e) {
      // Expected
    }

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 4");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void shouldHandleLargeDataWithCompression() {
    // Create a large string that should compress well
    final StringBuilder largeData = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeData.append("This is a repeating text that should compress well. ");
    }

    database.command("sql",
        "CREATE VERTEX CompressedVertex SET id = 5, largeData = '" + largeData.toString().replace("'", "\\'") + "'");

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 5");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("largeData")).contains("repeating text");
  }

  @Test
  void shouldHandleMultipleRecordsWithCompression() {
    for (int i = 0; i < 50; i++) {
      database.command("sql", "CREATE VERTEX CompressedVertex SET id = " + (100 + i) + ", name = 'Record " + i + "'");
    }

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id >= 100 AND id < 150");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(50);
  }

  @Test
  void shouldHandleAggregationWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 200, value = 10");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 201, value = 20");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 202, value = 30");

    final ResultSet result = database.query("sql",
        "SELECT count(*) AS cnt, sum(value) AS total FROM CompressedVertex WHERE id >= 200 AND id <= 202");

    assertThat(result.hasNext()).isTrue();
    final Result record = result.next();
    final Number cnt = record.getProperty("cnt");
    assertThat(cnt.longValue()).isEqualTo(3L);
    final Number total = record.getProperty("total");
    assertThat(total.longValue()).isEqualTo(60L);
  }

  @Test
  void shouldHandleUpdateWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 300, status = 'initial'");
    database.command("sql", "UPDATE CompressedVertex SET status = 'updated' WHERE id = 300");

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 300");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("status")).isEqualTo("updated");
  }

  @Test
  void shouldHandleDeleteWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 400, name = 'ToDelete'");

    final long countBefore = database.query("sql", "SELECT count(*) AS cnt FROM CompressedVertex WHERE id = 400")
        .next()
        .getProperty("cnt");

    database.command("sql", "DELETE FROM CompressedVertex WHERE id = 400");

    final long countAfter = database.query("sql", "SELECT count(*) AS cnt FROM CompressedVertex WHERE id = 400")
        .next()
        .getProperty("cnt");

    assertThat(countBefore).isEqualTo(1L);
    assertThat(countAfter).isEqualTo(0L);
  }

  @Test
  void shouldHandleEmptyResultSetWithCompression() {
    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 999999");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void shouldHandleNullValuesWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 500, name = null, description = 'Has null name'");

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 500");

    assertThat(result.hasNext()).isTrue();
    final Result record = result.next();
    assertThat((Object) record.getProperty("name")).isNull();
    assertThat(record.<String>getProperty("description")).isEqualTo("Has null name");
  }

  @Test
  void shouldHandleOrderByWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 600, sortKey = 30");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 601, sortKey = 10");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 602, sortKey = 20");

    final ResultSet result = database.query("sql",
        "SELECT FROM CompressedVertex WHERE id >= 600 AND id <= 602 ORDER BY sortKey");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("sortKey")).isEqualTo(10);
    assertThat(result.next().<Integer>getProperty("sortKey")).isEqualTo(20);
    assertThat(result.next().<Integer>getProperty("sortKey")).isEqualTo(30);
  }

  @Test
  void shouldHandleGroupByWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 700, category = 'A', value = 1");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 701, category = 'A', value = 2");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 702, category = 'B', value = 3");
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 703, category = 'B', value = 4");

    final ResultSet result = database.query("sql",
        "SELECT category, count(*) AS cnt FROM CompressedVertex WHERE id >= 700 AND id <= 703 GROUP BY category ORDER BY category");

    assertThat(result.hasNext()).isTrue();
    final Result recordA = result.next();
    assertThat(recordA.<String>getProperty("category")).isEqualTo("A");
    final Number cntA = recordA.getProperty("cnt");
    assertThat(cntA.longValue()).isEqualTo(2L);

    assertThat(result.hasNext()).isTrue();
    final Result recordB = result.next();
    assertThat(recordB.<String>getProperty("category")).isEqualTo("B");
    final Number cntB = recordB.getProperty("cnt");
    assertThat(cntB.longValue()).isEqualTo(2L);
  }

  @Test
  void shouldHandleMetadataWithCompression() {
    database.command("sql", "CREATE VERTEX CompressedVertex SET id = 800, name = 'Metadata Test'");

    final ResultSet result = database.query("sql", "SELECT FROM CompressedVertex WHERE id = 800");

    assertThat(result.hasNext()).isTrue();
    final Result record = result.next();
    if (record.getElement().isPresent()) {
      assertThat(record.getElement().get().getTypeName()).isEqualTo("CompressedVertex");
    }
  }

  @Test
  void shouldCloseGracefully() {
    database.close();

    // Should be able to close multiple times
    database.close();
  }
}
