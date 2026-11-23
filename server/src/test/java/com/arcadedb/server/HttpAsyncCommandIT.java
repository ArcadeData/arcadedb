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

import com.arcadedb.database.Database;
import com.arcadedb.index.Index;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Commands sent to {@code POST /api/v1/command} with {@code awaitResponse=false}: they are answered 202 and run on the async
 * command pool, and the DDL among them (index rebuild and creation, materialized view refresh) must do the same work it does
 * when run synchronously. Each test pairs the async request with the assertion about what it left behind.
 * <p>
 * Each group of tests below keeps the explanation of the issue it was written for.
 */
class HttpAsyncCommandIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME          = "HttpAsyncCommand";
  private static final String REBUILD_DOC_TYPE_NAME  = "TestDoc";
  private static final String VIEW_DOC_TYPE_NAME     = "Doc";
  private static final String VIEW_NAME              = "DocView";
  private static final String DDL_TYPE_NAME          = "AsyncDDLDoc";
  private static final int    DDL_RECORDS            = 100;

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    // No default population - each test sets up its own data
  }

  /*
   * Test for GitHub Issue #2097: REBUILD INDEX deletes indexes when used via async HTTP-API.
   * https://github.com/ArcadeData/arcadedb/issues/2097
   * <p>
   * When running `REBUILD INDEX *` asynchronously (via HTTP API with awaitResponse=false),
   * the indexes got deleted instead of rebuilt: BucketIndexBuilder.create() scheduled blocking
   * tasks on the async threads while already running ON one of them, which surfaced as a
   * "stalled queue" error partway through the rebuild.
   * <p>
   * Since issue #6303 the operation is SUPPORTED rather than refused - a dispatched command runs
   * on AsyncCommandPool, which is not one of those workers, so the barrier it needs can be
   * satisfied. What this class pins is what #2097 was about and is still worth pinning: the
   * indexes survive. That the rebuild now actually rebuilds is pinned by
   * {@link #createIndexWithAwaitResponseFalseBuildsAnIndexThatCoversEveryRecord()}.
   */

  // Regression: #2097 an async REBUILD INDEX must not delete the indexes
  /**
   * Test that REBUILD INDEX * via async HTTP API (awaitResponse=false) preserves indexes.
   * <p>
   * This tests the fix for issue #2097 where running REBUILD INDEX * asynchronously
   * would cause indexes to be deleted when the "Asynchronous queue is stalled" error occurred.
   * <p>
   * The first fix prevented REBUILD INDEX from running in an async context at all, throwing a
   * NeedRetryException before any index modification occurred, so the indexes were preserved even
   * though the command failed. Since issue #6303 the command runs off the async workers and
   * completes; either way the assertion below - the indexes are still there and still answer
   * queries - is the one #2097 asked for.
   */
  @Test
  void rebuildIndexAsyncDoesNotDeleteIndexes() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());

      // Setup: Create multiple document types with indexed properties
      // The issue is more likely to occur with multiple indexes
      final int NUM_TYPES = 5;
      database.transaction(() -> {
        for (int i = 0; i < NUM_TYPES; i++) {
          final String typeName = REBUILD_DOC_TYPE_NAME + i;
          database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
          database.command("sql", "CREATE PROPERTY " + typeName + ".num LONG");
          database.command("sql", "CREATE PROPERTY " + typeName + ".name STRING");
          database.command("sql", "CREATE INDEX ON " + typeName + " (num) UNIQUE");
          database.command("sql", "CREATE INDEX ON " + typeName + " (name) NOTUNIQUE");
        }
      });

      // Insert some test data
      database.transaction(() -> {
        for (int i = 0; i < NUM_TYPES; i++) {
          final String typeName = REBUILD_DOC_TYPE_NAME + i;
          database.command("sql", "INSERT INTO " + typeName + " SET num = 0, name = 'test0'");
          database.command("sql", "INSERT INTO " + typeName + " SET num = 1, name = 'test1'");
          database.command("sql", "INSERT INTO " + typeName + " SET num = 2, name = 'test2'");
        }
      });

      // Verify the index exists before rebuild
      final Index[] indexesBefore = database.getSchema().getIndexes();
      assertThat(indexesBefore).isNotEmpty();
      final int indexCountBefore = indexesBefore.length;
      LogManager.instance()
          .log(this, Level.INFO, "Indexes before async REBUILD INDEX: %d", indexCountBefore);

      // Execute REBUILD INDEX * asynchronously via HTTP API
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      formatPayload(connection, new JSONObject()
          .put("language", "sqlscript")
          .put("command", "REBUILD INDEX *;")
          .put("awaitResponse", false));
      connection.connect();

      try {
        final int responseCode = connection.getResponseCode();
        LogManager.instance().log(this, Level.INFO, "Async REBUILD INDEX response code: %d", responseCode);
        assertThat(responseCode).isEqualTo(202); // Accepted for async execution
      } finally {
        connection.disconnect();
      }

      // Wait for async command to complete
      database.async().waitCompletion(30000);

      // Additional wait to ensure any cleanup is complete
      Thread.sleep(1000);

      // Verify the index still exists after rebuild
      final Index[] indexesAfter = database.getSchema().getIndexes();
      final int indexCountAfter = indexesAfter.length;
      LogManager.instance()
          .log(this, Level.INFO, "Indexes after async REBUILD INDEX: %d", indexCountAfter);

      // The issue is that indexes get deleted - this assertion should pass after the fix
      assertThat(indexCountAfter)
          .as("Indexes should not be deleted after async REBUILD INDEX *. Before: %d, After: %d",
              indexCountBefore, indexCountAfter)
          .isEqualTo(indexCountBefore);

      // Verify indexes for all types exist
      for (int i = 0; i < 5; i++) {
        final String typeName = REBUILD_DOC_TYPE_NAME + i;
        final int typeNum = i;

        final boolean hasNumIndex = Arrays.stream(indexesAfter)
            .anyMatch(idx -> idx.getName().contains(typeName) && idx.getPropertyNames().contains("num"));
        assertThat(hasNumIndex)
            .as("Index on %s.num should exist after async REBUILD INDEX", typeName)
            .isTrue();

        // Verify the index works by querying with it
        database.transaction(() -> {
          final long count = database.query("sql", "SELECT FROM " + typeName + " WHERE num = 1")
              .stream().count();
          assertThat(count).as("Query using index on %s should return 1 result", typeName).isEqualTo(1);
        });
      }
    });
  }

  // Regression: #2097 an async REBUILD INDEX must not delete the indexes
  /**
   * Test that REBUILD INDEX works correctly when called synchronously.
   * This is a baseline test to confirm normal behavior.
   */
  @Test
  void rebuildIndexSyncWorksCorrectly() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());

      // Setup: Create document type with indexed property
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + REBUILD_DOC_TYPE_NAME + "Sync");
        database.command("sql", "CREATE PROPERTY " + REBUILD_DOC_TYPE_NAME + "Sync.num LONG");
        database.command("sql", "CREATE INDEX ON " + REBUILD_DOC_TYPE_NAME + "Sync (num) UNIQUE");
      });

      // Insert some test data
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + REBUILD_DOC_TYPE_NAME + "Sync SET num = 0");
        database.command("sql", "INSERT INTO " + REBUILD_DOC_TYPE_NAME + "Sync SET num = 1");
      });

      // Verify the index exists before rebuild
      final Index[] indexesBefore = database.getSchema().getIndexes();
      final int indexCountBefore = indexesBefore.length;

      // Execute REBUILD INDEX * synchronously via HTTP API (awaitResponse=true or omitted)
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      formatPayload(connection, new JSONObject()
          .put("language", "sqlscript")
          .put("command", "REBUILD INDEX *;"));
      // Note: awaitResponse defaults to true
      connection.connect();

      try {
        final int responseCode = connection.getResponseCode();
        LogManager.instance().log(this, Level.INFO, "Sync REBUILD INDEX response code: %d", responseCode);
        assertThat(responseCode).isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      // Verify the index still exists after rebuild
      final Index[] indexesAfter = database.getSchema().getIndexes();
      final int indexCountAfter = indexesAfter.length;

      assertThat(indexCountAfter)
          .as("Indexes should be preserved after sync REBUILD INDEX *")
          .isEqualTo(indexCountBefore);
    });
  }

  // Regression: #2097 an async REBUILD INDEX must not delete the indexes
  /**
   * Test that REBUILD INDEX on a specific index via async HTTP API preserves the index.
   */
  @Test
  void rebuildSpecificIndexAsyncDoesNotDeleteIndex() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());

      // Setup: Create document type with indexed property
      final String typeName = REBUILD_DOC_TYPE_NAME + "Specific";
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".val STRING");
        database.command("sql", "CREATE INDEX ON " + typeName + " (val) UNIQUE");
      });

      // Insert some test data
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + typeName + " SET val = 'test1'");
        database.command("sql", "INSERT INTO " + typeName + " SET val = 'test2'");
      });

      // Find the specific bucket index name
      final String bucketIndexName = Arrays.stream(database.getSchema().getIndexes())
          .filter(idx -> idx.getName().contains(typeName) && !idx.getName().equals(typeName + "[val]"))
          .findFirst()
          .map(Index::getName)
          .orElseThrow(() -> new RuntimeException("Could not find bucket index"));

      LogManager.instance().log(this, Level.INFO, "Rebuilding specific index: %s", bucketIndexName);

      final int indexCountBefore = database.getSchema().getIndexes().length;

      // Execute REBUILD INDEX on specific index asynchronously
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      formatPayload(connection, new JSONObject()
          .put("language", "sqlscript")
          .put("command", "REBUILD INDEX `" + bucketIndexName + "`;")
          .put("awaitResponse", false));
      connection.connect();

      try {
        final int responseCode = connection.getResponseCode();
        assertThat(responseCode).isEqualTo(202);
      } finally {
        connection.disconnect();
      }

      // Wait for async command to complete
      database.async().waitCompletion(30000);

      Thread.sleep(1000);

      // Verify the index still exists
      final int indexCountAfter = database.getSchema().getIndexes().length;
      assertThat(indexCountAfter)
          .as("Index count should remain the same after async REBUILD INDEX on specific index")
          .isEqualTo(indexCountBefore);
    });
  }

  /*
   * Regression test for GitHub Issue #3941: REFRESH MATERIALIZED VIEW not working in async HTTP commands.
   * https://github.com/ArcadeData/arcadedb/issues/3941
   *
   * When sending REFRESH MATERIALIZED VIEW via HTTP with awaitResponse=false, the view was not updated
   * because the refresh joined the async thread's long-running batched transaction instead of
   * committing its own transaction immediately.
   */

  // Regression: #3941 an async REFRESH MATERIALIZED VIEW must update the view
  @Test
  void refreshMaterializedViewAsyncUpdatesView() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());

      // Setup: create document type, insert initial record, then create view
      // The INSERT must be committed before CREATE MATERIALIZED VIEW so the initial
      // population query can see the committed data.
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + VIEW_DOC_TYPE_NAME);
        database.command("sql", "INSERT INTO " + VIEW_DOC_TYPE_NAME);
      });
      database.transaction(() ->
          database.command("sql", "CREATE MATERIALIZED VIEW " + VIEW_NAME + " AS SELECT FROM " + VIEW_DOC_TYPE_NAME));

      // Verify initial state: view has 1 record
      final long initialCount = countViewRecords(database);
      assertThat(initialCount).as("View should have 1 record after creation").isEqualTo(1L);

      // Insert a second document (not yet reflected in view)
      database.transaction(() -> database.command("sql", "INSERT INTO " + VIEW_DOC_TYPE_NAME));

      final long sourceCountBeforeRefresh = countSourceRecords(database);
      assertThat(sourceCountBeforeRefresh).as("Source type should have 2 records").isEqualTo(2L);

      // Send REFRESH MATERIALIZED VIEW asynchronously (awaitResponse=false) - the bug scenario
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      formatPayload(connection, new JSONObject()
          .put("language", "sql")
          .put("command", "REFRESH MATERIALIZED VIEW " + VIEW_NAME)
          .put("awaitResponse", false));
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).as("Async command should return 202 Accepted").isEqualTo(202);
      } finally {
        connection.disconnect();
      }

      // Wait for the async command to complete and commit
      database.async().waitCompletion(10000);

      // The view should now reflect the 2 source records
      final long refreshedCount = countViewRecords(database);
      assertThat(refreshedCount)
          .as("View should have 2 records after async REFRESH MATERIALIZED VIEW (was %d before refresh)", initialCount)
          .isEqualTo(2L);
    });
  }

  // Regression: #3941 an async REFRESH MATERIALIZED VIEW must update the view
  @Test
  void refreshMaterializedViewSyncUpdatesView() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());

      final String syncDocType = VIEW_DOC_TYPE_NAME + "Sync";
      final String syncViewName = VIEW_NAME + "Sync";

      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + syncDocType);
        database.command("sql", "INSERT INTO " + syncDocType);
      });
      database.transaction(() ->
          database.command("sql", "CREATE MATERIALIZED VIEW " + syncViewName + " AS SELECT FROM " + syncDocType));

      assertThat(countRecords(database, syncViewName)).isEqualTo(1L);

      database.transaction(() -> database.command("sql", "INSERT INTO " + syncDocType));

      // Send REFRESH synchronously (the working baseline)
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      formatPayload(connection, new JSONObject()
          .put("language", "sql")
          .put("command", "REFRESH MATERIALIZED VIEW " + syncViewName));
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).as("Sync command should return 200 OK").isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      assertThat(countRecords(database, syncViewName)).as("View should have 2 records after sync refresh").isEqualTo(2L);
    });
  }

  private long countViewRecords(final Database database) {
    return countRecords(database, VIEW_NAME);
  }

  private long countSourceRecords(final Database database) {
    return countRecords(database, VIEW_DOC_TYPE_NAME);
  }

  private long countRecords(final Database database, final String typeName) {
    try (final var rs = database.query("sql", "SELECT count(*) as cnt FROM " + typeName)) {
      if (rs.hasNext())
        return ((Number) rs.next().getProperty("cnt")).longValue();
      return 0;
    }
  }

  /*
   * Issue #6303, item 3, over the transport it was reported on: {@code POST /command} with {@code awaitResponse=false}.
   * <p>
   * That is a documented way to fire a long DDL statement, and index creation is exactly the kind of long DDL somebody
   * would fire that way. It used to HANG (the command ran on an async worker and the barrier it needs enqueues a marker
   * on every worker including that one); #6281 turned the hang into a clear refusal plus a "run it synchronously"
   * workaround. The #2097 tests above pin what survived the refusal - the indexes were at least not
   * destroyed - and this pins what the refusal did not give back: the index is actually built, and it covers every
   * record.
   */

  // Regression: #6303 a DDL statement sent with awaitResponse=false is built, not refused
  @Test
  void createIndexWithAwaitResponseFalseBuildsAnIndexThatCoversEveryRecord() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());
      final String typeName = DDL_TYPE_NAME + serverIndex;

      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName).close();
        database.command("sql", "CREATE PROPERTY " + typeName + ".num LONG").close();
      });
      database.transaction(() -> {
        for (int i = 0; i < DDL_RECORDS; i++)
          database.command("sql", "INSERT INTO " + typeName + " SET num = " + i).close();
      });

      assertThat(postAwaitResponseFalse(serverIndex, "CREATE INDEX ON " + typeName + " (num) UNIQUE;")).isEqualTo(202);

      // The command is not on a worker queue any more, but waitCompletion still covers it - that is the property
      // moving the dispatch would otherwise have taken away.
      assertThat(database.async().waitCompletion(60_000)).as("the dispatched command must finish, not park").isTrue();

      assertThat(database.getSchema().getIndexByName(typeName + "[num]").countEntries()).as(
          "an index created with awaitResponse=false must be built, not refused, and must cover every record")
          .isEqualTo(DDL_RECORDS);

      database.transaction(() -> assertThat(
          database.query("sql", "SELECT FROM " + typeName + " WHERE num = 42").stream().count()).isEqualTo(1));
    });
  }

  /** Sends the command with {@code awaitResponse=false} and returns the HTTP status. */
  private int postAwaitResponseFalse(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection,
        new JSONObject().put("language", "sqlscript").put("command", command).put("awaitResponse", false));
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
