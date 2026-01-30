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

import java.net.*;
import java.util.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Test for GitHub Issue #2097: REBUILD INDEX deletes indexes when used via async HTTP-API.
 * https://github.com/ArcadeData/arcadedb/issues/2097
 * <p>
 * When running `REBUILD INDEX *` asynchronously (via HTTP API with awaitResponse=false),
 * the indexes get deleted instead of rebuilt because the BucketIndexBuilder.create()
 * method tries to schedule blocking tasks on async threads while already running
 * in an async context, causing a "stalled queue" error.
 */
class Issue2097AsyncRebuildIndexIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "Issue2097AsyncRebuildIndex";
  private static final String DOC_TYPE_NAME = "TestDoc";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    // No default population - each test will set up its own data
  }

  /**
   * Test that REBUILD INDEX * via async HTTP API (awaitResponse=false) preserves indexes.
   * <p>
   * This tests the fix for issue #2097 where running REBUILD INDEX * asynchronously
   * would cause indexes to be deleted when the "Asynchronous queue is stalled" error occurred.
   * <p>
   * The fix prevents REBUILD INDEX from running in async context by throwing a NeedRetryException
   * with a clear error message before any index modification occurs. This ensures indexes are
   * preserved even though the async command fails.
   * <p>
   * Users should use synchronous execution (awaitResponse=true) for REBUILD INDEX operations.
   */
  @Test
  void rebuildIndexAsyncDoesNotDeleteIndexes() throws Exception {
    testEachServer((serverIndex) -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());

      // Setup: Create multiple document types with indexed properties
      // The issue is more likely to occur with multiple indexes
      final int NUM_TYPES = 5;
      database.transaction(() -> {
        for (int i = 0; i < NUM_TYPES; i++) {
          final String typeName = DOC_TYPE_NAME + i;
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
          final String typeName = DOC_TYPE_NAME + i;
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
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

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
        final String typeName = DOC_TYPE_NAME + i;
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

  /**
   * Test that REBUILD INDEX works correctly when called synchronously.
   * This is a baseline test to confirm normal behavior.
   */
  @Test
  void rebuildIndexSyncWorksCorrectly() throws Exception {
    testEachServer((serverIndex) -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());

      // Setup: Create document type with indexed property
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + DOC_TYPE_NAME + "Sync");
        database.command("sql", "CREATE PROPERTY " + DOC_TYPE_NAME + "Sync.num LONG");
        database.command("sql", "CREATE INDEX ON " + DOC_TYPE_NAME + "Sync (num) UNIQUE");
      });

      // Insert some test data
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + DOC_TYPE_NAME + "Sync SET num = 0");
        database.command("sql", "INSERT INTO " + DOC_TYPE_NAME + "Sync SET num = 1");
      });

      // Verify the index exists before rebuild
      final Index[] indexesBefore = database.getSchema().getIndexes();
      final int indexCountBefore = indexesBefore.length;

      // Execute REBUILD INDEX * synchronously via HTTP API (awaitResponse=true or omitted)
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

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

  /**
   * Test that REBUILD INDEX on a specific index via async HTTP API preserves the index.
   */
  @Test
  void rebuildSpecificIndexAsyncDoesNotDeleteIndex() throws Exception {
    testEachServer((serverIndex) -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());

      // Setup: Create document type with indexed property
      final String typeName = DOC_TYPE_NAME + "Specific";
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
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

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
}
