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
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Regression test for GitHub Issue #3941: REFRESH MATERIALIZED VIEW not working in async HTTP commands.
 * https://github.com/ArcadeData/arcadedb/issues/3941
 *
 * When sending REFRESH MATERIALIZED VIEW via HTTP with awaitResponse=false, the view was not updated
 * because the refresh joined the async thread's long-running batched transaction instead of
 * committing its own transaction immediately.
 */
class Issue3941AsyncRefreshMaterializedViewIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "Issue3941AsyncRefreshMaterializedView";
  private static final String DOC_TYPE_NAME = "Doc";
  private static final String VIEW_NAME     = "DocView";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    // No default population
  }

  @Test
  void refreshMaterializedViewAsyncUpdatesView() throws Exception {
    testEachServer((serverIndex) -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());

      // Setup: create document type, insert initial record, then create view
      // The INSERT must be committed before CREATE MATERIALIZED VIEW so the initial
      // population query can see the committed data.
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + DOC_TYPE_NAME);
        database.command("sql", "INSERT INTO " + DOC_TYPE_NAME);
      });
      database.transaction(() ->
          database.command("sql", "CREATE MATERIALIZED VIEW " + VIEW_NAME + " AS SELECT FROM " + DOC_TYPE_NAME));

      // Verify initial state: view has 1 record
      final long initialCount = countViewRecords(database);
      assertThat(initialCount).as("View should have 1 record after creation").isEqualTo(1L);

      // Insert a second document (not yet reflected in view)
      database.transaction(() -> database.command("sql", "INSERT INTO " + DOC_TYPE_NAME));

      final long sourceCountBeforeRefresh = countSourceRecords(database);
      assertThat(sourceCountBeforeRefresh).as("Source type should have 2 records").isEqualTo(2L);

      // Send REFRESH MATERIALIZED VIEW asynchronously (awaitResponse=false) — the bug scenario
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
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

  @Test
  void refreshMaterializedViewSyncUpdatesView() throws Exception {
    testEachServer((serverIndex) -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());

      final String syncDocType = DOC_TYPE_NAME + "Sync";
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
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
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
    return countRecords(database, DOC_TYPE_NAME);
  }

  private long countRecords(final Database database, final String typeName) {
    try (final var rs = database.query("sql", "SELECT count(*) as cnt FROM " + typeName)) {
      if (rs.hasNext())
        return ((Number) rs.next().getProperty("cnt")).longValue();
      return 0;
    }
  }
}
