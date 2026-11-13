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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #5144: index compaction must be triggerable from the HTTP {@code /command} endpoint
 * (client-server users, who cannot reach the Java {@code Index.compact()} API), via {@code COMPACT INDEX}.
 */
class CompactIndexHttpIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "CompactIndexHttpIT";
  private static final String DOC_TYPE_NAME = "CompactHttpDoc";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    // No default population.
  }

  @Test
  void compactIndexViaHttpCommand() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());

      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + DOC_TYPE_NAME);
        database.command("sql", "CREATE PROPERTY " + DOC_TYPE_NAME + ".num LONG");
        database.command("sql", "CREATE INDEX ON " + DOC_TYPE_NAME + " (num) UNIQUE");
      });

      database.transaction(() -> {
        for (int i = 0; i < 200; i++)
          database.command("sql", "INSERT INTO " + DOC_TYPE_NAME + " SET num = " + i);
      });

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      formatPayload(connection, new JSONObject()
          .put("language", "sql")
          .put("command", "COMPACT INDEX *"));
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final String response;
        try (final InputStream in = connection.getInputStream()) {
          response = new String(in.readAllBytes());
        }
        final JSONObject json = new JSONObject(response);
        final JSONObject record = json.getJSONArray("result").getJSONObject(0);
        assertThat(record.getString("operation")).isEqualTo("compact index");
      } finally {
        connection.disconnect();
      }

      // The index must still serve queries after compaction.
      database.transaction(() -> {
        final long count = database.query("sql", "SELECT FROM " + DOC_TYPE_NAME + " WHERE num = 42").stream().count();
        assertThat(count).isEqualTo(1);
      });
    });
  }

  /**
   * Issue #11580: the request above passed only because its index had nothing to merge. With pages to merge, the default
   * request (no {@code autoCommit}, so the handler wraps the statement in its auto-commit transaction) answered HTTP 500
   * "Cannot replace compacted index because a transaction is active".
   */
  @Test
  void compactIndexWithPagesToMergeViaDefaultHttpCommand() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabaseForOperator(getDatabaseName());
      final String typeName = "CompactHttpMergeDoc";
      final String indexName = typeName + "[email]";

      // Background compaction off, so it cannot take the COMPACTION_SCHEDULED status from the request under test.
      final Object previousMinPages = database.getConfiguration().getValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
      try {
        database.transaction(() -> {
          final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
          type.createProperty("email", Type.STRING);
          database.getSchema().buildTypeIndex(typeName, new String[] { "email" })
              .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(2048).create();
        });
        for (int batch = 0; batch < 5; batch++) {
          final int from = batch * 200;
          database.transaction(() -> {
            for (int i = from; i < from + 200; i++)
              database.newDocument(typeName).set("email", String.format("e%06d", i)).save();
          });
        }

        final HttpURLConnection connection = (HttpURLConnection) new URL(
            "http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + DATABASE_NAME).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
        formatPayload(connection, new JSONObject().put("language", "sql").put("command", "COMPACT INDEX `" + indexName + "`"));
        connection.connect();

        try {
          assertThat(connection.getResponseCode()).as(() -> readErrorQuietly(connection)).isEqualTo(200);
          final JSONObject record = new JSONObject(readResponse(connection)).getJSONArray("result").getJSONObject(0);
          assertThat(record.getBoolean("compacted")).as("a real merge ran").isTrue();
        } finally {
          connection.disconnect();
        }

        database.transaction(() -> {
          for (int i = 0; i < 1000; i++)
            assertThat(database.query("sql", "SELECT FROM " + typeName + " WHERE email = ?", String.format("e%06d", i)).hasNext())
                .isTrue();
        });
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, previousMinPages);
      }
    });
  }

  private String readErrorQuietly(final HttpURLConnection connection) {
    try {
      return readError(connection);
    } catch (final Exception e) {
      return e.toString();
    }
  }
}
