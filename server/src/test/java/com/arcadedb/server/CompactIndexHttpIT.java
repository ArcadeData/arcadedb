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
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());

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
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
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
}
