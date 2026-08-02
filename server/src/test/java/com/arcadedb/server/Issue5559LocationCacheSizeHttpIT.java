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
 * Issue #5559: a refused {@code locationCacheSize} reaches an HTTP client as 400, not 500.
 * <p>
 * The refusal is a client input error - the statement asked for a cap the index cannot honour - so it belongs in
 * the client-error family, where issue #5639 already put every other bad {@code METADATA} value. It gets there by a
 * different route than #5639's though, and the difference is why this test exists rather than a change to the
 * exception type: {@code metadataInt} throws {@link IllegalArgumentException}, which the handler has its own arm
 * for, while {@code setLocationCacheSize} throws {@code IndexException} - matching its sibling setters
 * {@code setSimilarity} and {@code setQuantization} - and the SQL DDL path wraps that into a
 * {@code CommandSQLParsingException}, which is exactly what {@code AbstractServerHttpHandler}'s 400 arm keys on,
 * both directly and unwrapped from a cause.
 * <p>
 * So the status code is correct today but it rests on a wrap two layers away from the throw. Nothing else pins it:
 * the engine-side {@code Issue5559LocationCacheSizeTest} asserts on the message and cannot see a status code, so a
 * future change to how DDL wraps a metadata failure would silently turn this into a 500 carrying a helpful message
 * nobody displays. Both rejection arms of the one key are asserted below, since they take different paths into the
 * handler and must not disagree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5559LocationCacheSizeHttpIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "Issue5559LocationCacheSizeHttpIT";
  private static final String TYPE_NAME     = "HttpVectorDoc";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    // No default population.
  }

  @Test
  void aRefusedLocationCacheSizeAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE " + TYPE_NAME + " IF NOT EXISTS");
        database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      });

      final String response = postCommand(serverIndex, """
          CREATE INDEX ON %s (embedding) LSM_VECTOR \
          METADATA {"dimensions": 16, "similarity": "COSINE", "locationCacheSize": 10}\
          """.formatted(TYPE_NAME), 400);

      assertThat(response).as("the 400 must carry the explanation, not a bare failure").contains("locationCacheSize");

      // The same key rejected by the other check in the same method already answered 400 (issue #5639). Both arms of
      // the one setting must agree, or the status code depends on which validation happened to fire.
      postCommand(serverIndex, """
          CREATE INDEX ON %s (embedding) LSM_VECTOR \
          METADATA {"dimensions": 16, "similarity": "COSINE", "locationCacheSize": 3000000000}\
          """.formatted(TYPE_NAME), 400);

      assertThat(database.getSchema().existsIndex(TYPE_NAME + "[embedding]"))
          .as("a refused statement must not leave an index behind").isFalse();

      // "no limit" is accepted, so the refusal is not simply rejecting the key.
      postCommand(serverIndex, """
          CREATE INDEX ON %s (embedding) LSM_VECTOR \
          METADATA {"dimensions": 16, "similarity": "COSINE", "locationCacheSize": -1}\
          """.formatted(TYPE_NAME), 200);

      assertThat(database.getSchema().existsIndex(TYPE_NAME + "[embedding]")).isTrue();
    });
  }

  /** POSTs a SQL command, asserts the status code and returns the body. */
  private String postCommand(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    formatPayload(connection, new JSONObject().put("language", "sql").put("command", command));
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).as("status for: %s", command).isEqualTo(expectedStatus);
      try (final InputStream stream = connection.getResponseCode() < 400 ?
          connection.getInputStream() :
          connection.getErrorStream()) {
        return stream == null ? "" : new String(stream.readAllBytes());
      }
    } finally {
      connection.disconnect();
    }
  }
}
