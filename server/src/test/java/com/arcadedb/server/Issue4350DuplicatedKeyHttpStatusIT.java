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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4350: a unique-constraint violation must surface over HTTP as
 * 409 Conflict (RFC 9110 §15.5.10), never 503 Service Unavailable. 503 is retry-worthy and
 * caused upstream clients/load balancers to retry the bad write indefinitely, amplifying
 * the problem and triggering 5xx-rate alerts on what is really a client data conflict.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4350DuplicatedKeyHttpStatusIT extends BaseGraphServerTest {

  @Test
  void duplicateKeyReturns409Conflict() throws Exception {
    testEachServer(serverIndex -> {
      // Create the type, unique index, and the first row in one script. We intentionally do not
      // include the duplicate insert here, so that the duplicate is committed by its own request
      // and the server reports the error synchronously (not as part of a sqlScript batch).
      execCommand(serverIndex, "sqlScript", """
          CREATE VERTEX TYPE issue4350;
          CREATE PROPERTY issue4350.code STRING;
          CREATE INDEX ON issue4350 (code) UNIQUE;
          CREATE VERTEX issue4350 SET code = 'X-1';
          """, 200);

      // Now insert a duplicate row. This must yield HTTP 409 Conflict, NOT 503.
      final HttpURLConnection connection = openCommandConnection(serverIndex);
      try {
        final JSONObject payload = new JSONObject()
            .put("language", "sql")
            .put("command", "CREATE VERTEX issue4350 SET code = 'X-1'");

        try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
          pw.write(payload.toString());
        }

        final int statusCode = connection.getResponseCode();
        final String response = readError(connection);

        assertThat(statusCode)
            .as("duplicate-key violation must return 409 Conflict, got %d (body=%s)", statusCode, response)
            .isEqualTo(409);

        final JSONObject json = new JSONObject(response);
        assertThat(json.getString("exception")).isEqualTo(DuplicatedKeyException.class.getName());
        assertThat(json.getString("error")).isEqualTo("Found duplicate key in index");
        // exceptionArgs format is "<indexName>|<keys>|<currentIndexedRID>" — keep the existing
        // contract intact so RemoteHttpComponent can still reconstruct DuplicatedKeyException
        // client-side.
        assertThat(json.getString("exceptionArgs")).contains("|").contains("X-1");
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Same scenario but the duplicate is produced inside a multi-statement {@code sqlScript},
   * which is the exact shape that triggered the original bug report (batch MERGE statements
   * against UNIQUE indexes). The transactional wrapper must not down-grade the status code
   * to 5xx.
   */
  @Test
  void duplicateKeyInScriptReturns409Conflict() throws Exception {
    testEachServer(serverIndex -> {
      // Prepare type + unique index in a successful script first so the duplicate below
      // executes against an existing schema.
      execCommand(serverIndex, "sqlScript", """
          CREATE VERTEX TYPE issue4350_script;
          CREATE PROPERTY issue4350_script.code STRING;
          CREATE INDEX ON issue4350_script (code) UNIQUE;
          """, 200);

      final HttpURLConnection connection = openCommandConnection(serverIndex);
      try {
        final JSONObject payload = new JSONObject()
            .put("language", "sqlScript")
            .put("command", """
                CREATE VERTEX issue4350_script SET code = 'dup';
                CREATE VERTEX issue4350_script SET code = 'dup';
                """);

        try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
          pw.write(payload.toString());
        }

        final int statusCode = connection.getResponseCode();
        final String response = readError(connection);

        assertThat(statusCode)
            .as("duplicate-key violation inside sqlScript must return 409 Conflict, got %d (body=%s)", statusCode, response)
            .isEqualTo(409);
        assertThat(response).contains("DuplicatedKeyException");
      } finally {
        connection.disconnect();
      }
    });
  }

  private void execCommand(final int serverIndex, final String language, final String sql, final int expectedStatus)
      throws Exception {
    final HttpURLConnection connection = openCommandConnection(serverIndex);
    try {
      final JSONObject payload = new JSONObject().put("language", language).put("command", sql);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      assertThat(connection.getResponseCode()).isEqualTo(expectedStatus);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openCommandConnection(final int serverIndex) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    return connection;
  }
}
