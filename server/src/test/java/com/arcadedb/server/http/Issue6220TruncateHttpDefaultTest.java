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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which path {@code TRUNCATE} takes is decided by whether a transaction is active when it runs (issue #6220), and
 * over HTTP that is decided by the request's auto-commit transaction rather than by anything in the statement. This
 * pins the surface most callers actually hit: the default is the transactional path, and {@code "autoCommit": false}
 * is how a bulk clear asks for the faster, non-undoable one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6220TruncateHttpDefaultTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  void theHttpDefaultTakesTheTransactionalPath() throws Exception {
    testEachServer(serverIndex -> assertTruncateReports(serverIndex, "TruncateDefaultDoc", null, true));
  }

  @Test
  void autoCommitFalseSelectsTheStatementOwnedPath() throws Exception {
    testEachServer(serverIndex -> assertTruncateReports(serverIndex, "TruncateNoAutoCommitDoc", false, false));
  }

  /**
   * Fills {@code typeName}, truncates it over HTTP with the given {@code autoCommit} payload value ({@code null}
   * leaves the parameter out entirely), and asserts both the reported path and that the records are gone for good.
   * <p>
   * The fixture is built through the database handle rather than through {@code executeCommand}: that helper
   * swallows a failed request and returns {@code null}, so anything upstream that breaks a POST surfaces here as a
   * baffling "Type not found" from the first assertion instead of as itself. Only the truncate - the thing actually
   * under test - goes over HTTP.
   */
  private void assertTruncateReports(final int serverIndex, final String typeName, final Boolean autoCommit,
      final boolean expectedTransactional) throws Exception {
    final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument(typeName).set("n", i).save();
    });

    assertThat(database.countType(typeName, false)).isEqualTo(5L);

    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", "TRUNCATE TYPE `" + typeName + "` UNSAFE");
    if (autoCommit != null)
      payload.put("autoCommit", autoCommit);

    formatPayload(connection, payload);
    connection.connect();

    try {
      // The status is read - and the error body reported with it - BEFORE readResponse(), which throws a bare
      // IOException naming only the code on anything but a 2xx.
      final int status = connection.getResponseCode();
      assertThat(status).as("TRUNCATE over HTTP failed: %s", status == 200 ? "" : readError(connection)).isEqualTo(200);

      final String response = readResponse(connection);
      final JSONObject result = new JSONObject(response).getJSONArray("result").getJSONObject(0);
      assertThat(result.getBoolean("transactional"))
          .as("autoCommit=%s must take the %s path", autoCommit, expectedTransactional ? "transactional" : "statement-owned")
          .isEqualTo(expectedTransactional);
    } finally {
      connection.disconnect();
    }

    // Either path leaves the type empty once the request is over: the transactional one because the handler commits
    // the request transaction, the statement-owned one because the statement committed its own.
    assertThat(database.countType(typeName, false)).isZero();
  }
}
