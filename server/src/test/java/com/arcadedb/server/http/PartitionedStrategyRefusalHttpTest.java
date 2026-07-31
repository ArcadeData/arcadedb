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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5603. A refused {@code ALTER TYPE ... BucketSelectionStrategy} is a client-side DDL mistake, so it has to
 * come back as HTTP 400.
 * <p>
 * This pins the status because the classification is easy to lose by accident: the handler decides between 400 and
 * 500 on whether the thrown exception is a {@link com.arcadedb.exception.CommandParsingException}, and it does so in
 * two places - once for a plainly thrown exception and once for the same exception wrapped by the auto-commit
 * transaction wrapper. Rewriting the refusal as a {@code CommandExecutionException} to carry a better message, which
 * is exactly what the message fix in this issue set out to do, silently turns it into a 500 that tells clients and
 * load balancers to retry a request that can only ever fail the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionedStrategyRefusalHttpTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  /** A partition key whose stored form does not hash the way the index compares keys: refused, and that is a 400. */
  @Test
  void anUnsuitablePartitionKeyIsRejectedAsAClientError() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE BinPartHttp BUCKETS 3");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY BinPartHttp.k BINARY");
      executeCommand(serverIndex, "sql", "CREATE INDEX ON BinPartHttp(k) UNIQUE");

      final JSONObject error = commandExpecting(serverIndex, 400,
          "ALTER TYPE BinPartHttp BucketSelectionStrategy `partitioned('k')`");

      // Asserting the status alone would not do: an unrelated 400 would pass for the refusal under test.
      assertThat(error.getString("detail"))
          .as("the reason travels to the client, not just the status")
          .contains("BINARY");
    });
  }

  /** The same for the missing-index refusal, which is the older of the two and reaches the client the same way. */
  @Test
  void aPartitionWithoutItsUniqueIndexIsRejectedAsAClientError() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE NoIdxHttp BUCKETS 3");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY NoIdxHttp.k STRING");

      final JSONObject error = commandExpecting(serverIndex, 400,
          "ALTER TYPE NoIdxHttp BucketSelectionStrategy `partitioned('k')`");

      assertThat(error.getString("detail")).contains("unique automatic index");
      assertThat(error.getString("detail"))
          .as("and no longer claims a perfectly valid name does not exist")
          .doesNotContain("was not found");
    });
  }

  /** A suitable partition key over the same endpoint, so the test above cannot pass by refusing everything. */
  @Test
  void aSuitablePartitionKeyIsAccepted() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE OkPartHttp BUCKETS 3");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY OkPartHttp.k STRING");
      executeCommand(serverIndex, "sql", "CREATE INDEX ON OkPartHttp(k) UNIQUE");

      commandExpecting(serverIndex, 200, "ALTER TYPE OkPartHttp BucketSelectionStrategy `partitioned('k')`");
    });
  }

  /** Posts a SQL command, asserts the HTTP status, and returns the parsed body (the error body on a failure). */
  private JSONObject commandExpecting(final int serverIndex, final int expectedStatus, final String sql)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", sql);
    formatPayload(connection, payload);
    connection.connect();

    try {
      // The status is read BEFORE the body: which stream carries it depends on the status, and asking for the error
      // stream of a successful response hands back null. Reading it first also means a wrong status is reported as
      // the mismatch it is, rather than as a NullPointerException on the stream that was not there.
      final int status = connection.getResponseCode();
      final String body = status < 400 ? readResponse(connection) : readError(connection);
      assertThat(status).as("HTTP status for `%s`; body: %s", sql, body).isEqualTo(expectedStatus);
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }
}
