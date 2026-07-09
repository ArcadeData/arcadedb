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
 * End-to-end regression for issue #5023. The idempotency cache used to key solely on the client
 * {@code X-Request-Id}, so a second, DIFFERENT write that happened to reuse the same correlation id had
 * the first request's response replayed and its write silently skipped. It also must still de-duplicate a
 * genuine retry of the SAME request so a double-commit does not occur.
 * <p>
 * Assertions compare the response bodies (which carry the inserted record @rid) rather than reading back
 * through the embedded database, so they are not subject to HTTP-commit / embedded-read visibility timing.
 * A distinct INSERT returns a distinct @rid, so a replayed response is byte-identical to the original.
 */
class Issue5023IdempotencyKeyReplayTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  void sameRequestIdDifferentCommandStillExecutes() throws Exception {
    // Uses the pre-existing "Person" type (created in BaseGraphServerTest.populateDatabase).
    testEachServer(serverIndex -> {
      // First write with X-Request-Id: abc
      final String body1 = postCommand(serverIndex, "abc", "INSERT INTO Person SET marker = 'idem5023', n = 1");

      // Second write reuses the SAME X-Request-Id but is a DIFFERENT command. Before the fix this replayed
      // the first response (body-identical) and never executed the second write.
      final String body2 = postCommand(serverIndex, "abc", "INSERT INTO Person SET marker = 'idem5023', n = 2");

      // Both executed independently -> distinct inserted records -> distinct response bodies.
      assertThat(body2).isNotEqualTo(body1);
      assertThat(body1).contains("\"n\":1");
      assertThat(body2).contains("\"n\":2");
    });
  }

  @Test
  void sameRequestIdSameCommandIsDeduplicated() throws Exception {
    testEachServer(serverIndex -> {
      // Two identical retries of the same logical request (same id, same body): the second call must replay
      // the cached response instead of executing a second insert, so the bodies (including the @rid) match.
      final String body1 = postCommand(serverIndex, "retry-1", "INSERT INTO Person SET marker = 'idem5023dup'");
      final String body2 = postCommand(serverIndex, "retry-1", "INSERT INTO Person SET marker = 'idem5023dup'");

      assertThat(body2).isEqualTo(body1);
    });
  }

  private String postCommand(final int serverIndex, final String requestId, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty(IdempotencyCache.HEADER_REQUEST_ID, requestId);

    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", command);
    payload.put("autoCommit", true);
    formatPayload(connection, payload);
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return response;
    } finally {
      connection.disconnect();
    }
  }
}
