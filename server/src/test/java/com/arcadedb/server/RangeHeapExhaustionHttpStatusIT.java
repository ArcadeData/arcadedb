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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for advisory GHSA-xmjm-8q85-g778, reported against the HTTP command API: an authenticated
 * user could submit {@code RETURN range(0, 9999999999)} and the server materialised the ten billion elements
 * into an ArrayList, exhausting the JVM heap with an OutOfMemoryError (HTTP 500) and degrading every other
 * request sharing the JVM.
 * <p>
 * The range is now lazy and a range longer than {@code arcadedb.queryMaxRangeSize} is refused before a single
 * element is produced, so the request fails fast with 400 Bad Request and no memory pressure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RangeHeapExhaustionHttpStatusIT extends BaseGraphServerTest {

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void oversizedRangeReturns400WithoutExhaustingHeap() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN range(0, 9999999999) AS v", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("range(").contains("arcadedb.queryMaxRangeSize");
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void oversizedRangeInsideUnwindReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "UNWIND range(0, 9999999999) AS i RETURN count(i) AS c", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
    });
  }

  /** The server must keep serving requests: the PoC checked recovery with a control query. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void serverKeepsServingAfterTheRejectedRequest() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "RETURN range(0, 9999999999) AS v", 400);
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 AS v", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getInt("v")).isEqualTo(1);
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void reasonableRangesStillWork() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "UNWIND range(1, 1000) AS i RETURN sum(i) AS s", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getLong("s")).isEqualTo(500500L);
      assertThat(executeCypher(serverIndex, "RETURN range(0, 4) AS v", 200).getJSONArray("result").getJSONObject(0)
          .getJSONArray("v").length()).isEqualTo(5);
    });
  }

  private JSONObject executeCypher(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "opencypher").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("expected HTTP %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
