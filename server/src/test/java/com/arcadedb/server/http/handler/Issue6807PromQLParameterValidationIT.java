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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for the {@code start}/{@code end}/{@code step}/{@code time} validation of the PromQL
 * endpoints (issue #6807). The unit tests pin the parser; these pin the <i>handler</i> contract - that a
 * rejected parameter reaches the client as a 400 with a {@code bad_data} body rather than as a 500, or (for
 * the overflowing range) rather than as a request that never returns at all.
 * <p>
 * The {@code @Timeout} is a hang detector, not a latency bound: before the fix the first case below wedged
 * the Undertow worker thread serving it for the life of the process, so the failure mode being guarded is
 * "no response ever", not "slow response".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6807PromQLParameterValidationIT extends BaseGraphServerTest {

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aRangeWiderThanTheRepresentableSpanIsRejectedInsteadOfWedgingTheWorker() throws Exception {
    testEachServer(serverIndex -> {
      // The exact repro from the issue. endMs - startMs overflowed to a negative number, the MAX_RANGE_STEPS
      // guard passed, and the per-step loop ran forever on a worker thread with no timeout and no
      // cancellation. One request per worker thread took the HTTP listener down.
      assertBadData(serverIndex, "/query_range?query=up&start=-9e15&end=9e15&step=60");
    });
  }

  @Test
  void aMalformedOrOutOfRangeRangeParameterIsAnswered400() throws Exception {
    testEachServer(serverIndex -> {
      assertBadData(serverIndex, "/query_range?query=up&start=yesterday&end=1700000060&step=60");
      assertBadData(serverIndex, "/query_range?query=up&start=1700000000&end=NaN&step=60");
      assertBadData(serverIndex, "/query_range?query=up&start=1700000000&end=9e15&step=60");
      // Infinity and 1e300 both survive Double.parseDouble and saturate to Long.MAX_VALUE, which is
      // positive - so they used to pass the "step must be positive" test and yield a one-point 200.
      assertBadData(serverIndex, "/query_range?query=up&start=1700000000&end=1700000600&step=Infinity");
      assertBadData(serverIndex, "/query_range?query=up&start=1700000000&end=1700000600&step=1e300");
      // The pre-existing guards must keep answering 400 too.
      assertBadData(serverIndex, "/query_range?query=up&start=1700000600&end=1700000000&step=60");
      assertBadData(serverIndex, "/query_range?query=up&start=1700000000&end=1700000600&step=0");
      assertBadData(serverIndex, "/query_range?query=up&start=0&end=2000000000&step=0.001");
    });
  }

  @Test
  void aMalformedInstantTimeParameterIsAnswered400RatherThan500() throws Exception {
    testEachServer(serverIndex -> {
      // The instant endpoint parsed "time" with a bare Double.parseDouble outside its own catch, so a
      // non-numeric value surfaced as a 500.
      assertBadData(serverIndex, "/query?query=up&time=now");
      assertBadData(serverIndex, "/query?query=up&time=Infinity");
      assertBadData(serverIndex, "/query?query=up&time=9e15");
    });
  }

  @Test
  void wellFormedParametersStillSucceed() throws Exception {
    testEachServer(serverIndex -> {
      // Control: the validation must not reject a legitimate request. The metric does not exist, so the
      // evaluator answers an empty result - a 200 either way.
      assertThat(get(serverIndex, "/query_range?query=up&start=1700000000&end=1700000600&step=60"))
          .isEqualTo(200);
      assertThat(get(serverIndex, "/query_range?query=up&start=1700000000&end=1700000600&step=1m"))
          .isEqualTo(200);
      assertThat(get(serverIndex, "/query?query=up&time=1700000000")).isEqualTo(200);
      assertThat(get(serverIndex, "/query?query=up")).isEqualTo(200);
    });
  }

  private void assertBadData(final int serverIndex, final String query) throws Exception {
    final HttpURLConnection connection = open(serverIndex, query);
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).as("status of %s", query).isEqualTo(400);
      final JSONObject body = new JSONObject(readBody(connection));
      assertThat(body.getString("status")).isEqualTo("error");
      assertThat(body.getString("errorType")).as("errorType of %s", query).isEqualTo("bad_data");
      assertThat(body.getString("error", "")).as("error message of %s", query).isNotBlank();
    } finally {
      connection.disconnect();
    }
  }

  private int get(final int serverIndex, final String query) throws Exception {
    final HttpURLConnection connection = open(serverIndex, query);
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String query) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1"
            + query).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    return connection;
  }

  private static String readBody(final HttpURLConnection connection) throws Exception {
    final InputStream stream = connection.getErrorStream() != null
        ? connection.getErrorStream()
        : connection.getInputStream();
    return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
  }
}
