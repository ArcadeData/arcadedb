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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end HTTP regression test for issue #5225: a range Cypher predicate against a String bound on an
 * index whose key type was inferred as numeric (e.g. {@code WHERE n.val < "zzz"}) used to return HTTP 500
 * with an internal {@code ClassCastException} ("String cannot be cast to Integer") or a
 * {@code NumberFormatException}. Cross-type comparisons are undefined (null) in Cypher, so the query must
 * complete (HTTP 200) and return the same result as the un-indexed full-scan path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5225MixedTypeIndexIT extends BaseGraphServerTest {

  private JSONObject cypher(final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "cypher").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      final int status = connection.getResponseCode();
      final InputStream is = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String response = new String(is.readAllBytes());
      assertThat(status).as("command [%s] expected HTTP %d but got %d (body=%s)", command, expectedStatus, status, response)
          .isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  private long count(final String command) throws Exception {
    final JSONArray result = cypher(command, 200).getJSONArray("result");
    assertThat(result.length()).isEqualTo(1);
    return result.getJSONObject(0).getLong("c");
  }

  @Test
  void mixedTypeRangeDoesNotReturn500() throws Exception {
    cypher("CREATE (:BugIdx {id: 1, val: 1})", 200);
    cypher("CREATE (:BugIdx {id: 2, val: 2})", 200);
    cypher("CREATE (:BugIdx {id: 3})", 200);
    cypher("CREATE INDEX FOR (n:BugIdx) ON (n.val)", 200);
    cypher("MATCH (n:BugIdx) WHERE n.id = 1 SET n.val = \"1\" RETURN n.val", 200);

    // Strict-typed equality on the coerced integer value returns no rows (Neo4j: 1 = "1" is false).
    assertThat(cypher("MATCH (n:BugIdx) WHERE n.val = \"1\" RETURN n.id, n.val", 200).getJSONArray("result").length())
        .isZero();

    // Range predicates against a String bound must complete (no 500), matching the un-indexed result of 0.
    assertThat(count("MATCH (n:BugIdx) WHERE n.val < \"zzz\" RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:BugIdx) WHERE n.val > \"a\" RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:BugIdx) WHERE n.val >= \"a\" AND n.val <= \"zzz\" RETURN count(n) AS c")).isZero();

    // A correctly-typed numeric range still uses the index and returns the right rows.
    assertThat(count("MATCH (n:BugIdx) WHERE n.val >= 1 AND n.val <= 5 RETURN count(n) AS c")).isEqualTo(2);
  }
}
