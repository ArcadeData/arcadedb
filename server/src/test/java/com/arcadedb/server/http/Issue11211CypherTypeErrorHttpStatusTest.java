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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11211 over HTTP, the surface it was reported from: {@code r.weight} on a variable-length relationship answered
 * 500 "Cannot execute command" from both the command and the query endpoint. It is the caller's error, refused before
 * the query runs as Neo4j refuses it; and a type error met only while the rows are serialized is the caller's too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11211CypherTypeErrorHttpStatusTest extends BaseGraphServerTest {

  @Test
  void propertyAccessOnAVariableLengthRelationshipIsAClientError() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "opencypher", "CREATE (:N11211 {id: 1})-[:R11211 {weight: 7}]->(:N11211 {id: 2})");

      for (final String endpoint : new String[] { "command", "query" }) {
        final JSONObject error = post(serverIndex, endpoint, 400,
            "MATCH (n:N11211)<-[r:R11211*1..3]-(m:N11211) RETURN r.weight AS w");
        assertThat(error.getString("exception")).as(endpoint).isEqualTo(CommandSemanticException.class.getName());
        assertThat(error.getString("detail")).as(endpoint).contains("List<Relationship>");
      }

      // the control: the relationship list itself, and a fixed-length relationship's property, still answer 200
      post(serverIndex, "query", 200, "MATCH (n:N11211)<-[r:R11211*1..3]-(m:N11211) RETURN size(r) AS s");
      post(serverIndex, "query", 200, "MATCH (n:N11211)<-[r:R11211]-(m:N11211) RETURN r.weight AS w");
    });
  }

  @Test
  void aTypeErrorMetWhileTheRowsAreSerializedIsAClientError() throws Exception {
    testEachServer(serverIndex -> {
      for (final String endpoint : new String[] { "command", "query" }) {
        final JSONObject error = post(serverIndex, endpoint, 400, "UNWIND [[1, 2]] AS l RETURN l.x AS v");
        assertThat(error.getString("exception")).as(endpoint).isEqualTo(TypeMismatchException.class.getName());
        assertThat(error.getString("detail")).as(endpoint).contains("TypeError: Cannot access property 'x'");
        assertThat(error.getString("error")).as(endpoint).isEqualTo("Cannot execute command");
      }
    });
  }

  /** Posts a Cypher statement to an endpoint, asserts the HTTP status, and returns the parsed body. */
  private JSONObject post(final int serverIndex, final String endpoint, final int expectedStatus, final String command)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        getServerHttpUrl(serverIndex, "/api/v1/" + endpoint + "/graph")).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", "opencypher");
    payload.put("command", command);
    formatPayload(connection, payload);
    connection.connect();

    try {
      final int status = connection.getResponseCode();
      final String body = status < 400 ? readResponse(connection) : readError(connection);
      assertThat(status).as("HTTP status of /%s for `%s`; body: %s", endpoint, command, body).isEqualTo(expectedStatus);
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }
}
