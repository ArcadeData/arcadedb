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
 * Issue #5898 over HTTP, which is the surface it was reported from. Dereferencing a Cypher property whose stored
 * LINK no longer resolves used to let a {@code RecordNotFoundException} out of the query engine; the auto-commit
 * wrapper in {@code DatabaseAbstractHandler} re-throws it inside a {@code TransactionException}, and
 * {@code AbstractServerHttpHandler} - which classifies by {@code getCause()} - has no arm for it, so the client was
 * told "Error on transaction commit" and learned nothing about what actually failed.
 * <p>
 * What this pins is the message, not the status: a Cypher {@code TypeError} is a 500 by deliberate design (the
 * #5219 rationale in {@code AbstractServerHttpHandler}, matching TinkerPop's {@code SERVER_ERROR_SCRIPT_EVALUATION}),
 * and this failure must be classified as one of those rather than as the catch-all commit failure. Asserting the
 * status alone would pass on the very bug being fixed, since both shapes answer 500.
 */
class Issue5898BrokenLinkPropertyAccessHttpTest extends BaseGraphServerTest {

  @Test
  void aDanglingLinkIsReportedAsACypherTypeError() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "opencypher",
          "CREATE (holder:LinkHolder {role: 'holder'}), (target:LinkHolder {role: 'target', id: 42}) "
              + "SET holder.ref = target");

      // Deleted in its own request, so the read below meets a genuinely unresolvable RID rather than the
      // in-transaction deleted-entity marker.
      executeCommand(serverIndex, "sql", "DELETE FROM LinkHolder WHERE role = 'target'");

      final JSONObject error = commandExpecting(serverIndex, 500, "opencypher",
          "MATCH (holder:LinkHolder {role: 'holder'}) RETURN holder.ref.id AS referencedId");

      assertThat(error.getString("detail"))
          .as("the client is told which property could not be read, and through which link")
          .contains("TypeError: Cannot access property 'id'");
      assertThat(error.getString("error"))
          .as("classified as a failed command, not as a failed commit")
          .isEqualTo("Cannot execute command");
    });
  }

  /**
   * The control: a link that still resolves must keep answering 200 with the referenced value, so the test above
   * cannot pass by breaking every dereference.
   */
  @Test
  void aLiveLinkStillDereferences() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "opencypher",
          "CREATE (holder:LiveLinkHolder {role: 'holder'}), (target:LiveLinkHolder {role: 'target', id: 7}) "
              + "SET holder.ref = target");

      final JSONObject response = executeCommand(serverIndex, "opencypher",
          "MATCH (holder:LiveLinkHolder {role: 'holder'}) RETURN holder.ref.id AS referencedId");

      // The Cypher engine answers a records/columns envelope rather than the flat array SQL returns.
      final JSONObject result = response.getJSONObject("result");
      assertThat(result.getJSONArray("records").getJSONObject(0).getInt("referencedId")).isEqualTo(7);
    });
  }

  /** Posts a command, asserts the HTTP status, and returns the parsed body (the error body on a failure). */
  private JSONObject commandExpecting(final int serverIndex, final int expectedStatus, final String language,
      final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", command);
    formatPayload(connection, payload);
    connection.connect();

    try {
      // The status is read BEFORE the body: which stream carries it depends on the status, and asking for the error
      // stream of a successful response hands back null.
      final int status = connection.getResponseCode();
      final String body = status < 400 ? readResponse(connection) : readError(connection);
      assertThat(status).as("HTTP status for `%s`; body: %s", command, body).isEqualTo(expectedStatus);
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }
}
