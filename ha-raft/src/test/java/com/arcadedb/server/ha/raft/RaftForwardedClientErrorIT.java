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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12931: a statement of the client's own that the leader refuses as a client error is answered by a follower exactly as the
 * leader answers it. The follower forwards every write, and rebuilt the leader's refusal only for the classes the forward path
 * listed one issue at a time: any other came back as a plain {@code TransactionException}, answered 500 "Error on transaction
 * commit" for a taken name (409), a value that does not convert or divides by zero (400) and a record that does not exist (404).
 */
class RaftForwardedClientErrorIT extends BaseRaftHATest {
  private static final String   DOCUMENT_TYPE = "ClientErrorDoc";
  private static final String   VERTEX_TYPE   = "ClientErrorVertex";
  private static final String   EDGE_TYPE     = "ClientErrorEdge";
  private static final Duration HTTP_TIMEOUT  = Duration.ofSeconds(60);

  /** The refusals a follower used to answer 500, then the ones it always rebuilt, as controls: language, statement, status. */
  private static final String[][] REFUSALS = {
      { "sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE, "409" },
      { "sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".id INTEGER", "409" },
      { "sql", "CREATE INDEX ON " + DOCUMENT_TYPE + " (id) UNIQUE", "409" },
      { "sql", "INSERT INTO " + DOCUMENT_TYPE + " SET id = 4, n = 'abc'", "400" },
      { "sql", "INSERT INTO " + DOCUMENT_TYPE + " SET id = 5, dt = 'not a date'", "400" },
      { "sql", "UPDATE " + DOCUMENT_TYPE + " SET id = 1 / 0", "400" },
      { "sql", "ALTER DATABASE `arcadedb.noSuchSetting` 5", "400" },
      { "sql", "CREATE EDGE " + EDGE_TYPE + " FROM #99:99 TO #98:98", "404" },
      { "opencypher", "CREATE (n:ClientErrorQ {a: 1 / 0})", "400" },
      { "opencypher", "MATCH (n:" + VERTEX_TYPE + ") SET n.x = [1, 2] + {a: 1}", "400" },
      { "opencypher", "CALL no.such.procedure()", "400" },
      { "opencypher", "CREATE INDEX FOR (n:" + DOCUMENT_TYPE + ") ON (n.id)", "400" },
      // already rebuilt before: the answers must keep matching
      { "sql", "INSERT INTO " + DOCUMENT_TYPE + " SET id = 1", "409" },
      { "sql", "INSERT INTO " + DOCUMENT_TYPE + " SET id = 6, s = 'too long a value'", "400" },
      { "sql", "UPDATE NoSuchClientErrorType SET a = 1", "400" },
      { "sql", "INSERT INTO " + DOCUMENT_TYPE + " SET id = 7, x = nosuchfunction(1)", "400" } };

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aFollowerAnswersAClientErrorAsTheLeaderDoes() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    leader.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE).close();
    leader.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".id INTEGER").close();
    leader.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".s STRING (MAX 5)").close();
    leader.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".n INTEGER").close();
    leader.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".dt DATETIME").close();
    leader.command("sql", "CREATE INDEX ON " + DOCUMENT_TYPE + " (id) UNIQUE").close();
    leader.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE).close();
    leader.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE).close();
    leader.command("sql", "INSERT INTO " + DOCUMENT_TYPE + " SET id = 1").close();
    leader.command("sql", "CREATE VERTEX " + VERTEX_TYPE).close();
    waitForAllServers();

    final HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    try {
      for (final String[] refusal : REFUSALS) {
        final String what = refusal[0] + ": " + refusal[1];
        final HttpResponse<String> onTheLeader = send(client, leaderIndex, refusal[0], refusal[1]);
        assertThat(onTheLeader.statusCode()).as("the leader's own answer to %s: %s", what, onTheLeader.body())
            .isEqualTo(Integer.parseInt(refusal[2]));

        final HttpResponse<String> onTheFollower = send(client, followerIndex, refusal[0], refusal[1]);
        assertThat(onTheFollower.statusCode()).as("the follower's answer to %s: %s", what, onTheFollower.body())
            .isEqualTo(onTheLeader.statusCode());
        final JSONObject leaderBody = new JSONObject(onTheLeader.body());
        final JSONObject followerBody = new JSONObject(onTheFollower.body());
        assertThat(followerBody.getString("exception", "")).as("exception class of %s", what)
            .isEqualTo(leaderBody.getString("exception", ""));
        assertThat(followerBody.getString("error", "")).as("label of %s", what).isEqualTo(leaderBody.getString("error", ""));
      }
    } finally {
      client.close();
    }
  }

  /** The statement posted to the {@code /command} endpoint of the server {@code serverIndex}, as the root user. */
  private HttpResponse<String> send(final HttpClient client, final int serverIndex, final String language, final String command)
      throws Exception {
    return client.send(HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/" + getDatabaseName()))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", language).put("command", command).toString()))
        .build(), HttpResponse.BodyHandlers.ofString());
  }
}
