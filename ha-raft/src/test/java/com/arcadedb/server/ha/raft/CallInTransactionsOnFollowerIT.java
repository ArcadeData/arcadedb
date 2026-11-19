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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11588 on a follower. A session-less CALL { ... } IN TRANSACTIONS sent to a follower is a write, so
 * {@code RaftReplicatedDatabase.command} forwards it to the leader's {@code /api/v1/command}, where the HTTP command
 * handler runs it in the implicit transaction it opens around the request: it runs in batches and every node gets the
 * rows. Inside a {@code /begin} session on the follower it runs in the client's transaction there (issue #10040) and is
 * refused, so nothing reaches any node.
 */
@Tag("slow")
class CallInTransactionsOnFollowerIT extends BaseRaftHATest {
  private static final String STATEMENT = "UNWIND range(1, 25) AS i CALL { WITH i CREATE (:%s {i: i}) } IN TRANSACTIONS OF 10 ROWS";

  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aSessionlessCommandOnAFollowerIsForwardedAndRunsInBatchesOnTheLeader() throws Exception {
    final int follower = firstFollowerOf(requireLeaderIndex());
    final String label = "Forwarded11588";

    final HttpResponse<String> response = post(follower, "/api/v1/command/" + getDatabaseName(), cypher(STATEMENT.formatted(label)),
        null);
    assertThat(response.statusCode()).as("command: %s", response.body()).isEqualTo(200);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, label)).as("server %d", i).isEqualTo(25L);
  }

  @Test
  @Timeout(180)
  void insideASessionOnAFollowerItIsRefused() throws Exception {
    final int follower = firstFollowerOf(requireLeaderIndex());
    final String label = "Session11588";

    HttpResponse<String> response = post(follower, "/api/v1/begin/" + getDatabaseName(), "{}", null);
    assertThat(response.statusCode()).as("begin: %s", response.body()).isEqualTo(204);
    final String session = response.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(session).isNotBlank();

    response = post(follower, "/api/v1/command/" + getDatabaseName(), cypher(STATEMENT.formatted(label)), session);
    assertThat(response.statusCode()).as("command: %s", response.body()).isGreaterThanOrEqualTo(400);
    assertThat(response.body()).contains("can only be executed in an implicit transaction");
    // the failed command may already have ended the session's transaction: the outcome of this rollback is not the point
    post(follower, "/api/v1/rollback/" + getDatabaseName(), "{}", session);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, label)).as("server %d", i).isZero();
  }

  private static String cypher(final String command) {
    return new JSONObject().put("language", "opencypher").put("command", command).toString();
  }

  private long count(final int serverIndex, final String label) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private HttpResponse<String> post(final int serverIndex, final String path, final String body, final String session)
      throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(serverIndex, path)))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    if (session != null)
      request.header(HttpSessionManager.ARCADEDB_SESSION_ID, session);
    return HTTP_CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
