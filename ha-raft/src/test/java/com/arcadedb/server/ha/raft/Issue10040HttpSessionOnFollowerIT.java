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
 * Issue #10040 over HTTP: a write a client sends to a follower inside the session transaction it opened with
 * {@code /begin} used to be forwarded to the leader and committed there on its own, so {@code /rollback} could not undo
 * it. It now runs in the session's transaction on the follower, and {@code /commit} replicates it through Raft.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10040HttpSessionOnFollowerIT extends BaseRaftHATest {
  private static final String TYPE = "Issue10040Http";

  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aSessionRollbackOnAFollowerUndoesItsWritesAndACommitPublishesThem() throws Exception {
    final int leader = requireLeaderIndex();
    getServerDatabase(leader, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    waitForAllServers();
    final int follower = firstFollowerOf(leader);

    // ROLLBACK
    String session = begin(follower);
    command(follower, session, "INSERT INTO " + TYPE + " SET name = 'rb'");
    command(follower, session, "INSERT INTO " + TYPE + " SET name = 'rb'");
    assertThat(count(leader, "rb")).as("nothing reaches the leader before the session ends").isZero();
    end(follower, session, "rollback");

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "rb")).as("server %d must hold no row of the rolled back session", i).isZero();

    // COMMIT
    session = begin(follower);
    command(follower, session, "INSERT INTO " + TYPE + " SET name = 'ok'");
    command(follower, session, "INSERT INTO " + TYPE + " SET name = 'ok'");
    end(follower, session, "commit");

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "ok")).as("server %d must hold the committed session", i).isEqualTo(2L);
  }

  private long count(final int serverIndex, final String name) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE name = ?", name)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String begin(final int serverIndex) throws Exception {
    final HttpResponse<String> response = post(serverIndex, "/api/v1/begin/" + getDatabaseName(), "{}", null);
    assertThat(response.statusCode()).as("begin: %s", response.body()).isEqualTo(204);
    final String session = response.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(session).as("the follower must open a session").isNotBlank();
    return session;
  }

  private void command(final int serverIndex, final String session, final String sql) throws Exception {
    final String body = new JSONObject().put("language", "sql").put("command", sql).toString();
    final HttpResponse<String> response = post(serverIndex, "/api/v1/command/" + getDatabaseName(), body, session);
    assertThat(response.statusCode()).as("command: %s", response.body()).isEqualTo(200);
  }

  private void end(final int serverIndex, final String session, final String verb) throws Exception {
    final HttpResponse<String> response = post(serverIndex, "/api/v1/" + verb + "/" + getDatabaseName(), "{}", session);
    assertThat(response.statusCode()).as("%s: %s", verb, response.body()).isEqualTo(204);
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
