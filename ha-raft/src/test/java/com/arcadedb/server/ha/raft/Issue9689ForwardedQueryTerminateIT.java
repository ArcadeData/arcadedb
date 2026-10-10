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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9689: a write sent to a replica runs on the leader, and terminating it on the replica - the node the client
 * talks to, whose {@code list queries} the client reads - stops the work on the leader. The replica lists its entry as
 * forwarded to the leader; the leader's entry carries the replica's id and the client's tag, and is found by either id.
 * Before, a terminate on the replica stopped nothing: the replica's thread only waits for the leader, and the leader
 * ran the statement to its end, committing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue9689ForwardedQueryTerminateIT extends BaseRaftHATest {
  private static final String TAG = "ha-9689";
  /** About 16 s on one core when left alone, then a write: forwarded to the leader, where it runs. */
  private static final String LONG_WRITE =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j WITH sum(sin(toFloat(i * j))) AS total "
          + "CREATE (:Written9689 {total: total})";

  private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void terminatingAForwardedWriteOnTheReplicaStopsItOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replica = leader == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    if (!leaderDb.getSchema().existsType("Written9689"))
      leaderDb.command("sql", "CREATE VERTEX TYPE Written9689 IF NOT EXISTS");
    await().atMost(30, TimeUnit.SECONDS).until(() -> getServerDatabase(replica, getDatabaseName()).getSchema().existsType("Written9689"));

    final CompletableFuture<HttpResponse<String>> running = HTTP.sendAsync(HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(replica) + "/api/v1/command/" + getDatabaseName())).timeout(Duration.ofSeconds(120))
        .header("Content-Type", "application/json").header("Authorization", basic()).header("X-ArcadeDB-Query-Tag", TAG)
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "opencypher").put("command", LONG_WRITE).toString(), StandardCharsets.UTF_8))
        .build(), HttpResponse.BodyHandlers.ofString());

    // The replica lists the statement as forwarded to the leader...
    final JSONObject onReplica = awaitSingle(replica, "forwardedTo");
    // ...and the leader lists its own entry for it, with the replica's id and the client's tag
    final JSONObject onLeader = awaitSingle(leader, "forwardedFrom");
    assertThat(onLeader.getString("forwardedFrom", null)).isEqualTo(onReplica.getString("id"));
    assertThat(onLeader.getString("tag")).isEqualTo(TAG);

    final JSONObject terminated = serverCommand(replica, "terminate query " + onReplica.getString("id")).getJSONObject("result");
    assertThat(terminated.getString("status")).as("the terminate on the replica: %s", terminated).isEqualTo("terminated");

    final HttpResponse<String> response = running.get(60, TimeUnit.SECONDS);
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(409);

    // The work stopped on the leader, and wrote nothing
    await().atMost(30, TimeUnit.SECONDS).until(() -> listQueries(leader).length() == 0);
    assertThat(listQueries(replica).length()).isZero();
    assertThat(leaderDb.countType("Written9689", false)).isZero();
  }

  /** The one statement {@code server} lists under the tag, once it carries {@code property}. */
  private JSONObject awaitSingle(final int server, final String property) {
    final JSONObject[] found = new JSONObject[1];
    await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(50)).until(() -> {
      final JSONArray list = listQueries(server);
      if (list.length() == 1 && list.getJSONObject(0).has(property)) {
        found[0] = list.getJSONObject(0);
        return true;
      }
      return false;
    });
    return found[0];
  }

  private JSONArray listQueries(final int server) throws Exception {
    return serverCommand(server, "list queries tag " + TAG).getJSONArray("result");
  }

  private JSONObject serverCommand(final int server, final String command) throws Exception {
    final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(server) + "/api/v1/server")).timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json").header("Authorization", basic())
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString(), StandardCharsets.UTF_8))
        .build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("%s: %s", command, response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private String baseUrl(final int server) {
    return "http://127.0.0.1:" + getServer(server).getHttpServer().getPort();
  }

  private static String basic() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
