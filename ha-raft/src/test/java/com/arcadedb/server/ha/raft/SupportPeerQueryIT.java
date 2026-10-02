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

import com.arcadedb.serializer.json.JSONArray;
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
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cluster version of a support request on a real 3-node cluster: the node Studio talks to asks the OTHER two, over the
 * cluster channel, and each peer's own engine is the read-only gate. A statement that writes is refused by every peer and
 * changes nothing anywhere.
 */
class SupportPeerQueryIT extends BaseRaftHATest {
  private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  private JSONObject call(final int server, final String method, final String path, final String body) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(server).getHttpServer().getPort() + path))
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .timeout(Duration.ofSeconds(60));
    if (body == null)
      request.method(method, HttpRequest.BodyPublishers.noBody());
    else
      request.method(method, HttpRequest.BodyPublishers.ofString(body));
    final HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(method + " " + path + " -> " + response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private static String query(final String statement, final String nodes, final String language, final String db) {
    return new JSONObject().put("database", db).put("language", language).put("statement", statement).put("nodes", nodes).toString();
  }

  @Test
  void aReadOnlyStatementAsksEveryOtherNodeAndAWriteIsRefusedByEach() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int asked = (leader + 1) % 3;
    final String db = getDatabaseName();

    final JSONObject peers = call(asked, "GET", "/api/v1/server/support/peers", null);
    assertThat(peers.getBoolean("ha")).isTrue();
    assertThat(peers.getJSONArray("peers").length()).isEqualTo(2);
    assertThat(peers.toString()).doesNotContain("127.0.0.1");

    final JSONObject all = call(asked, "POST", "/api/v1/server/support/peer-query",
        query("SELECT count(*) AS c FROM V1", "all", "sql", db));
    assertThat(all.getBoolean("ha")).isTrue();
    final JSONArray nodes = all.getJSONArray("nodes");
    assertThat(nodes.length()).isEqualTo(2);
    final Set<String> names = new HashSet<>();
    for (int i = 0; i < nodes.length(); i++) {
      final JSONObject node = nodes.getJSONObject(i);
      assertThat(node.getString("status")).as(node.toString()).isEqualTo("ok");
      assertThat(node.getJSONArray("records").length()).isEqualTo(1);
      names.add(node.getString("node"));
    }
    assertThat(names).isEqualTo(new HashSet<>(peers.getJSONArray("peers").toList().stream().map(Object::toString).toList()));

    final String one = peers.getJSONArray("peers").getString(0);
    final JSONObject named = call(asked, "POST", "/api/v1/server/support/peer-query",
        query("MATCH (n:V1) RETURN count(n) AS c", one, "opencypher", db));
    assertThat(named.getJSONArray("nodes").length()).isEqualTo(1);
    assertThat(named.getJSONArray("nodes").getJSONObject(0).getString("node")).isEqualTo(one);
    assertThat(named.getJSONArray("nodes").getJSONObject(0).getString("status")).isEqualTo("ok");

    // A write is not run anywhere: each peer's own engine refuses it on the idempotent query endpoint
    final long before = getServerDatabase(0, db).countType("V1", true);
    final JSONObject write = call(asked, "POST", "/api/v1/server/support/peer-query",
        query("INSERT INTO V1 SET id = 99999, name = 'x'", "all", "sql", db));
    final JSONArray refused = write.getJSONArray("nodes");
    assertThat(refused.length()).isEqualTo(2);
    for (int i = 0; i < refused.length(); i++)
      assertThat(refused.getJSONObject(i).getString("status")).as(refused.getJSONObject(i).toString()).isEqualTo("failed");
    waitForReplicationIsCompleted(0);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, db).countType("V1", true)).as("server " + i).isEqualTo(before);

    // A name that is not a member is a failed row and nothing is dialled
    final JSONObject unknown = call(asked, "POST", "/api/v1/server/support/peer-query",
        query("SELECT 1", "evil.example.com:80", "sql", db));
    assertThat(unknown.getJSONArray("nodes").getJSONObject(0).getString("status")).isEqualTo("failed");
  }
}
