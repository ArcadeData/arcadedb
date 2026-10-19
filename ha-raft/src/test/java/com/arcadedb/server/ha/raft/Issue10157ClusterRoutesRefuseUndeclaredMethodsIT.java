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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10157: the HA plugin registered its routes on the raw {@code PathHandler}, which matches on
 * the path alone, so every cluster operation answered every HTTP method. {@code GET /api/v1/cluster/stepdown} dropped
 * leadership, {@code GET /api/v1/cluster/leave?force=true} removed the node from the Raft configuration and
 * {@code GET /api/v1/cluster/peer/<id>} evicted a peer - with the root credentials a browser replays on its own.
 * <p>
 * Driven against a live two-node cluster, as root, so the only thing that can stop each request is the method check:
 * every undeclared method must be answered 405 naming the declared one, and the cluster must come out exactly as it
 * went in - same leader, same term, same members.
 */
class Issue10157ClusterRoutesRefuseUndeclaredMethodsIT extends BaseRaftHATest {

  /** One concrete request path per HA route, and the one method the specification declares for it. */
  private static final Map<String, String> DECLARED = new LinkedHashMap<>() {{
    put("/api/v1/cluster", "GET");
    put("/api/v1/cluster/peer", "POST");
    put("/api/v1/cluster/peer/somePeer", "DELETE");
    put("/api/v1/cluster/leader", "POST");
    put("/api/v1/cluster/stepdown", "POST");
    put("/api/v1/cluster/leave", "POST");
    put("/api/v1/cluster/verify/graph", "POST");
    put("/api/v1/cluster/resync/graph", "POST");
    put("/api/v1/cluster/accept-copy/graph", "POST");
    put("/api/v1/cluster/accept-diverged/graph", "POST");
    put("/api/v1/cluster/accept-stale-snapshot", "POST");
    put("/api/v1/cluster/bootstrap-state", "POST");
    put("/api/v1/cluster/capabilities", "POST");
    put("/api/v1/cluster/security-seed", "POST");
    put("/api/v1/ha/snapshot/graph", "GET");
    put("/api/v1/ha/snapshot/graph/checksums", "GET");
  }};

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aGetCannotStepDownLeaveOrEvict() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final long termBefore = leaderRaft.getCurrentTerm();
    final int membersBefore = leaderRaft.getLivePeers().size();

    assertRefused(leader, "GET", "/api/v1/cluster/stepdown", "POST");
    assertRefused(follower, "GET", "/api/v1/cluster/leave?force=true", "POST");
    assertRefused(leader, "GET", "/api/v1/cluster/peer/" + peerIdForIndex(follower) + "?force=true", "DELETE");

    assertThat(findLeaderIndex()).as("a refused GET must not move the leadership").isEqualTo(leader);
    assertThat(leaderRaft.getCurrentTerm()).as("a refused GET must not start an election").isEqualTo(termBefore);
    assertThat(leaderRaft.getLivePeers()).as("a refused GET must not change the membership").hasSize(membersBefore);
    assertThat(getRaftPlugin(follower).getRaftHAServer().isRemovedFromConfiguration())
        .as("the follower the GET asked to leave is still a member").isFalse();
  }

  @Test
  void everyHaRouteRefusesAMethodItDoesNotDeclare() throws Exception {
    final int follower = firstFollowerOf(requireLeaderIndex());
    // A follower, so that a request the check let through could not take the cluster's leadership with it
    for (final Map.Entry<String, String> route : DECLARED.entrySet()) {
      final String undeclared = "GET".equals(route.getValue()) ? "POST" : "GET";
      assertRefused(follower, undeclared, route.getKey(), route.getValue());
      assertRefused(follower, "PUT", route.getKey(), route.getValue());
    }
  }

  @Test
  void theDeclaredMethodStillReachesTheHandler() throws Exception {
    final int leader = requireLeaderIndex();
    assertThat(call(leader, "GET", "/api/v1/cluster").status).as("GET /api/v1/cluster").isEqualTo(200);
    // An empty add-peer body passes the root check and fails validation inside the handler: the handler ran
    assertThat(call(leader, "POST", "/api/v1/cluster/peer").status).as("POST /api/v1/cluster/peer").isEqualTo(400);
  }

  @Test
  void noShippedPluginRegistersARouteThatAnswersEveryMethod() {
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).getHttpServer().getUnrestrictedPluginRoutes())
          .as("server %d: plugin routes registered on the raw PathHandler answer every method", i).isEmpty();
  }

  private void assertRefused(final int serverIndex, final String method, final String path, final String allowed)
      throws Exception {
    final Response response = call(serverIndex, method, path);
    assertThat(response.status).as("%s %s", method, path).isEqualTo(405);
    // A route declared GET also answers HEAD, and the Allow header says so (issue #10384)
    assertThat(response.allow).as("Allow header of %s %s", method, path).isEqualTo("GET".equals(allowed) ? "GET, HEAD" : allowed);
  }

  private record Response(int status, String allow) {
  }

  private Response call(final int serverIndex, final String method, final String path) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI("http://localhost:" + port + path).toURL().openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      if ("POST".equals(method) || "PUT".equals(method)) {
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.getOutputStream().write("{}".getBytes(StandardCharsets.UTF_8));
      }
      return new Response(conn.getResponseCode(), conn.getHeaderField("Allow"));
    } finally {
      conn.disconnect();
    }
  }
}
