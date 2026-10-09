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

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9554: a peer's forwarded command was refused with HTTP 401 "Invalid cluster token" around leader changes.
 * <p>
 * The HTTP gate checks the token through {@link HAServerPlugin#effectiveClusterToken}, which read it only from
 * {@code server.getHA()}. {@code RaftHAPlugin.startService()} registers itself there only once {@code raft.start()} has
 * returned, but the HTTP listener is up long before, and Ratis is running - and can be elected leader, so its peers
 * forward to it - inside {@code raft.start()}. In that window the gate fell back to the raw
 * {@code arcadedb.ha.clusterToken} setting, which is empty on every cluster that lets the token be derived, so the
 * peer's correct token was refused as invalid.
 * <p>
 * The window is reproduced on a running node by unregistering the HA plugin from the server, which leaves exactly
 * what the starting node has: a running plugin, discovered and registered with the plugin manager, that
 * {@code getHA()} does not name yet. The node's own background HA threads keep running meanwhile and may read
 * {@code getHA()} as null too - the readiness probe, for instance - which is the state a starting node is in anyway;
 * every test restores it in a {@code finally}.
 */
class Issue9554ClusterTokenAcrossHALifecycleIT extends BaseRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Test
  @Timeout(120)
  void aPeerForwardIsAcceptedBeforeTheHAPluginRegistersItselfOnTheServer() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String clusterToken = getRaftPlugin(0).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("the cluster declares no token, so the effective one is derived at startup").isNotBlank();

    final HAServerPlugin ha = server.getHA();
    assertThat(ha).isNotNull();
    try {
      server.setHA(null);

      assertThat(HAServerPlugin.effectiveClusterToken(server))
          .as("the token this node accepts must not depend on whether the HA plugin registered itself yet")
          .isEqualTo(clusterToken);

      final HttpResponse<String> response = forwardedCommand(0, clusterToken);
      assertThat(response.statusCode())
          .as("a peer's forward carrying the right cluster token must not be refused as 'Invalid cluster token', body: %s",
              response.body())
          .isEqualTo(200);
    } finally {
      server.setHA(ha);
    }
  }

  @Test
  @Timeout(120)
  void aWrongClusterTokenIsStillRefusedInTheSameWindow() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final HAServerPlugin ha = server.getHA();
    try {
      server.setHA(null);

      final HttpResponse<String> response = forwardedCommand(0, "issue9554-not-the-cluster-token");
      assertThat(response.statusCode()).isEqualTo(401);
      assertThat(response.body()).contains("Invalid cluster token");
    } finally {
      server.setHA(ha);
    }
  }

  /**
   * The snapshot route authenticates peers on its own rather than through the shared HTTP gate, and read the token the
   * same way: a follower asking a just-elected leader for a snapshot in the same window was refused 401.
   */
  @Test
  @Timeout(120)
  void aPeerSnapshotRequestIsAuthenticatedBeforeTheHAPluginRegistersItselfOnTheServer() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String clusterToken = getRaftPlugin(0).getRaftHAServer().getClusterToken();
    final HAServerPlugin ha = server.getHA();
    try {
      server.setHA(null);

      final HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create("http://127.0.0.1:" + server.getHttpServer().getPort() + "/api/v1/ha/snapshot/" + getDatabaseName()))
          .timeout(Duration.ofSeconds(60))
          .header("X-ArcadeDB-Cluster-Token", clusterToken)
          .GET()
          .build();
      final HttpResponse<Void> response = HTTP.send(request, HttpResponse.BodyHandlers.discarding());
      assertThat(response.statusCode())
          .as("a peer's snapshot request carrying the right cluster token must be authenticated and served")
          .isEqualTo(200);
    } finally {
      server.setHA(ha);
    }
  }

  /** The shape {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft} sends: the token and a forwarded user. */
  private HttpResponse<String> forwardedCommand(final int serverIndex, final String clusterToken) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/command/" + getDatabaseName()))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", "root")
        .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"SELECT 1\"}", StandardCharsets.UTF_8))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
