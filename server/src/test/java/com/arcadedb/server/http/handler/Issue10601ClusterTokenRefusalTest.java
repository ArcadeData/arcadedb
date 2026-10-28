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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10601: a request refused for its cluster token is answered 401 "Invalid cluster token" - the string issue #9554
 * taught operators to read as a transient leadership race. The answer now says that the caller's token does not match
 * this node's, and the refusal is handed to the HA plugin, which logs where this node's token came from (and, for a
 * token derived from a since-rewritten root password file, that the rewrite is the cause). Neither the cluster name nor
 * the file path goes back to the unauthenticated caller.
 */
class Issue10601ClusterTokenRefusalTest extends BaseGraphServerTest {

  private static final String     CLUSTER_TOKEN = "issue10601-cluster-secret-token";
  private static final HttpClient HTTP          = HttpClient.newHttpClient();

  private final RecordingHA ha = new RecordingHA();

  @BeforeEach
  void installHA() {
    getServer(0).setHA(ha);
  }

  @AfterEach
  void removeHA() {
    getServer(0).setHA(null);
  }

  @Test
  void aMismatchedClusterTokenIsAnsweredWithWhichSideFailedAndReportedToTheHAPlugin() throws Exception {
    final HttpResponse<String> response = command("not-the-cluster-token");

    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.body()).contains("Invalid cluster token");
    assertThat(response.body()).contains("the caller's cluster token does not match this node's");
    assertThat(response.body()).doesNotContain(CLUSTER_TOKEN);
    assertThat(ha.refusedCallers).hasSize(1);
    assertThat(ha.refusedCallers.getFirst()).isNotBlank();
  }

  @Test
  void aMatchingClusterTokenIsNotReportedAsRefused() throws Exception {
    final HttpResponse<String> response = command(CLUSTER_TOKEN);

    // No forwarded user and no credentials: refused for identity, which is not a token mismatch.
    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.body()).doesNotContain("Invalid cluster token");
    assertThat(ha.refusedCallers).isEmpty();
  }

  private HttpResponse<String> command(final String token) throws Exception {
    return HTTP.send(HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/command/" + getDatabaseName()))
            .header("Content-Type", "application/json")
            .header("X-ArcadeDB-Cluster-Token", token)
            .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"SELECT 1\"}", StandardCharsets.UTF_8))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  /** An HA plugin reduced to a cluster token and a record of the refusals reported to it. */
  private static final class RecordingHA implements HAServerPlugin {
    final List<String> refusedCallers = new CopyOnWriteArrayList<>();

    @Override
    public String getClusterToken() {
      return CLUSTER_TOKEN;
    }

    @Override
    public void onClusterTokenRefused(final String caller) {
      refusedCallers.add(caller);
    }

    @Override
    public boolean isLeader() {
      return true;
    }

    @Override
    public String getLeaderName() {
      return null;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public void startService() {
    }

    @Override
    public String getClusterName() {
      return "issue10601";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 1;
    }

    @Override
    public String getLeaderAddress() {
      return null;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }
}
