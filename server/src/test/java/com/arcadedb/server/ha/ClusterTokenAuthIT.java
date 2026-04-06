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
package com.arcadedb.server.ha;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests cluster-internal token authentication used for inter-node HTTP forwarding.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class ClusterTokenAuthIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void validClusterTokenWithKnownUserIsAccepted() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String clusterToken = server.getHA().getClusterToken();
    assertThat(clusterToken).isNotNull().isNotEmpty();

    // Send a request using cluster-internal auth headers (simulating inter-node forwarding)
    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.getHttpServer().getPort() + "/api/v1/server"))
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", "root")
        .GET().build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void invalidClusterTokenIsRejected() throws Exception {
    final ArcadeDBServer server = getServer(0);

    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.getHttpServer().getPort() + "/api/v1/server"))
        .header("X-ArcadeDB-Cluster-Token", "wrong-token")
        .header("X-ArcadeDB-Forwarded-User", "root")
        .GET().build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(401);
  }

  @Test
  void validTokenWithUnknownUserIsRejected() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String clusterToken = server.getHA().getClusterToken();

    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.getHttpServer().getPort() + "/api/v1/server"))
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", "nonexistent_user")
        .GET().build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(401);
  }

  @Test
  void clusterTokenWithoutUserHeaderIsRejected() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String clusterToken = server.getHA().getClusterToken();

    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + server.getHttpServer().getPort() + "/api/v1/server"))
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .GET().build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(401);
  }

  @Test
  void allNodesDeriveSameClusterToken() {
    // All nodes in the cluster should derive the same token from clusterName + rootPassword
    final String token0 = getServer(0).getHA().getClusterToken();
    final String token1 = getServer(1).getHA().getClusterToken();
    assertThat(token0).isEqualTo(token1);
  }
}
