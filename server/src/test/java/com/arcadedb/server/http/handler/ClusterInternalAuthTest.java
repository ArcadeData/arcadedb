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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the server accepts commands forwarded from a replica via
 * the cluster-internal trust-token headers, and rejects invalid tokens.
 */
class ClusterInternalAuthTest extends BaseGraphServerTest {

  private static final String CLUSTER_TOKEN = "test-cluster-secret-token";
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @BeforeEach
  void setClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
  }

  @AfterEach
  void clearClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
  }

  @Test
  void validClusterTokenWithKnownUserIsAccepted() throws Exception {
    final HttpResponse<String> resp = sendWithInternalHeaders(CLUSTER_TOKEN, "root");
    assertThat(resp.statusCode()).isNotEqualTo(401);
    assertThat(resp.statusCode()).isNotEqualTo(403);
  }

  @Test
  void invalidClusterTokenIsRejected() throws Exception {
    final HttpResponse<String> resp = sendWithInternalHeaders("wrong-token", "root");
    assertThat(resp.statusCode()).isEqualTo(401);
  }

  @Test
  void validTokenWithUnknownUserIsRejected() throws Exception {
    final HttpResponse<String> resp = sendWithInternalHeaders(CLUSTER_TOKEN, "no-such-user");
    assertThat(resp.statusCode()).isEqualTo(401);
  }

  @Test
  void clusterTokenHeaderAloneWithoutUserHeaderIsRejected() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/api/v1/command/" + getDatabaseName()))
        .header("Content-Type", "application/json")
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"SELECT 1\"}"))
        .build();
    final HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(resp.statusCode()).isEqualTo(401);
  }

  private HttpResponse<String> sendWithInternalHeaders(final String clusterToken, final String userName)
      throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/api/v1/command/" + getDatabaseName()))
        .header("Content-Type", "application/json")
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", userName)
        .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"SELECT 1\"}"))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
