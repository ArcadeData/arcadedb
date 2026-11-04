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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.PeerRequest;
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
 * Issue #10827: the snapshot route read a present but INVALID {@code X-ArcadeDB-Cluster-Token} as if the header were absent and
 * fell through to the {@code Authorization} check, so a wrong token sent with valid root Basic credentials was served the
 * snapshot. The shared HTTP gate ({@code AbstractServerHttpHandler}) refuses that request with 401 before it looks at any
 * credential ({@code Issue7516ClusterTokenHopProofTest}); the two cluster-token checks now agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10827SnapshotInvalidClusterTokenTest extends BaseRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  private static final String ROOT_BASIC = "Basic " + Base64.getEncoder()
      .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));

  @Override
  protected int getServerCount() {
    return 1;
  }

  /** The base class turns HA on only for two servers or more; this cluster is one voter on purpose. */
  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, getServerAddresses());
    config.setValue(GlobalConfiguration.HA_ENABLED, true);
  }

  @Test
  @Timeout(120)
  void anInvalidClusterTokenIsRefusedEvenWhenTheBasicCredentialsBesideItAreValid() throws Exception {
    final HttpResponse<String> response = snapshot("issue10827-not-the-cluster-token", ROOT_BASIC);
    assertThat(response.statusCode())
        .as("a wrong cluster token must be refused before the Basic credentials beside it are looked at, body: %s", response.body())
        .isEqualTo(401);
    assertThat(response.body()).contains("Invalid cluster token");
  }

  @Test
  @Timeout(120)
  void aBlankClusterTokenIsRefusedToo() throws Exception {
    assertThat(snapshot("", ROOT_BASIC).statusCode()).isEqualTo(401);
  }

  @Test
  @Timeout(120)
  void thePeerShapeAndTheOperatorShapeAreStillServed() throws Exception {
    final String clusterToken = getRaftPlugin(0).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).isNotBlank();

    assertThat(snapshot(clusterToken, null).statusCode()).as("a peer: the cluster token alone").isEqualTo(200);
    assertThat(snapshot(null, ROOT_BASIC).statusCode()).as("an operator: root's Basic credentials alone").isEqualTo(200);
    assertThat(snapshot(null, null).statusCode()).as("nothing at all").isEqualTo(401);
  }

  private HttpResponse<String> snapshot(final String clusterToken, final String authorization) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ha/snapshot/" + getDatabaseName()))
        .timeout(Duration.ofSeconds(60))
        .GET();
    if (clusterToken != null)
      builder.header(PeerRequest.CLUSTER_TOKEN_HEADER, clusterToken);
    if (authorization != null)
      builder.header("Authorization", authorization);
    // the body of a served snapshot is a ZIP: read as text it is only discarded
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.ISO_8859_1));
  }
}
