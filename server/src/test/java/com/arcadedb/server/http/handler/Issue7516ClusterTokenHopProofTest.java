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
import com.arcadedb.server.LeaderForwardContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7516: {@code X-ArcadeDB-Cluster-Token} was doing two jobs at once - proving the request came from a
 * cluster peer, and switching identity resolution to {@code X-ArcadeDB-Forwarded-User}. A forward that has to
 * relay the client's own stateless credentials (Basic auth, or an API token whose scopes resolving the user
 * by name on the leader would discard) therefore could not carry the token, and so could not carry a
 * trustworthy one-hop marker either.
 * <p>
 * The two jobs are now separate: a valid cluster token proves the hop, and identity substitution happens only
 * when a forwarded user travels with it. What must not change is the fail-closed behaviour on either side of
 * that split, which is what these cases pin. The loop the split exists to stop is driven end to end in
 * {@code Issue7516BasicAuthForwardLoopIT}; it needs a cluster, and this does not.
 */
class Issue7516ClusterTokenHopProofTest extends BaseGraphServerTest {

  private static final String CLUSTER_TOKEN = "issue7516-cluster-secret-token";
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @BeforeEach
  void setClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
  }

  @AfterEach
  void clearClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
  }

  /**
   * The shape a Basic/API-token forward now has: the token proves the hop, the client's own credentials
   * authenticate the caller. Before the split this was answered 401 "Missing forwarded user".
   */
  @Test
  void aClusterTokenBesideRelayedCredentialsAuthenticatesWithThoseCredentials() throws Exception {
    final HttpResponse<String> response = command(b -> b
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .header("Authorization", basic("root", DEFAULT_PASSWORD_FOR_TESTS))
        .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true"));

    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
  }

  /**
   * Fail closed on the proof: an invalid token is refused before the relayed credentials are looked at, so a
   * caller cannot probe the token by watching which error comes back.
   */
  @Test
  void anInvalidClusterTokenIsRefusedEvenWhenTheRelayedCredentialsAreValid() throws Exception {
    final HttpResponse<String> response = command(b -> b
        .header("X-ArcadeDB-Cluster-Token", "not-the-cluster-token")
        .header("Authorization", basic("root", DEFAULT_PASSWORD_FOR_TESTS)));

    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.body()).contains("Invalid cluster token");
  }

  /**
   * Fail closed on identity: a valid token with neither a forwarded user nor credentials of its own still
   * authenticates nobody. The cluster token proves a hop; it has never been a principal.
   */
  @Test
  void aValidClusterTokenAloneStillAuthenticatesNobody() throws Exception {
    final HttpResponse<String> response = command(b -> b
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true"));

    assertThat(response.statusCode()).isEqualTo(401);
  }

  /**
   * And the substitution branch is untouched: a forwarded user still names the principal, and an unknown one
   * is still refused.
   */
  @Test
  void aForwardedUserStillNamesThePrincipal() throws Exception {
    assertThat(command(b -> b
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .header("X-ArcadeDB-Forwarded-User", "root")).statusCode()).isEqualTo(200);

    assertThat(command(b -> b
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .header("X-ArcadeDB-Forwarded-User", "issue7516-no-such-user")).statusCode()).isEqualTo(401);
  }

  /**
   * The port the server actually bound, not the first of the {@code 2480-2489} range it is allowed to pick
   * from: a hard-coded 2480 sends these requests to whatever else is listening there - another test JVM, an
   * IDE - and the failures then read as authentication errors rather than as a port clash.
   */
  private HttpResponse<String> command(final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + getServer(0).getHttpServer().getPort()
            + "/api/v1/command/" + getDatabaseName()))
        .header("Content-Type", "application/json");
    builder = decorate.apply(builder);
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(
        "{\"language\":\"sql\",\"command\":\"SELECT 1\"}", StandardCharsets.UTF_8)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder()
        .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
