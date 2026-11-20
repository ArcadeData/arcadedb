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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.HAServerPlugin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code connect cluster} over the HTTP {@code POST /api/v1/server} transport.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class ConnectClusterHttpIT {

  /**
   * Issue #7400, the HTTP side of the pair. The gRPC {@code ConnectCluster} RPC added by that issue
   * delegates to {@code ServerControlPlane.connectCluster}, the same method {@code POST /api/v1/server}
   * has always called - so what the two transports answer for the same input is now a property worth
   * pinning from both ends rather than from one.
   * <p>
   * Issue #7401 has since made the verb a real join, so what this fixture - a server with no HA at all -
   * exercises is the refusal path. What must hold is unchanged and is the reason the class survives that:
   * the refusal is the <em>same</em> refusal, naming the address the caller asked for, on whichever
   * transport the caller used.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class TransportParity extends BaseGraphServerTest {
    private static final String PEER_ADDRESS = "localhost:2425";

    private final HttpClient client = HttpClient.newHttpClient();

    /**
     * The address survives {@code extractTarget} and reaches the shared implementation, which names it
     * in the refusal. This is the HTTP half of the gRPC IT's
     * {@code theAddressReachesTheSharedImplementationUnmodified}: a handler that dropped the argument
     * on either transport would otherwise be indistinguishable from one that passed it through, since
     * nothing else in the answer depends on it.
     */
    @Test
    void connectClusterIsRefusedAndNamesTheAddress() throws Exception {
      final HttpResponse<String> response = executeServerCommand("connect cluster " + PEER_ADDRESS);

      assertThat(response.statusCode()).isEqualTo(500);
      assertThat(response.body())
          .contains(PEER_ADDRESS)
          .contains("not running with High Availability module enabled");
    }

    /**
     * A bare {@code connect cluster} yields {@code ""} from {@code extractTarget}. Since #7401 the verb
     * has an argument it genuinely needs, so that is a client error - 400 - and no longer the same
     * refusal a filled address gets. The gRPC side mirrors it as {@code INVALID_ARGUMENT} rather than
     * adding a gate of its own, which is the parity this class is about: both statuses come from the one
     * shared implementation.
     */
    @Test
    void connectClusterWithNoAddressIsRefusedTheSameWay() throws Exception {
      final HttpResponse<String> response = executeServerCommand("connect cluster");

      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(response.body()).contains("requires the address of the server to join");
    }

    private HttpResponse<String> executeServerCommand(final String command) throws Exception {
      final HttpRequest request = HttpRequest.newBuilder()
          // The bound port, not the 2480 default: HttpServer takes the first free port of the
          // configured range, so a hardcoded 2480 sends the request to whatever else already holds it.
          .uri(new URI("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
          .setHeader("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .build();

      return client.send(request, BodyHandlers.ofString());
    }
  }

  /**
   * Issue #7532 (absorbing #7550) on the HTTP {@code POST /api/v1/server} transport: {@code connect cluster}
   * answers 503, and names the documents, when the join succeeded but a security document could not be seeded to
   * the new peer.
   * <p>
   * That is the status {@code POST /api/v1/cluster/peer} has answered the identical condition since issue #7521.
   * Before this the two verbs disagreed - the add-peer route failed hard and named the documents, while this one
   * answered 200 and left the failure in a SEVERE log line - so which verb an operator drove the join with
   * decided whether their automation could see that the cluster was enforcing two different security states.
   * <p>
   * Driven over real HTTP rather than against the handler, because the status code is the whole contract here and
   * a unit test of the shared control-plane method cannot observe it.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class SeedFailure extends BaseGraphServerTest {
    private static final String PEER_ADDRESS = "localhost:2425";

    private final HttpClient client = HttpClient.newHttpClient();

    @Override
    protected int getServerCount() {
      return 1;
    }

    /** A join whose seed lands completely is still a 200, unchanged. */
    @Test
    void aCleanJoinIsStillReportedAsSuccess() throws Exception {
      getServer(0).setHA(new SeedingHAPlugin());

      final HttpResponse<String> response = executeServerCommand("connect cluster " + PEER_ADDRESS);

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).contains("\"result\"");
    }

    /**
     * The defect: this used to be a 200 with the failure nowhere in the response. The 503 is what an operator's
     * automation branches on, and the document names are what tells them which change to reissue.
     */
    @Test
    void aResidualSeedFailureAnswers503AndNamesTheDocuments() throws Exception {
      getServer(0).setHA(new SeedingHAPlugin() {
        @Override
        public void replicateSecurityGroups(final String groupsJson) {
          throw new IllegalStateException("no quorum to replicate the group document to");
        }
      });

      final HttpResponse<String> response = executeServerCommand("connect cluster " + PEER_ADDRESS);

      assertThat(response.statusCode()).isEqualTo(503);
      final JSONObject body = new JSONObject(response.body());
      assertThat(body.getString("error", "")).contains(PEER_ADDRESS, "groups");
      assertThat(body.getString("detail", "")).contains("connect cluster");
      assertThat(body.getJSONArray("failedSeeds").toList()).containsExactly("groups");
      assertThat(body.getString("result", ""))
          .as("the membership change DID happen; a caller treating the call as a no-op would be wrong about it")
          .isEqualTo("ok");
    }

    private HttpResponse<String> executeServerCommand(final String command) throws Exception {
      final HttpRequest request = HttpRequest.newBuilder()
          // The bound port, not the 2480 default: HttpServer takes the first free port of the configured range.
          .uri(new URI("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/server"))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
          .setHeader("Authorization", "Basic " + Base64.getEncoder()
              .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .build();

      return client.send(request, BodyHandlers.ofString());
    }

    /** An HA plugin that accepts the join and whose three security replications succeed unless overridden. */
    private static class SeedingHAPlugin implements HAServerPlugin {
      @Override
      public void connectCluster(final String serverAddress) {
      }

      @Override
      public void replicateSecurityUsers(final String usersJsonArray) {
      }

      @Override
      public void replicateSecurityGroups(final String groupsJson) {
      }

      @Override
      public void replicateSecurityApiTokens(final String apiTokensJson) {
      }

      @Override
      public ELECTION_STATUS getElectionStatus() {
        return ELECTION_STATUS.DONE;
      }

      @Override
      public void startService() {
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
      public String getClusterName() {
        return "test";
      }

      @Override
      public Map<String, Object> getStats() {
        return Collections.emptyMap();
      }

      @Override
      public int getConfiguredServers() {
        return 2;
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
}
