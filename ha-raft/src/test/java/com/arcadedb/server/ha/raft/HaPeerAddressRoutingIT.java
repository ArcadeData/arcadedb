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
import com.arcadedb.exception.CauseChain;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * How a node names its peers, on a correctly addressed cluster, on ONE three-node cluster shared by every test
 * ({@link SharedClusterRaftHATest}). These are the control halves of three source classes whose other half injects a
 * misaddressed peer map, which is in {@link HaPeerAddressRoutingMapMutationIT}: from
 * {@code Issue6267AmbiguousAddressVisibilityIT}, {@code Issue10060SchemaRefusalNamesTheLeaderIT} and
 * {@code Issue10066SecurityRefusalNamesTheLeaderIT}, which ran on the same fixture: three servers, no configuration
 * override.
 * <ul>
 * <li><b>#6267</b>: with no {@code http} port declared in {@code arcadedb.ha.serverList}, every peer's HTTP endpoint
 * derives to its Raft host plus THIS node's port, so on a cluster whose nodes differ by port they all collapse onto one
 * address. {@code GET /api/v1/cluster} used to report a plausible address per peer with nothing saying it named none of
 * them, and its presence matrix dialled that address and credited every peer with the local node's databases. The flag
 * must appear only when it is true: a correctly addressed cluster carries none.</li>
 * <li><b>#10060</b>: a follower refusing a schema change named the leader by the HTTP address it resolved for it, even
 * when that was the follower's OWN (a same-host cluster that declares no HTTP ports, #6191), sending the client back to
 * the node that refused it. The forwarding refusals already fell back to the leader's server name; the schema refusal
 * now does too. Reached by an embedded caller that creates a type on the follower (an openCypher {@code CREATE} with a
 * new label inside a client transaction no longer reaches it: the leader creates the type, #10067).</li>
 * <li><b>#10066</b>: the same for the refusal of a {@code SecurityManager} user mutation on a follower, which named the
 * leader by {@code HAServerPlugin.getLeaderAddress()} unvetted: it now names the leader's HTTP address when that
 * identifies it, its server name when it does not.</li>
 * </ul>
 * Why these share a cluster: they only read. The schema change and the user creation are refused on the follower and
 * write nothing; no test touches the address map, the capability monitor, a node or leadership.
 * <p>
 * Siblings: {@link HaPeerAddressRoutingMapMutationIT} (#6267, #10060, #10066, #9255, rewrite the live address map),
 * {@link HaPeerAddressRoutingDeclaredGrpcIT} (#6091, a server list declaring gRPC ports),
 * {@link HaPeerAddressRoutingUndeclaredPortsIT} (#6183, two nodes and a gRPC port setting) and
 * {@link HaPeerAddressRoutingRestartIT} (#9243, unbound hints, restarts a node).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaPeerAddressRoutingIT extends SharedClusterRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Issue #6267, from {@code Issue6267AmbiguousAddressVisibilityIT}.
   * <p>
   * Control: with the addresses it actually has, every peer owns its endpoint and no flag is emitted. Without it the
   * assertions of {@link HaPeerAddressRoutingMapMutationIT} would also pass against a build that flagged every cluster.
   */
  @Test
  @Timeout(120)
  void aCorrectlyAddressedClusterCarriesNoAmbiguityFlag() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final JSONArray peers = getCluster(leader, "").getJSONArray("peers");
    assertThat(peers.length()).isEqualTo(getServerCount());
    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      assertThat(peer.has("httpAddressAmbiguous"))
          .as("a declared, distinct endpoint per peer carries no flag: %s", peer)
          .isFalse();
    }
  }

  /** Issue #10060, from {@code Issue10060SchemaRefusalNamesTheLeaderIT}. */
  @Test
  @Timeout(120)
  void aSchemaRefusalOnAFollowerOfAWellConfiguredClusterNamesTheLeadersHttpAddress() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final String leaderHttpAddress = raft.getLeaderHttpAddress();
    assertThat(raft.isOwnHttpAddress(leaderHttpAddress)).as("the fixture declares every node's HTTP port").isFalse();

    final ServerIsNotTheLeaderException refusal = schemaRefusalOn(follower, "Issue10060Configured");
    assertThat(refusal.getLeaderAddress()).isEqualTo(leaderHttpAddress);
  }

  /** Issue #10066, from {@code Issue10066SecurityRefusalNamesTheLeaderIT}. */
  @Test
  @Timeout(120)
  void aSecurityRefusalOnAFollowerOfAWellConfiguredClusterNamesTheLeadersHttpAddress() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final String leaderHttpAddress = getRaftPlugin(follower).getRaftHAServer().getLeaderHttpAddress();

    final ServerIsNotTheLeaderException refusal = securityRefusalOn(follower, "issue10066configured");
    assertThat(refusal.getLeaderAddress()).isEqualTo(leaderHttpAddress);
  }

  // ---------------------------------------------------------------------------------------------

  /** #10060: creates a type through a follower's database, which must be refused as a leader refusal. */
  static ServerIsNotTheLeaderException schemaRefusalOn(final Database db, final String label) {
    final Throwable error = catchThrowable(() -> db.getSchema().createVertexType(label));
    final ServerIsNotTheLeaderException refusal = CauseChain.find(error, ServerIsNotTheLeaderException.class);
    assertThat(refusal).as("creating a type on a follower must be refused as a leader refusal, got %s", error).isNotNull();
    assertThat(refusal.getMessage()).contains("Changes to the schema must be executed on the leader server");
    return refusal;
  }

  private ServerIsNotTheLeaderException schemaRefusalOn(final int follower, final String label) {
    return schemaRefusalOn(getServerDatabase(follower, getDatabaseName()), label);
  }

  /** #10066: creates a user through the {@code SecurityManager} entry point of a follower, which must be refused. */
  static ServerIsNotTheLeaderException securityRefusalOn(final ServerSecurity followerSecurity, final String user) {
    final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
        () -> followerSecurity.createUser(user, "issue10066password"));
    assertThat(refusal).as("a user mutation on a follower must be refused as a leader refusal").isNotNull();
    assertThat(refusal.getMessage()).contains("must run on the cluster leader");
    return refusal;
  }

  private ServerIsNotTheLeaderException securityRefusalOn(final int follower, final String user) {
    return securityRefusalOn(getServer(follower).getSecurity(), user);
  }

  private JSONObject getCluster(final int serverIndex, final String query) throws Exception {
    return getCluster(getServer(serverIndex), query);
  }

  /** #6267: GETs the cluster endpoint of one server as the root operator, returning the parsed 200 body. */
  static JSONObject getCluster(final ArcadeDBServer server, final String query) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + server.getHttpServer().getPort() + "/api/v1/cluster" + query)
        .toURL().openConnection();
    try {
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }
}
