/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.utility.FileUtils;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The peer-to-peer dials issue #7563 left proved only by unit tests, driven over a real TLS socket (issue
 * #7835).
 * <p>
 * Each of these dials chooses its address and scheme through a small pure function that is already unit-tested
 * against a recording client. What no recording client can reach is what only exists once a handshake happens:
 * whether the certificate the peer presents is issued for the name the dial asked for, whether the truststore
 * the node is configured with is the one being consulted, and whether the credential the dial carries is
 * authenticated at the other end. {@code Issue7563HaTlsPeerDialIT} established that for the leader forward, the
 * bootstrap-state probe and the shutdown endpoint; this covers the rest of the table in issue #7835, plus the
 * security-seed RPC issues #7833 and #7834 add.
 * <p>
 * The negative that gives them all meaning - a client trusting a foreign CA is refused - lives in
 * {@code Issue7563HaTlsPeerDialIT.aPeerNotSignedByTheClusterCaIsRejected}, and the name check in
 * {@link Issue7836HaTlsWrongCertificateNameIT}. Without those, a passing assertion here would be equally
 * consistent with a listener that accepts anything.
 * <p>
 * Tagged {@code slow}: it generates a certificate authority with {@code keytool} and starts a two-node cluster
 * with two listeners each.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue7835HaTlsRemainingPeerDialsIT extends BaseRaftHASslTest {

  @Override
  protected String pkiDirectoryName() {
    return "issue7835-ha-tls-remaining-dials-pki";
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  // ------------------------------------------------------------------ the snapshot download

  /**
   * The biggest dial, and the only one that moves bytes: a follower pulls a whole database from the leader over
   * {@code /api/v1/ha/snapshot/&#123;db&#125;}. Driven through the production entry point, with the production
   * trust context, into a scratch directory rather than over the follower's live copy - an install is a
   * different question from a download, and this is the download's.
   */
  @Test
  void theSnapshotDownloadPullsARealDatabaseOverTls() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    final RaftPeerId leaderPeer = RaftPeerId.valueOf(peerIdForIndex(leaderIndex));
    final String httpsAddr = follower.getPeerHttpsAddress(leaderPeer);
    assertThat(httpsAddr)
        .as("the leader's HTTPS endpoint must resolve, or this test proves nothing about TLS")
        .isEqualTo("localhost:" + getServer(leaderIndex).getHttpServer().getHttpsPort());

    final Path target = Files.createTempDirectory("issue7835-snapshot-");
    try {
      SnapshotInstaller.downloadWithRetry(getDatabaseName(), target,
          () -> follower.getHttpAddresses().get(leaderPeer), () -> httpsAddr, follower.getClusterToken(),
          0, 100L, getServer(followerIndex));

      assertThat(Files.list(target).findAny())
          .as("the snapshot download must have written the leader's database files over the TLS socket")
          .isPresent();
    } finally {
      FileUtils.deleteRecursively(target.toFile());
    }
  }

  // ------------------------------------------------------------------ the capability probe

  /**
   * The dial the leader repeats for as long as it leads, and therefore the one that carries the cluster token
   * most often. It answers an advertisement only if the whole chain worked, so the peer id coming back is the
   * assertion.
   */
  @Test
  void theCapabilityProbeOfAPeerTravelsOverTls() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftPeerId followerPeer = RaftPeerId.valueOf(peerIdForIndex(followerIndex));
    final String httpsAddr = leader.getPeerHttpsAddress(followerPeer);

    assertThat(PeerCapabilityQuery.chooseUrl(leader.getHttpAddresses().get(followerPeer), httpsAddr, true))
        .isEqualTo("https://" + httpsAddr + "/api/v1/cluster/capabilities");

    assertThat(PeerCapabilityQuery.fetch(followerPeer.toString(), leader.getHttpAddresses().get(followerPeer),
        httpsAddr, leader.getClusterToken(), 10_000L, getServer(leaderIndex), leader.getHttpsClients()).peerId())
        .as("the HTTPS capability probe must reach the peer and be understood")
        .isEqualTo(followerPeer.toString());
  }

  // ------------------------------------------------------------------ the auth-session RPC

  /**
   * A node asks the ISSUER of a session token about it (issue #7424). A token no node ever issued is answered
   * 404, which the client returns as {@code null} - and getting that answer at all means the handshake, the
   * hostname check and the cluster token were all accepted, because every other outcome of this call is an
   * {@link java.io.IOException}.
   */
  @Test
  void theAuthSessionRpcTravelsOverTls() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    assertThat(PeerAuthSessionQuery.validate(getRaftPlugin(leaderIndex).getRaftHAServer(),
        RaftPeerId.valueOf(peerIdForIndex(followerIndex)), "AU-a-token-no-node-ever-issued", 10_000L))
        .as("a token the issuer does not hold is a 404, which is an answer and therefore a completed dial")
        .isNull();
  }

  // ------------------------------------------------------------------ the stalled-replica resync

  /**
   * The route a leader forces a persistently stalled replica onto, dialled with the trust context and the
   * credential {@code RaftHAServer.forceResyncStalledReplica} builds.
   * <p>
   * Aimed at the LEADER on purpose. The endpoint's own first act after authenticating is to refuse a resync of
   * the node that holds the authoritative copy, so the answer is a 400 that could only have been produced past
   * the authentication - which is what is under test - without making a node in this suite re-download its
   * databases as a side effect.
   */
  @Test
  void theStalledReplicaResyncEndpointAuthenticatesOverTls() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final String url = "https://localhost:" + getServer(leaderIndex).getHttpServer().getHttpsPort()
        + "/api/v1/cluster/resync/" + getDatabaseName();

    final HttpResponse<String> response = sendWithClusterToken(leaderIndex, url, leader.getClusterToken());

    assertThat(response.statusCode())
        .as("the cluster token must authenticate on this route: %s", response.body())
        .isNotEqualTo(401);
    assertThat(response.body())
        .as("and the answer must come from inside the handler, past the authentication")
        .contains("the leader holds the authoritative copy");
  }

  // ------------------------------------------------------------------ the cross-node verify

  /**
   * The fan-out dial, driven the way an operator triggers it: {@code POST /api/v1/cluster/verify/&#123;db&#125;}
   * on one node, which then queries every peer over the peer transport and reports what each answered. A peer
   * entry with a status rather than a transport error is the evidence that the inner dial handshook.
   */
  @Test
  void theCrossNodeVerifyQueriesItsPeerOverTls() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final String url = "https://localhost:" + getServer(leaderIndex).getHttpServer().getHttpsPort()
        + "/api/v1/cluster/verify/" + getDatabaseName();

    final HttpResponse<String> response = sendWithBasicRoot(leaderIndex, url);
    assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);

    final JSONArray peers = new JSONObject(response.body()).getJSONObject("result").getJSONArray("peers");
    assertThat(peers.length())
        .as("the verify must have reached the other node over the peer transport: %s", response.body())
        .isPositive();
    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      assertThat(peer.getString("status", ""))
          .as("a peer answered: %s", peer)
          .isIn("CONSISTENT", "INCONSISTENT");
    }
  }

  // ------------------------------------------------------------------ the security-seed RPC

  /**
   * The RPC issues #7833 and #7834 add: a follower asks the leader to seed the cluster security documents and
   * reads back what did not commit. New, so it is covered here from the start rather than joining the list this
   * issue exists to work through.
   */
  @Test
  void theSecuritySeedRequestTravelsOverTls() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    assertThat(ClusterSecuritySeedQuery.seedForCatchUp(getServer(followerIndex), getRaftPlugin(followerIndex),
        "an Issue7835 regression test"))
        .as("the follower must reach the leader's security-seed route over the encrypted endpoint")
        .isEmpty();
  }

  // ------------------------------------------------------------------ helpers

  /** A POST carrying the peer-to-peer credential, on the trust context this node's own dials are built from. */
  private HttpResponse<String> sendWithClusterToken(final int fromIndex, final String url, final String token)
      throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}", StandardCharsets.UTF_8));
    if (token != null && !token.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", token);
    builder.header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER);
    return send(fromIndex, builder.build());
  }

  /** A POST as the operator, for the routes an operator triggers. */
  private HttpResponse<String> sendWithBasicRoot(final int fromIndex, final String url) throws Exception {
    return send(fromIndex, HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString("{}", StandardCharsets.UTF_8))
        .build());
  }

  private HttpResponse<String> send(final int fromIndex, final HttpRequest request) throws Exception {
    try (final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(getServer(fromIndex)))
        .build()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
