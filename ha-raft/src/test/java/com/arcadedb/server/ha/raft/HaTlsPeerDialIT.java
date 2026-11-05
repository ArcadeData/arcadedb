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

import com.arcadedb.database.Database;
import com.arcadedb.network.HttpClientLifecycle;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.LeaderDial;
import com.arcadedb.utility.FileUtils;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every peer-to-peer dial of the module driven across a <b>real TLS socket</b> between two nodes: the end-to-end half
 * of issue #7563 (the follower-to-leader forward, the bootstrap-state probe and the remote shutdown endpoint, plus the
 * foreign-CA negative) and of issue #7835 (the snapshot download, the capability probe, the auth-session RPC, the
 * stalled-replica resync, the cross-node verify and the security-seed RPC of issues #7833 / #7834). Merged from
 * {@code Issue7563HaTlsPeerDialIT} and {@code Issue7835HaTlsRemainingPeerDialsIT}, which ran on the same fixture: a
 * two-node {@link BaseRaftHASslTest} cluster with the default subject alternative names.
 * <p>
 * Everything these dials DECIDE is already unit-tested against recording clients - which address, which scheme, which
 * client ({@code Issue7508LeaderForwardSchemeTest}, {@code Issue7508LeaderHttpsEndpointTest},
 * {@link Issue7563PlaintextPeerDialSchemeTest}). Four things only appear once a handshake actually happens, and no
 * recording client can reach any of them:
 * <ul>
 * <li><b>hostname verification.</b> The endpoint is taken from {@code arcadedb.ha.serverList} or derived from the
 * peer's Raft host; a certificate issued for a different name than the one the dial asks for fails at handshake time,
 * and a test that never handshakes cannot notice. The name check itself is pinned by
 * {@link Issue7836HaTlsWrongCertificateNameIT}.</li>
 * <li><b>the truststore, in anger.</b> {@code SnapshotInstaller.buildSSLContext} falls back to
 * {@code SSLContext.getDefault()} when none is configured, which is the state a test JVM is normally in - so a
 * genuinely misconfigured truststore and a correct one are indistinguishable until one is configured and a peer that
 * it does NOT vouch for is shown to be rejected. {@link #aPeerNotSignedByTheClusterCaIsRejected()} is that half, and
 * without it every passing test here would prove only that TLS accepts everything.</li>
 * <li><b>the credential.</b> Whether the cluster token a dial carries is authenticated at the other end.</li>
 * <li><b>the interaction with the 5th field of {@code arcadedb.ha.serverList}.</b> The fixture DECLARES each node's
 * {@code https} port rather than letting the derive fallback collapse every peer's HTTPS endpoint onto this node's own
 * port; see {@link BaseRaftHASslTest}.</li>
 * </ul>
 * Runs on the per-test fixture: {@link BaseRaftHASslTest} extends {@link BaseRaftHATest} directly, so it cannot be put
 * on {@link SharedClusterRaftHATest} without a shared variant of the SSL base.
 * <p>
 * Tagged {@code slow}: it generates a certificate authority with {@code keytool} and starts a two-node cluster with
 * two listeners each.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class HaTlsPeerDialIT extends BaseRaftHASslTest {

  private static final String VERTEX_TYPE = "TlsForwardedWrite";

  /** A foreign authority, generated per run: its CA is the one the cluster must refuse. */
  private static RaftTestPki foreignPki;

  @Override
  protected String pkiDirectoryName() {
    return "ha-tls-peer-dial-pki";
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  // ------------------------------------------------------------------ the leader forward

  /**
   * Issue #7563, from {@code Issue7563HaTlsPeerDialIT}.
   * <p>
   * The case issue #7563 was opened for. A non-idempotent SQL command issued on a FOLLOWER is forwarded by
   * {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft} to the leader, and on this cluster the URL it
   * builds is {@code https://}. There is no plain-HTTP path left for it to have silently taken: the assertion
   * on the dial fixes the scheme and the address before the write, and the write then either completes over
   * that socket or fails.
   * <p>
   * So the outcome is the evidence. A certificate issued for a name other than {@code localhost}, a truststore
   * that does not vouch for it, or a client built without one, each ends this test in an exception rather than
   * in a row on the leader.
   */
  @Test
  void aWriteIssuedOnAFollowerReachesTheLeaderOverARealTlsSocket() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected before a forward can be tested").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });

    final LeaderDial dial = LeaderDial.resolve(getServer(followerIndex).getHA(), HttpClient.newHttpClient());
    assertThat(dial).isNotNull();
    assertThat(dial.refused()).as("the forward must not be refused: %s", dial.refusal()).isFalse();
    assertThat(dial.https())
        .as("the follower must forward to the leader's HTTPS endpoint, not its plain listener")
        .isTrue();
    assertThat(dial.address())
        .isEqualTo("localhost:" + getServer(leaderIndex).getHttpServer().getHttpsPort());

    // The write itself. Non-idempotent, so command() forwards it rather than running it locally.
    getServerDatabase(followerIndex, getDatabaseName())
        .command("sql", "INSERT INTO " + VERTEX_TYPE + " SET marker = 'forwarded-over-tls'");

    assertThat(leaderDb.countType(VERTEX_TYPE, true))
        .as("the forwarded write must have landed on the leader, having crossed a real TLS socket")
        .isEqualTo(1);
  }

  // ------------------------------------------------------------------ the bootstrap-state probe

  /**
   * Issue #7563, from {@code Issue7563HaTlsPeerDialIT}.
   * <p>
   * The first of the two dials issue #7546 found still on plain HTTP, driven for real: the production probe,
   * against the live leader, over the cluster's own truststore. It answers a parsed state only if the whole
   * chain worked - scheme choice, handshake, hostname verification, the cluster token's authentication at the
   * other end - so a {@code null} here is the failure this test exists to catch.
   */
  @Test
  void theBootstrapStateProbeOfAPeerTravelsOverTls() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    final RaftPeerId leaderPeer = RaftPeerId.valueOf(peerIdForIndex(leaderIndex));
    final String httpAddr = follower.getHttpAddresses().get(leaderPeer);
    final String httpsAddr = follower.getPeerHttpsAddress(leaderPeer);

    assertThat(httpsAddr)
        .as("the 5th field of the server list must resolve the leader's HTTPS endpoint from the follower")
        .isEqualTo("localhost:" + getServer(leaderIndex).getHttpServer().getHttpsPort());
    assertThat(BootstrapElection.chooseUrl(httpAddr, httpsAddr, true))
        .isEqualTo("https://" + httpsAddr + "/api/v1/cluster/bootstrap-state");

    assertThat(BootstrapElection.fetchBootstrapState(getServer(followerIndex),
        getRaftPlugin(leaderIndex).getRaftHAServer().getLocalPeerId().toString(), httpAddr, httpsAddr,
        follower.getClusterToken(), Set.of(getDatabaseName()), 10_000L))
        .as("the HTTPS bootstrap-state probe must reach the leader and be understood")
        .isNotNull()
        .containsKey(getDatabaseName());
  }

  // ------------------------------------------------------------------ the remote shutdown

  /**
   * Issue #7563, from {@code Issue7563HaTlsPeerDialIT}.
   * <p>
   * The second dial, up to but not including the command it would send. The URL is resolved exactly as
   * {@code RaftHAPlugin.shutdownRemoteServer} resolves it - through {@link PeerDialAddress}, which is the
   * other half of what issue #7546 asked for, since this dial used to read {@code getHttpAddresses()} by hand
   * and so inherited neither the ambiguity guard nor the self-dial guard - and the endpoint it names is then
   * handshaked with the very client the shutdown is sent on.
   * <p>
   * The {@code POST} itself is deliberately not issued: it would stop a node mid-suite, because
   * {@code ServerControlPlane.shutdownServer("")} ends in {@code System.exit}. What is proved here is that the
   * transport the command would travel on is TLS and that it works. The CREDENTIAL it carries was the separate
   * question, fixed in issue #7837 and covered by {@code Issue7837RemoteShutdownCredentialIT}.
   */
  @Test
  void theRemoteShutdownDialResolvesAndHandshakesThePeersEncryptedEndpoint() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final PeerDialAddress dial = PeerDialAddress.resolve(leader, RaftPeerId.valueOf(peerIdForIndex(followerIndex)),
        "peer");

    assertThat(dial.refused()).as("resolving the peer to shut down must not be refused: %s", dial.refusal()).isFalse();

    final String url = RaftHAPlugin.shutdownUrl(dial, true);
    assertThat(url)
        .isEqualTo("https://localhost:" + getServer(followerIndex).getHttpServer().getHttpsPort() + "/api/v1/server");

    // The same endpoint, the same trust context the shutdown builds its client from. Any HTTP status answers
    // the question this test asks - the handshake is what is being pinned, not the route's own behaviour.
    assertThat(readyProbeStatus(getServer(leaderIndex), url.substring(0, url.lastIndexOf('/')) + "/ready"))
        .as("the peer's HTTPS listener must complete a handshake against the configured cluster truststore")
        .isBetween(200, 599);
  }

  // ------------------------------------------------------------------ the truststore, in anger

  /**
   * Issue #7563, from {@code Issue7563HaTlsPeerDialIT}.
   * <p>
   * The negative that gives the three tests above their meaning. A client that trusts a DIFFERENT certificate
   * authority - one the cluster's truststore has never heard of - is rejected by the leader's HTTPS listener
   * at the handshake, before any HTTP status exists.
   * <p>
   * Without this, every passing assertion above is equally consistent with a listener that accepts anything
   * and a client that verifies nothing, which is precisely the blind spot issue #7563 describes: a test JVM
   * with no truststore configured cannot tell a working configuration from a broken one.
   */
  @Test
  void aPeerNotSignedByTheClusterCaIsRejected() throws Exception {
    if (foreignPki == null)
      foreignPki = RaftTestPki.create(Path.of("target", "issue7563-foreign-pki"), "foreign");

    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final String leaderUrl = "https://localhost:" + getServer(leaderIndex).getHttpServer().getHttpsPort()
        + "/api/v1/ready";

    final SSLContext foreignTrust = RaftTestPki.anonymousClientContext(foreignPki);
    final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(foreignTrust)
        .build();
    try {
      assertThatThrownBy(() -> client.send(
          HttpRequest.newBuilder().uri(URI.create(leaderUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.discarding()))
          .as("a client trusting a foreign CA must not be able to talk to a cluster node over TLS")
          .isInstanceOf(IOException.class);
        } finally {
      HttpClientLifecycle.close(client);
    }
  }

  // ------------------------------------------------------------------ the snapshot download

  /**
   * Issue #7835, from {@code Issue7835HaTlsRemainingPeerDialsIT}.
   * <p>
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

      // try-with-resources: Files.list holds an open directory handle, and the finally below has to be able to
      // delete the directory (CodeRabbit on PR #7854).
      try (final Stream<Path> written = Files.list(target)) {
        assertThat(written.findAny())
            .as("the snapshot download must have written the leader's database files over the TLS socket")
            .isPresent();
      }
    } finally {
      FileUtils.deleteRecursively(target.toFile());
    }
  }

  // ------------------------------------------------------------------ the capability probe

  /**
   * Issue #7835, from {@code Issue7835HaTlsRemainingPeerDialsIT}.
   * <p>
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
   * Issue #7835, from {@code Issue7835HaTlsRemainingPeerDialsIT}.
   * <p>
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
   * Issue #7835, from {@code Issue7835HaTlsRemainingPeerDialsIT}.
   * <p>
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
   * Issue #7835, from {@code Issue7835HaTlsRemainingPeerDialsIT}.
   * <p>
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
   * Issue #7835, from {@code Issue7835HaTlsRemainingPeerDialsIT}.
   * <p>
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

  /** Sends a GET over the trust context {@code server}'s own peer-to-peer dials are built from. */
  private static int readyProbeStatus(final ArcadeDBServer server, final String url) throws Exception {
    final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(server))
        .build();
    try {
      return client.send(
          HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.discarding()).statusCode();
        } finally {
      HttpClientLifecycle.close(client);
    }
  }

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
    final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(getServer(fromIndex)))
        .build();
    try {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
        } finally {
      HttpClientLifecycle.close(client);
    }
  }
}
