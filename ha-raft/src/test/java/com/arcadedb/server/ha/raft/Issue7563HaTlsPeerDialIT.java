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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.handler.LeaderDial;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The end-to-end half of issue #7563: a follower-to-leader forward, and the two peer-to-peer dials that used
 * to be hardcoded to {@code http://}, driven across a <b>real TLS socket</b> between two nodes.
 * <p>
 * Everything these dials DECIDE is already unit-tested against recording clients - which address, which
 * scheme, which client ({@code Issue7508LeaderForwardSchemeTest}, {@code Issue7508LeaderHttpsEndpointTest},
 * {@link Issue7563PlaintextPeerDialSchemeTest}). Three things only appear once a handshake actually happens,
 * and no recording client can reach any of them:
 * <ul>
 * <li><b>hostname verification.</b> The endpoint is taken from {@code arcadedb.ha.serverList} or derived from
 * the peer's Raft host; a certificate issued for a different name than the one the dial asks for fails at
 * handshake time, and a test that never handshakes cannot notice.</li>
 * <li><b>the truststore, in anger.</b> {@code SnapshotInstaller.buildSSLContext} falls back to
 * {@code SSLContext.getDefault()} when none is configured, which is the state a test JVM is normally in - so
 * a genuinely misconfigured truststore and a correct one are indistinguishable until one is configured and a
 * peer that it does NOT vouch for is shown to be rejected. {@link #aPeerNotSignedByTheClusterCaIsRejected()}
 * is that half, and without it the passing tests below would prove only that TLS accepts everything.</li>
 * <li><b>the interaction with the 5th field of {@code arcadedb.ha.serverList}.</b> The fixture DECLARES each
 * node's {@code https} port rather than letting the derive fallback collapse every peer's HTTPS endpoint onto
 * this node's own port; see {@link BaseRaftHASslTest}.</li>
 * </ul>
 * Tagged {@code slow}: it generates a certificate authority with {@code keytool} and starts a two-node
 * cluster with two listeners each.
 */
@Tag("slow")
class Issue7563HaTlsPeerDialIT extends BaseRaftHASslTest {

  private static final String VERTEX_TYPE = "TlsForwardedWrite";

  /** A foreign authority, generated per run: its CA is the one the cluster must refuse. */
  private static RaftTestPki foreignPki;

  @Override
  protected String pkiDirectoryName() {
    return "issue7563-ha-tls-peer-dial-pki";
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  // ------------------------------------------------------------------ the leader forward

  /**
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

    assertThat(BootstrapElection.fetchBootstrapState(getServer(followerIndex), httpAddr, httpsAddr,
        follower.getClusterToken(), Set.of(getDatabaseName()), 10_000L))
        .as("the HTTPS bootstrap-state probe must reach the leader and be understood")
        .isNotNull()
        .containsKey(getDatabaseName());
  }

  // ------------------------------------------------------------------ the remote shutdown

  /**
   * The second dial, up to but not including the command it would send. The URL is resolved exactly as
   * {@code RaftHAPlugin.shutdownRemoteServer} resolves it - through {@link PeerDialAddress}, which is the
   * other half of what issue #7546 asked for, since this dial used to read {@code getHttpAddresses()} by hand
   * and so inherited neither the ambiguity guard nor the self-dial guard - and the endpoint it names is then
   * handshaked with the very client the shutdown is sent on.
   * <p>
   * The {@code POST} itself is deliberately not issued: it would stop a node mid-suite, and its
   * {@code Authorization: Bearer <clusterToken>} header is a separate question from this issue's (tracked as a
   * follow-up). What is proved here is that the transport the command would travel on is TLS and that it works.
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
    try (final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(foreignTrust)
        .build()) {
      assertThatThrownBy(() -> client.send(
          HttpRequest.newBuilder().uri(URI.create(leaderUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.discarding()))
          .as("a client trusting a foreign CA must not be able to talk to a cluster node over TLS")
          .isInstanceOf(IOException.class);
    }
  }

  /** Sends a GET over the trust context {@code server}'s own peer-to-peer dials are built from. */
  private static int readyProbeStatus(final ArcadeDBServer server, final String url) throws Exception {
    try (final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(server))
        .build()) {
      return client.send(
          HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.discarding()).statusCode();
    }
  }
}
