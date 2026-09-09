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

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7301, a follow-up to #7219: what {@code GET /api/v1/cluster} is entitled to say about a peer's
 * capabilities, when the leader's cache stops being believed, and what the probe rebuilds on every round.
 * <p>
 * The first item is the one with an operator-facing wrong answer. The guard was written for the leader rendering
 * its own row and tested "the peer being rendered is the leader" rather than "the peer being rendered is this
 * node", so a FOLLOWER answering the endpoint published its own capabilities under the leader's id: on a rolling
 * upgrade the un-upgraded leader reads as capable, with no {@code version} field to contradict it, and the diff
 * the field exists to make possible names the wrong node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7301PeerCapabilityReportingTest {

  private static final String LEADER    = "arcadedb0";
  private static final String FOLLOWER  = "arcadedb1";
  private static final String TRUSTSTORE_DIR = "./target/test-truststore-7301";

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TRUSTSTORE_DIR));
  }

  // ---------------------------------------------------------------------------------------------------------
  // 1. A follower must not publish its own capabilities under the leader's row
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The registry is empty on a follower by design - only the leader probes - so the leader's row has nothing
   * true to carry, and an omitted field is the honest answer. Publishing the local set there tells an operator
   * that the node they did NOT ask can decode what the node they DID ask can.
   */
  @Test
  void aFollowerPublishesNothingUnderTheLeadersRow() {
    final JSONObject peerJson = new JSONObject();

    final boolean published = GetClusterHandler.putPeerCapabilities(peerJson, LEADER, FOLLOWER, null,
        Set.of(PeerCapabilities.SCHEMA_DELTA));

    assertThat(published).isFalse();
    assertThat(peerJson.has("capabilities"))
        .as("a follower has no answer about the leader, and the local node's answer is not one")
        .isFalse();
    assertThat(peerJson.has("version")).isFalse();
  }

  /**
   * The local row is the one every node can answer for, leader or not, so it is published either way - a true
   * statement in both roles, and the row an operator polling this node is actually asking about.
   */
  @Test
  void theLocalRowCarriesItsOwnCapabilitiesWhateverRoleThisNodeHolds() {
    final JSONObject peerJson = new JSONObject();

    final boolean published = GetClusterHandler.putPeerCapabilities(peerJson, FOLLOWER, FOLLOWER, null,
        Set.of(PeerCapabilities.SCHEMA_DELTA));

    assertThat(published).isTrue();
    assertThat(peerJson.getJSONArray("capabilities").toList()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
    assertThat(peerJson.getString("version"))
        .as("the same version a probing leader would have recorded for this node, so the two rows agree")
        .isEqualTo(Constants.getVersion());
  }

  /** A probed answer is published as given, and outranks anything the local node could say about itself. */
  @Test
  void aProbedAdvertisementIsPublishedAsGiven() {
    final JSONObject peerJson = new JSONObject();

    final boolean published = GetClusterHandler.putPeerCapabilities(peerJson, FOLLOWER, LEADER,
        new PeerCapabilityRegistry.Advertisement(Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1", 0L), Set.of());

    assertThat(published).isTrue();
    assertThat(peerJson.getJSONArray("capabilities").toList()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
    assertThat(peerJson.getString("version")).isEqualTo("26.10.1");
  }

  /** An older build answers with no version string; the field is omitted rather than reported empty. */
  @Test
  void anAdvertisementWithNoVersionOmitsTheField() {
    final JSONObject peerJson = new JSONObject();

    GetClusterHandler.putPeerCapabilities(peerJson, FOLLOWER, LEADER,
        new PeerCapabilityRegistry.Advertisement(Set.of(), "", 0L), Set.of(PeerCapabilities.SCHEMA_DELTA));

    assertThat(peerJson.getJSONArray("capabilities").toList()).isEmpty();
    assertThat(peerJson.has("version")).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------
  // 2. Advertisements from the previous leadership term are not inherited
  // ---------------------------------------------------------------------------------------------------------

  /**
   * A node that led, lost leadership and leads again used to believe the answers it took under the previous term
   * until its first refresh round completed - the window in which an optional wire-format section could be
   * written to a peer whose capabilities had not been re-confirmed.
   * <p>
   * The seeded entry is the LOCAL peer's, deliberately: the refresh round skips this node and
   * {@code retainOnly} keeps it (it is a configured peer), so nothing the background round does can remove it.
   * Only the invalidation can, which is what makes the assertion below say what it claims.
   */
  @Test
  void acquiringLeadershipDropsThePreviousTermsAdvertisements() {
    final RaftHAServer raft = newDetachedServer();
    // Nothing answers: the round this starts can only ever forget, never record.
    raft.setCapabilityProber((peerId, http, https, token) -> {
      throw new IOException("no answer");
    });

    final PeerCapabilityRegistry registry = raft.getPeerCapabilityRegistry();
    registry.record(registry.generation(), raft.getLocalPeerId().toString(), Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
    assertThat(registry.freshAdvertisementOf(raft.getLocalPeerId().toString())).isNotNull();

    raft.startCapabilityMonitor();
    try {
      assertThat(registry.freshAdvertisementOf(raft.getLocalPeerId().toString()))
          .as("an answer taken under a previous term is not inherited by the new one")
          .isNull();
    } finally {
      raft.stopCapabilityMonitor();
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // 3. The log-throttle shadow is pruned with the entry it shadows
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The throttle exists so a steady state - a peer permanently on an older build - does not print a line every
   * five seconds. It must not outlive the peer: held outside the registry, nothing pruned it, and a peer removed
   * and later re-added had its first advertisement suppressed as "unchanged" against what it said before it left.
   * That first advertisement is the one an operator watching a rejoin most wants to see.
   */
  @Test
  void aReAddedPeersFirstAdvertisementIsReportedAgain() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final Set<String> capabilities = Set.of(PeerCapabilities.SCHEMA_DELTA);

    assertThat(registry.record(registry.generation(), FOLLOWER, capabilities, "26.10.1")).as("the first answer is a change").isTrue();
    assertThat(registry.record(registry.generation(), FOLLOWER, capabilities, "26.10.1")).as("the same answer again is not").isFalse();

    assertThat(registry.forget(registry.generation(), FOLLOWER, "the probe failed")).as("the transition into failure is").isTrue();
    assertThat(registry.forget(registry.generation(), FOLLOWER, "the probe failed")).as("staying failed is not").isFalse();

    registry.retainOnly(registry.generation(), List.of(LEADER));
    assertThat(registry.record(registry.generation(), FOLLOWER, capabilities, "26.10.1"))
        .as("a peer that left and came back is new again, whatever it advertised before it left")
        .isTrue();
  }

  /**
   * The other half of the same window, raised in review of PR #7314. {@code stopCapabilityMonitor} ends the
   * refresh with {@code shutdownNow()} and does NOT wait for the round in flight, so a probe that was already
   * dialling when leadership was lost can answer after the next term has cleared the registry - recording the
   * previous term's answer over it, which is exactly what the clear exists to prevent.
   * <p>
   * Every write carries the generation its round started in, so the straggler is dropped. Closing it this way
   * rather than by waiting on the executor keeps a leadership transition from blocking on a network timeout.
   */
  @Test
  void anAnswerFromAnEndedLeadershipTermIsDropped() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long previousTerm = registry.generation();

    // The new term starts while the previous term's round is still dialling.
    registry.clear();
    assertThat(registry.generation()).isNotEqualTo(previousTerm);

    assertThat(registry.record(previousTerm, FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
        .as("the straggler is dropped, so it is not a transition to report either")
        .isFalse();
    assertThat(registry.freshAdvertisementOf(FOLLOWER))
        .as("and above all it is not believed: this is the window the clear exists to close")
        .isNull();

    assertThat(registry.forget(previousTerm, FOLLOWER, "the previous term's probe failed")).isFalse();
    assertThat(registry.unknownReasonOf(FOLLOWER))
        .as("nor does an ended term get to explain a peer this term has not asked about")
        .isNull();

    registry.record(registry.generation(), LEADER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
    registry.retainOnly(previousTerm, List.of(FOLLOWER));
    assertThat(registry.freshAdvertisementOf(LEADER))
        .as("a previous term's configuration cannot prune this term's answers either")
        .isNotNull();

    assertThat(registry.record(registry.generation(), FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
        .as("this term's own round writes normally")
        .isTrue();
  }

  /** Acquiring leadership clears the shadow too, or the first round of the new term reports nothing at all. */
  @Test
  void clearingTheRegistryAlsoClearsWhatWasLastReported() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();

    assertThat(registry.record(registry.generation(), FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1")).isTrue();
    registry.clear();

    assertThat(registry.freshAdvertisementOf(FOLLOWER)).isNull();
    assertThat(registry.unknownReasonOf(FOLLOWER)).as("a peer nobody has asked yet has nothing to explain").isNull();
    assertThat(registry.record(registry.generation(), FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
        .as("the first answer of the new term is reported, not swallowed as unchanged")
        .isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------
  // 4. The HTTPS client is built once, not once per peer per refresh period
  // ---------------------------------------------------------------------------------------------------------

  /**
   * Every probe used to build a fresh {@code SSLContext} from the truststore on disk - a file read, a
   * certificate-chain parse and an init - plus a fresh client whose connection pool it then threw away. Once per
   * peer, every five seconds, for the whole life of the leadership.
   */
  @Test
  void theHttpsClientIsReusedUntilTheTruststoreChanges() throws Exception {
    final File truststore = writeEmptyTruststore();
    final ArcadeDBServer server = serverWithTruststore(truststore, "changeit");
    final TrustedHttpClientCache cache = new TrustedHttpClientCache();

    final HttpClient first = cache.clientFor(server);
    assertThat(cache.clientFor(server))
        .as("an unchanged truststore costs a stat, not a certificate-chain parse")
        .isSameAs(first);

    // A rotation: same path, new bytes. The check has to notice, or a rotated certificate is never picked up.
    truststore.setLastModified(System.currentTimeMillis() + 5_000L);
    final HttpClient afterRotation = cache.clientFor(server);
    assertThat(afterRotation).as("a rotated truststore is picked up").isNotSameAs(first);

    // A different truststore is a different set of trust anchors, whatever its timestamps say. The password is
    // part of the material too, but it cannot be varied on its own here: a keystore's integrity check is computed
    // FROM the password, so a file opened with a different one does not load at all.
    final File other = new File(TRUSTSTORE_DIR, "rotated.jks");
    Files.copy(truststore.toPath(), other.toPath());
    assertThat(cache.clientFor(serverWithTruststore(other, "changeit"))).isNotSameAs(afterRotation);
  }

  /**
   * Raised in review of PR #7314: the cache was a bare {@code static}, and {@code BaseGraphServerTest} and every
   * HA suite start several servers in ONE JVM. Each server's probe would then see the other's trust material as a
   * change - rebuilding on every probe - and, since the request is sent outside the cache's monitor, could close a
   * client another server was still using. One cache per server is what makes "has the truststore changed" the
   * question it reads as.
   */
  @Test
  void twoServersInOneJvmDoNotInvalidateEachOthersClient() throws Exception {
    final File truststore = writeEmptyTruststore();
    final File other = new File(TRUSTSTORE_DIR, "other.jks");
    Files.copy(truststore.toPath(), other.toPath());

    final TrustedHttpClientCache first = new TrustedHttpClientCache();
    final TrustedHttpClientCache second = new TrustedHttpClientCache();
    final ArcadeDBServer firstServer = serverWithTruststore(truststore, "changeit");
    final ArcadeDBServer secondServer = serverWithTruststore(other, "changeit");

    final HttpClient firstClient = first.clientFor(firstServer);
    final HttpClient secondClient = second.clientFor(secondServer);

    assertThat(secondClient).as("each server builds its own from its own truststore").isNotSameAs(firstClient);
    assertThat(first.clientFor(firstServer))
        .as("and the other server's probe is not a reason to rebuild - or to close - this one's")
        .isSameAs(firstClient);
    assertThat(second.clientFor(secondServer)).isSameAs(secondClient);
  }

  /**
   * The client holds a connection pool and a selector thread, so a JVM that starts and stops many servers - what
   * the HA suites do - must not keep one per server that ever probed an HTTPS peer. {@code RaftHAServer.stop()}
   * closes it; this pins that closing works, is idempotent, and that the cache refuses to build another
   * afterwards - a client built after that single close() would be one nothing releases (PR #7314 review).
   */
  @Test
  void theClientIsReleasedOnCloseAndTheCacheRefusesToBuildAnother() throws Exception {
    final File truststore = writeEmptyTruststore();
    final ArcadeDBServer server = serverWithTruststore(truststore, "changeit");
    final TrustedHttpClientCache cache = new TrustedHttpClientCache();

    cache.clientFor(server);
    cache.close();
    cache.close();

    // A probe that outlived stopCapabilityMonitor()'s shutdownNow() can still reach the cache after stop() made
    // its one close() call. Building it a client then would leak the one thing this class exists to release, so
    // the closed cache refuses - which the refresh round handles exactly as it handles an unreachable peer.
    assertThatThrownBy(() -> cache.clientFor(server))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("shutting down");

    // A cache that never built anything has nothing to release, and must not fail saying so.
    new TrustedHttpClientCache().close();
  }

  private static File writeEmptyTruststore() throws Exception {
    final File dir = new File(TRUSTSTORE_DIR);
    dir.mkdirs();
    final File store = new File(dir, "truststore.jks");
    final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    try (final OutputStream out = Files.newOutputStream(store.toPath())) {
      ks.store(out, "changeit".toCharArray());
    }
    return store;
  }

  private static ArcadeDBServer serverWithTruststore(final File truststore, final String password) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststore.getAbsolutePath());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, password);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    return server;
  }

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private static RaftHAServer newDetachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
    return new RaftHAServer(mockServer, config);
  }
}
