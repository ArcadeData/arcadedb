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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9553: when every voter has quarantined the same database there is no copy left to resync
 * from, and the cluster used to say so only through one SEVERE line and one stalled resync loop per node.
 * <p>
 * Each node now advertises its quarantined databases on the capability poll every node already runs against every peer,
 * so any node can tell that every voter holds the database quarantined. That raises a dedicated critical alert naming
 * the way out, opens the accept-diverged override (#9449) on those nodes - the way out the alert names - and stops a
 * follower from retrying a resync from a leader that holds the same database quarantined and cannot serve it.
 */
class Issue9553AllVotersQuarantinedTest {

  private static final String DB    = "chaos";
  private static final String OTHER = "other";

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
  private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

  @TempDir
  Path root;

  // -- the wire: a node says what it holds quarantined --------------------------------------------------------------

  @Test
  void aNodeAdvertisesItsQuarantinedDatabasesSorted() {
    final JSONObject json = PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), false, Set.of(OTHER, DB));

    assertThat(json.getJSONArray(PostCapabilitiesHandler.QUARANTINED).toList()).containsExactly(DB, OTHER);
  }

  @Test
  void aHealthyNodesDocumentCarriesNoQuarantineField() {
    assertThat(PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), false, Set.of())
        .has(PostCapabilitiesHandler.QUARANTINED)).isFalse();
    assertThat(PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), true)
        .has(PostCapabilitiesHandler.QUARANTINED)).isFalse();
  }

  @Test
  void theQueryReadsTheQuarantinesAndAPeerThatPredatesThemReadsAsNone() throws Exception {
    final String url = "http://peer-b:2480/api/v1/cluster/capabilities";

    assertThat(PeerCapabilityQuery.parse(B.toString(),
        PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), false, Set.of(DB)).toString(), url).quarantined())
        .containsExactly(DB);
    assertThat(PeerCapabilityQuery.parse(B.toString(),
        "{\"peerId\":\"" + B + "\",\"version\":\"26.10.1\",\"capabilities\":[]}", url).quarantined())
        .as("a build that predates the field").isEmpty();
  }

  // -- what each node remembers about its peers ---------------------------------------------------------------------

  @Test
  void theRegistryRemembersWhatEachPeerHoldsQuarantined() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();
    registry.record(generation, B.toString(), Set.of(), "26.11.1", false, Set.of(DB));
    registry.record(generation, C.toString(), Set.of(), "26.11.1", false, Set.of());

    assertThat(registry.reportsQuarantined(B.toString(), DB)).isTrue();
    assertThat(registry.reportsQuarantined(B.toString(), OTHER)).isFalse();
    assertThat(registry.reportsQuarantined(C.toString(), DB)).isFalse();
    assertThat(registry.reportsQuarantined("never-asked", DB)).as("unknown is not quarantined").isFalse();

    registry.record(generation, B.toString(), Set.of(), "26.11.1", false, Set.of());
    assertThat(registry.reportsQuarantined(B.toString(), DB)).as("the resync landed, the next poll says so").isFalse();
  }

  @Test
  void aStaleAnswerIsNotBelieved() {
    final AtomicLong clock = new AtomicLong(1_000L);
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(5_000L);
    registry.setClock(clock::get);
    registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, Set.of(DB));

    clock.addAndGet(5_001L);
    assertThat(registry.reportsQuarantined(B.toString(), DB)).isFalse();
  }

  // -- the decision: every voter said so ----------------------------------------------------------------------------

  @Test
  void aDatabaseEveryVoterHoldsQuarantinedIsReported() {
    final PeerCapabilityRegistry registry = registryWhere(Set.of(DB, OTHER), Set.of(DB));

    assertThat(RaftHAServer.quarantinedOnEveryVoter(Set.of(DB, OTHER), voters(), SELF, registry))
        .as("only the database every voter reports").containsExactly(DB);
  }

  @Test
  void oneVoterWithAUsableCopyIsEnough() {
    assertThat(RaftHAServer.quarantinedOnEveryVoter(Set.of(DB), voters(), SELF, registryWhere(Set.of(DB), Set.of())))
        .isEmpty();
  }

  @Test
  void aVoterThatDidNotAnswerKeepsTheDatabaseOut() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, Set.of(DB));
    // C never answered: it may hold the only good copy, and "unknown" must not read as "quarantined"

    assertThat(RaftHAServer.quarantinedOnEveryVoter(Set.of(DB), voters(), SELF, registry)).isEmpty();
  }

  @Test
  void aVoterOnABuildThatDoesNotReportQuarantinesKeepsTheDatabaseOut() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, Set.of(DB));
    registry.record(registry.generation(), C.toString(), Set.of(), "26.10.1", false);

    assertThat(RaftHAServer.quarantinedOnEveryVoter(Set.of(DB), voters(), SELF, registry)).isEmpty();
  }

  @Test
  void aSoleVoterAndAHealthyNodeAreNeverReported() {
    final PeerCapabilityRegistry registry = registryWhere(Set.of(DB), Set.of(DB));

    assertThat(RaftHAServer.quarantinedOnEveryVoter(Set.of(DB), List.of(peer(SELF)), SELF, registry))
        .as("a sole voter has its own alert and override (#9308, #9449)").isEmpty();
    assertThat(RaftHAServer.quarantinedOnEveryVoter(Set.of(), voters(), SELF, registry))
        .as("this node holds no quarantine, so it holds a usable copy").isEmpty();
  }

  // -- the alert ----------------------------------------------------------------------------------------------------

  @Test
  void theAlertIsCriticalAndNamesTheWayOut() {
    final JSONArray alerts = new JSONArray();

    ClusterAlerts.addNoHealthyCopyAlert(List.of(DB), null, alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("no-healthy-copy-on-any-voter");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getString("message")).contains(DB);
    assertThat(alert.getString("recommendation")).contains(PostAcceptDivergedHandler.ROUTE).contains("backup")
        .contains("/api/v1/cluster/leader");
    assertThat(alert.getJSONObject("details").getJSONArray("databases").toList()).containsExactly(DB);
  }

  @Test
  void theAlertIsScopedToTheDatabasesTheCallerMaySee() {
    final JSONArray hidden = new JSONArray();
    ClusterAlerts.addNoHealthyCopyAlert(List.of(DB), Set.of(OTHER), hidden);
    assertThat(hidden.length()).as("a caller authorized on none of them has nothing to read").isZero();

    final JSONArray scoped = new JSONArray();
    ClusterAlerts.addNoHealthyCopyAlert(List.of(DB, OTHER), Set.of(OTHER), scoped);
    assertThat(scoped.getJSONObject(0).getJSONObject("details").getJSONArray("databases").toList())
        .containsExactly(OTHER);
    assertThat(scoped.getJSONObject(0).getString("message")).doesNotContain(DB);
  }

  @Test
  void noAlertWhenNothingIsQuarantinedEverywhere() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addNoHealthyCopyAlert(List.of(), null, alerts);
    assertThat(alerts.length()).isZero();
  }

  /** The caller: the status document's alert scan raises it from a real state machine and the peers' answers. */
  @Test
  void theClusterScanRaisesTheAlertWhenEveryVoterReportsTheQuarantine() throws Exception {
    final ArcadeDBServer server = newServer();
    final ArcadeStateMachine sm = newStateMachine(server);
    try {
      sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
      final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of(DB)));
      sm.setRaftHAServer(raft);

      assertThat(alertIds(ClusterAlerts.scan(server, sm))).contains("no-healthy-copy-on-any-voter")
          .as("each node's own resync state is still reported").contains("local-resync-in-progress");

      raft.peerCapabilityRegistry(registryWhere(Set.of(DB), Set.of()));
      assertThat(alertIds(ClusterAlerts.scan(server, sm))).as("C serves a copy again")
          .doesNotContain("no-healthy-copy-on-any-voter");
    } finally {
      sm.close();
    }
  }

  // -- the way out: accept one copy ---------------------------------------------------------------------------------

  @Test
  void theOverrideIsOpenWhenEveryVoterHoldsTheDatabaseQuarantined() throws Exception {
    final ArcadeDBServer server = newServer();
    final ArcadeStateMachine sm = newStateMachine(server);
    try {
      sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
      final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of(DB)));

      final JSONObject result = pluginOf(raft, server).acceptDivergedDatabase(DB, "user 'root'");

      assertThat(result.getString("database")).isEqualTo(DB);
      assertThat(sm.isDatabaseDiverged(DB)).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();
    } finally {
      sm.close();
    }
  }

  @Test
  void theOverrideStaysClosedWhileOneVoterHoldsAUsableCopy() throws Exception {
    final ArcadeDBServer server = newServer();
    final ArcadeStateMachine sm = newStateMachine(server);
    try {
      sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
      final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of()));

      assertThatThrownBy(() -> pluginOf(raft, server).acceptDivergedDatabase(DB, "user 'root'"))
          .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
          .hasMessageContaining("not every voter");
      assertThat(sm.isDatabaseDiverged(DB)).isTrue();
    } finally {
      sm.close();
    }
  }

  // -- no resync from a leader that cannot serve it -----------------------------------------------------------------

  @Test
  void theDatabasesTheLeaderHoldsQuarantinedAreLeftOutOfTheResync() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, Set.of(DB));

    assertThat(ArcadeStateMachine.resyncableFromLeader(Set.of(DB, OTHER), B.toString(), registry))
        .containsExactly(OTHER);
    assertThat(ArcadeStateMachine.resyncableFromLeader(Set.of(DB), C.toString(), registry))
        .as("a leader that did not answer is not assumed quarantined").containsExactly(DB);
    assertThat(ArcadeStateMachine.resyncableFromLeader(Set.of(DB), null, registry)).containsExactly(DB);
  }

  /** The caller: the health tick does not even take its throttle slot for a resync the leader can only refuse. */
  @Test
  void theHealthTickDoesNotRetryAResyncFromALeaderThatHoldsTheSameQuarantine() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();
    try {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, Set.of(DB));
      sm.setRaftHAServer(followerOf(B, registry));
      sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);

      sm.retryUnfilledSnapshotGap();
      assertThat(readLastRetryMs(sm)).as("nothing a resync from this leader could restore").isZero();

      registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, Set.of());
      sm.retryUnfilledSnapshotGap();
      assertThat(readLastRetryMs(sm)).as("the leader serves it again: the very next tick retries").isNotZero();
    } finally {
      sm.close();
    }
  }

  // -- helpers ------------------------------------------------------------------------------------------------------

  /** A registry in which B answered {@code quarantinedOnB} and C answered {@code quarantinedOnC}. */
  private static PeerCapabilityRegistry registryWhere(final Set<String> quarantinedOnB, final Set<String> quarantinedOnC) {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.record(registry.generation(), B.toString(), Set.of(), "26.11.1", false, quarantinedOnB);
    registry.record(registry.generation(), C.toString(), Set.of(), "26.11.1", false, quarantinedOnC);
    return registry;
  }

  private static FakeRaftHAServer clusterOf(final ArcadeStateMachine sm, final PeerCapabilityRegistry registry) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.localPeerId(SELF);
    raft.livePeers(voters());
    raft.stateMachine(sm);
    raft.peerCapabilityRegistry(registry);
    return raft;
  }

  /** The plugin the HTTP route and the gRPC RPC both end at, wired to the fake cluster. */
  private static RaftHAPlugin pluginOf(final FakeRaftHAServer raft, final ArcadeDBServer server) {
    final RaftHAPlugin plugin = raft.plugin();
    plugin.configure(server, server.getConfiguration());
    return plugin;
  }

  private static FakeRaftHAServer followerOf(final RaftPeerId leader, final PeerCapabilityRegistry registry) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.localPeerId(SELF);
    raft.leaderId(leader);
    raft.peerHttpAddress(leader, "peer-b:2480");
    raft.peerCapabilityRegistry(registry);
    return raft;
  }

  private ArcadeStateMachine newStateMachine() {
    return newStateMachine(newServer());
  }

  private static ArcadeStateMachine newStateMachine(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private ArcadeDBServer newServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    return new ArcadeDBServer(config);
  }

  private static List<Object> alertIds(final JSONArray alerts) {
    final JSONArray ids = new JSONArray();
    for (int i = 0; i < alerts.length(); i++)
      ids.put(alerts.getJSONObject(i).getString("id"));
    return ids.toList();
  }

  private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
    f.setAccessible(true);
    return ((AtomicLong) f.get(sm)).get();
  }

  private static List<RaftPeer> voters() {
    return List.of(peer(SELF), peer(B), peer(C));
  }

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
  }
}
