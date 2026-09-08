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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7219: a mixed-version cluster, covered by a test rather than by an operator instruction.
 * <p>
 * #7211 gave {@code SCHEMA_ENTRY} an optional trailing schema-delta section. A node whose build predates it stops
 * decoding before the section, sees an entry with an EMPTY {@code schemaJson}, applies nothing, reports nothing,
 * and diverges. The only thing standing between a cluster and that outcome was {@code arcadedb.ha.schemaDelta}
 * being off by default plus an operator upgrading every node before turning it on - an instruction nothing
 * enforced. This test is the enforcement.
 * <p>
 * <b>How a mixed-version cluster is built inside one JVM.</b> Every node here runs the same classes, so "old
 * build" is simulated at the one place a build's identity actually enters the protocol:
 * {@link RaftHAServer#setAdvertisedCapabilities} makes a node answer the capability RPC exactly as a build with no
 * delta decoder would - it says it cannot decode one. That is a faithful stand-in for the case that matters,
 * because the leader's decision consults nothing else: not a version string, not a build number, only what the
 * peer said it can decode. The one case it does NOT reproduce is a peer with no capability route at all, which
 * answers 404; that arm is covered by {@code Issue7219PeerCapabilityRegistryTest} (a failed probe forgets the
 * peer, and a forgotten peer blocks the capability), because standing up a second, older jar is not something a
 * single-JVM test can do.
 * <p>
 * Note what this class does NOT configure: {@code arcadedb.ha.schemaDelta}. Deltas ship here on the default,
 * which is the third acceptance point of the issue.
 */
@Tag("slow")
class Issue7219MixedVersionSchemaDeltaIT extends BaseRaftHATest {

  private static final int SEED_TYPES  = 60;
  private static final int DDL_CHANGES = 8;

  /** How long the leader is given to notice a peer's advertisement change (a few refresh rounds). */
  private static final long CAPABILITY_CONVERGENCE_TIMEOUT_MS = 30_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // arcadedb.ha.schemaDelta is deliberately NOT set: since #7219 it defaults to on, and the negotiation below
    // is what makes that safe. A test that turned it on by hand could not tell the two apart.
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aPeerThatCannotDecodeADeltaKeepsGettingWholeDocuments() throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    // The replica now behaves like a node whose build predates the delta section.
    raftServerOf(replicaIndex).setAdvertisedCapabilities(Set.of());
    awaitObservedIncapablePeer(leaderIndex, peerIdForIndex(replicaIndex));

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    requireAPristineDatabase(leaderDb, "MixedSeed_0");
    seedSchema(leaderDb, "MixedSeed_");

    final long deltasBefore = schemaDeltasShipped();
    final long documentsBefore = schemaDocumentsShipped();

    for (int i = 0; i < DDL_CHANGES; i++) {
      final int typeIndex = i;
      leaderDb.transaction(() -> leaderDb.getSchema().getType("MixedSeed_" + typeIndex)
          .createProperty("added_" + typeIndex, Type.INTEGER));
    }

    assertThat(schemaDeltasShipped() - deltasBefore)
        .as("not one DDL may go out as a delta while a peer has not advertised that it can decode one - that "
            + "entry would apply nothing on the peer and diverge silently")
        .isZero();
    assertThat(schemaDocumentsShipped() - documentsBefore)
        .as("every DDL fell back to the whole document")
        .isGreaterThanOrEqualTo(DDL_CHANGES);

    // The fallback is not just quiet, it is correct: the peer that could not read a delta still has the schema.
    assertClusterConsistency();
    final Schema replicaSchema = getServerDatabase(replicaIndex, getDatabaseName()).getSchema();
    for (int i = 0; i < DDL_CHANGES; i++)
      assertThat(replicaSchema.getType("MixedSeed_" + i).existsProperty("added_" + i))
          .as("DDL %d reached the peer that cannot decode deltas", i)
          .isTrue();
  }

  @Test
  void deltasResumeByThemselvesOnceEveryPeerAdvertisesTheCapability() throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    // Start the cluster in the mixed-version state, then "finish the rolling upgrade" mid-test. No leader
    // restart, no setting change, no operator step: the point of the issue is that the cluster works this out.
    raftServerOf(replicaIndex).setAdvertisedCapabilities(Set.of());
    awaitObservedIncapablePeer(leaderIndex, peerIdForIndex(replicaIndex));

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    requireAPristineDatabase(leaderDb, "UpgradeSeed_0");
    seedSchema(leaderDb, "UpgradeSeed_");

    raftServerOf(replicaIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    awaitObservedCapablePeer(leaderIndex, peerIdForIndex(replicaIndex));

    // The first change after the upgrade re-primes the diff base and is a whole document BY DESIGN, and asserting
    // that is the second half of the fix: rememberReplicatedSchema releases the base through the very same
    // predicate the emission gate reads, so a cluster that cannot ship a delta is also not paying to hold the
    // multi-MB document it would have diffed against. If the two ever drift apart this assertion is what says so.
    final long documentsBeforePrimer = schemaDocumentsShipped();
    final long deltasBeforePrimer = schemaDeltasShipped();
    leaderDb.transaction(() -> leaderDb.getSchema().getType("UpgradeSeed_0").createProperty("primer", Type.STRING));
    assertThat(schemaDocumentsShipped() - documentsBeforePrimer)
        .as("the base was released while deltas were withheld, so the first change after the upgrade re-primes it")
        .isGreaterThan(0);
    assertThat(schemaDeltasShipped() - deltasBeforePrimer)
        .as("and it cannot have been a delta - there was nothing to diff against")
        .isZero();

    final long deltasBefore = schemaDeltasShipped();
    for (int i = 1; i <= DDL_CHANGES; i++) {
      final int typeIndex = i;
      leaderDb.transaction(() -> leaderDb.getSchema().getType("UpgradeSeed_" + typeIndex)
          .createProperty("added_" + typeIndex, Type.INTEGER));
    }

    assertThat(schemaDeltasShipped() - deltasBefore)
        .as("with every peer advertising the capability the leader ships deltas again, without anyone touching "
            + GlobalConfiguration.HA_SCHEMA_DELTA.getKey())
        .isGreaterThanOrEqualTo(DDL_CHANGES - 1);

    assertClusterConsistency();
    final Schema replicaSchema = getServerDatabase(replicaIndex, getDatabaseName()).getSchema();
    for (int i = 1; i <= DDL_CHANGES; i++)
      assertThat(replicaSchema.getType("UpgradeSeed_" + i).existsProperty("added_" + i))
          .as("the property added by delta %d reached the replica", i)
          .isTrue();
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    assertThat(plugin).as("server %d runs the Raft HA plugin", serverIndex).isNotNull();
    assertThat(plugin.getRaftHAServer()).as("server %d has started its Raft server", serverIndex).isNotNull();
    return plugin.getRaftHAServer();
  }

  /**
   * Waits until the leader has actually ASKED {@code peerId} and been told it cannot decode a delta.
   * <p>
   * Deliberately not "until the leader reports the peer as missing the capability": a peer the leader has never
   * successfully probed reports exactly the same way, so a run in which the capability RPC was broken end to end
   * would satisfy that weaker condition instantly - and the whole-document assertions that follow would then pass
   * for the wrong reason, proving nothing about negotiation. Waiting for a recorded, empty advertisement means
   * the probe demonstrably worked and the answer it carried is the one the test planted.
   */
  private void awaitObservedIncapablePeer(final int leaderIndex, final String peerId) throws InterruptedException {
    awaitObservedCapabilities(leaderIndex, peerId,
        capabilities -> capabilities != null && !capabilities.contains(PeerCapabilities.SCHEMA_DELTA),
        "the leader must probe peer '" + peerId + "' and be told it cannot decode " + PeerCapabilities.SCHEMA_DELTA);
  }

  /** Waits until the leader has been told by {@code peerId} that it CAN decode a delta, and agrees. */
  private void awaitObservedCapablePeer(final int leaderIndex, final String peerId) throws InterruptedException {
    awaitObservedCapabilities(leaderIndex, peerId,
        capabilities -> capabilities != null && capabilities.contains(PeerCapabilities.SCHEMA_DELTA),
        "the leader must probe peer '" + peerId + "' and be told it can decode " + PeerCapabilities.SCHEMA_DELTA);
    assertThat(raftServerOf(leaderIndex).peersMissingCapability(PeerCapabilities.SCHEMA_DELTA))
        .as("and with that answer in hand the leader must consider the whole cluster covered")
        .isEmpty();
  }

  private void awaitObservedCapabilities(final int leaderIndex, final String peerId,
      final Predicate<Set<String>> settled, final String what) throws InterruptedException {
    final RaftHAServer leader = raftServerOf(leaderIndex);
    final long deadline = System.currentTimeMillis() + CAPABILITY_CONVERGENCE_TIMEOUT_MS;
    Set<String> observed = observedCapabilities(leader, peerId);
    while (!settled.test(observed) && leader.isLeader() && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      observed = observedCapabilities(leader, peerId);
    }
    // Only the leader polls for capabilities, so a node that lost leadership mid-test stops refreshing and its
    // advertisements go stale - which would otherwise read here as "the negotiation is broken" and cost the next
    // reader half an hour. This test pins one leader (its DDL has to run there) so an election is a failed
    // PRECONDITION, and it says so rather than timing out silently.
    assertThat(leader.isLeader())
        .as("this test pins the leader it elected at the start; %s lost leadership mid-test, so the capability "
            + "refresh it drives stopped", leader.getLocalPeerId())
        .isTrue();
    // A wall-clock bound used as a hang detector, not as a latency assertion: the refresh runs every
    // PeerCapabilityRegistry.REFRESH_PERIOD_MS, so a budget of several rounds only ever fires when the leader
    // stopped asking altogether.
    assertThat(settled.test(observed))
        .as(what + " (leader is %s, last observed advertisement: %s)", leader.getLocalPeerId(), observed)
        .isTrue();
  }

  private static Set<String> observedCapabilities(final RaftHAServer leader, final String peerId) {
    final PeerCapabilityRegistry.Advertisement advertisement =
        leader.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
    return advertisement != null ? advertisement.capabilities() : null;
  }

  private void seedSchema(final Database leaderDb, final String prefix) {
    leaderDb.transaction(() -> {
      final Schema schema = leaderDb.getSchema();
      for (int i = 0; i < SEED_TYPES; i++) {
        final DocumentType type = schema.createVertexType(prefix + i);
        type.createProperty("id_" + i, Type.STRING);
        type.createProperty("name_" + i, Type.STRING);
        type.createProperty("payload_" + i, Type.STRING);
      }
    });
  }

  /** Same guard as {@code Issue6989SchemaDeltaReplicationIT}: the counters are only readable from a clean base. */
  private void requireAPristineDatabase(final Database leaderDb, final String firstSeededType) {
    assertThat(leaderDb.getSchema().existsType(firstSeededType))
        .as("this test seeds its own schema and needs a pristine database; remove the leftover "
            + "ha-raft/target/databases* directories an aborted run left behind")
        .isFalse();
  }

  /** The leader can move between tests, so ask every node rather than guessing which one shipped. */
  private long schemaDeltasShipped() {
    return sumOverNodes(RaftReplicatedDatabase::getSchemaDeltasShipped);
  }

  private long schemaDocumentsShipped() {
    return sumOverNodes(RaftReplicatedDatabase::getSchemaDocumentsShipped);
  }

  private long sumOverNodes(final ToLongFunction<RaftReplicatedDatabase> counter) {
    long total = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal wrapped =
          ((DatabaseInternal) getServerDatabase(i, getDatabaseName())).getWrappedDatabaseInstance();
      if (wrapped instanceof final RaftReplicatedDatabase raft)
        total += counter.applyAsLong(raft);
    }
    return total;
  }
}
