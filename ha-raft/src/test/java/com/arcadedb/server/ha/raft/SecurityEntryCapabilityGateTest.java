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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the peer capability gate that decides whether a security entry (and its precondition) may be sent to the cluster.
 *
 * <p>Regression coverage for #7511:
 * Issue #7511: a group or API-token change committed on an already-upgraded node halted every node older than the
 * two Raft log entry types #7373 added.
 * <p>
 * The halt itself is correct and stays - {@code ArcadeStateMachine} must not skip a committed entry it cannot
 * decode (#4798), because skipping would diverge the cluster's security state silently. What was missing is
 * anything that stops the entry being written while a peer in the configuration cannot read it, which during a
 * rolling upgrade is the normal state of the cluster. The fix refuses the operation instead, so nothing is
 * submitted and nothing halts.
 * <p>
 * The interesting half is the LAST test: a gate helper nothing calls is a gate that is not there, so
 * {@code replicateSecurityGroups} and {@code replicateSecurityApiTokens} - the single chokepoint every HTTP and
 * gRPC entry point funnels through - are driven directly, and the assertion is that the transaction broker was
 * never handed anything.
 *
 * <p>Regression coverage for #7509:
 * Issue #7509's rolling-upgrade guard: the compare-and-set precondition is written only while EVERY peer has
 * proved it can read one.
 * <p>
 * Without the gate a mixed cluster is worse off than an ungated one, not better. A peer that predates the
 * section skips it and installs the document unconditionally, so a losing entry would be REFUSED on the
 * upgraded nodes and APPLIED on the older one: the security state diverges, and the same credentials then
 * resolve differently depending on which node answers - the failure #7373 exists to prevent. Withholding the
 * precondition keeps the pre-#7509 behaviour uniform instead, and the check resumes by itself once the last
 * node is upgraded.
 */
class SecurityEntryCapabilityGateTest {

  private static final String LAGGING_PEER = "arcadedb2";

  // -------------------------------------------------------------------------------------------------------
  // 1. The vocabulary: the tokens exist, are advertised, and keep their exact spelling
  // -------------------------------------------------------------------------------------------------------

  // Regression for #7511: the two entry types of issue7373 have stable capability tokens
  /**
   * The spellings are pinned deliberately. {@code PeerCapabilities} states that a token which has shipped keeps
   * its exact spelling for as long as any supported version might advertise it: renaming one makes every older
   * peer read as incapable, which for THIS consumer does not silently disable a feature - it refuses every group
   * change and every token mint in the cluster.
   */
  @Test
  void theTwoConfigEntryTypesHaveStableCapabilityTokens() {
    assertThat(PeerCapabilities.SECURITY_GROUPS_ENTRY).isEqualTo("security-groups-entry");
    assertThat(PeerCapabilities.SECURITY_API_TOKENS_ENTRY).isEqualTo("security-api-tokens-entry");
  }

  // Regression for #7511: this build advertises both entry types alongside the schema delta
  /**
   * A capability names what THIS build can decode, and this build has both applies
   * ({@code ArcadeStateMachine.applySecurityGroupsEntry} / {@code applySecurityApiTokensEntry}), so it must say
   * so. A build that decoded the entries without advertising them would deadlock a cluster of its own peers:
   * every node capable, none of them saying it, so no group could ever be created.
   */
  @Test
  void thisBuildAdvertisesBothEntryTypesAlongsideTheSchemaDelta() {
    assertThat(PeerCapabilities.LOCAL).containsExactlyInAnyOrder(PeerCapabilities.SCHEMA_DELTA,
        PeerCapabilities.SECURITY_GROUPS_ENTRY, PeerCapabilities.SECURITY_API_TOKENS_ENTRY,
        PeerCapabilities.SECURITY_PRECONDITION, PeerCapabilities.TX_PREPARED_AT_INDEX,
        PeerCapabilities.SCHEMA_FILE_RENAMES, PeerCapabilities.DATABASE_CONFIGURATION_ENTRY,
        PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS, PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS, PeerCapabilities.RESTORE_FENCE);
  }

  // -------------------------------------------------------------------------------------------------------
  // 2. Which entry types need a token, and which deliberately do not
  // -------------------------------------------------------------------------------------------------------

  // Regression for #7511: only the two entry types issue7373 added require a capability
  @Test
  void onlyTheTwoConfigEntryTypesAddedRequireACapability() {
    assertThat(SecurityEntryCapabilityGate.capabilityFor(RaftLogEntryType.SECURITY_GROUPS_ENTRY))
        .isEqualTo(PeerCapabilities.SECURITY_GROUPS_ENTRY);
    assertThat(SecurityEntryCapabilityGate.capabilityFor(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY))
        .isEqualTo(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  // Regression for #7511: every entry type that predates the mechanism is deliberately ungated
  /**
   * Ids 1-5 came in with the Raft HA work itself, so no build that has ever spoken this protocol fails to decode
   * them. Id 6 shipped in 26.5.1 and is written only at first cluster formation, where refusing it would stop the
   * cluster forming rather than protect a peer - the opposite of what the gate is for.
   * <p>
   * Asserted over {@code values()} rather than by listing six constants, so this test is the one that fails the
   * day a ninth entry type is added without a decision about it - alongside the exhaustive switch in
   * {@code capabilityFor}, which fails the compile.
   */
  @Test
  void everyEntryTypeThatPredatesTheMechanismIsDeliberatelyUngated() {
    for (final RaftLogEntryType type : RaftLogEntryType.values()) {
      if (type == RaftLogEntryType.SECURITY_GROUPS_ENTRY || type == RaftLogEntryType.SECURITY_API_TOKENS_ENTRY
          || type == RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY)
        continue;
      assertThat(SecurityEntryCapabilityGate.capabilityFor(type))
          .as("%s (id %d) predates the capability mechanism and must not be gated", type, type.getId())
          .isNull();
    }
  }

  // Regression for #7511: an ungated entry type never queries the peers
  /** An ungated type never asks the cluster anything, so it cannot be refused - or slowed down - by this gate. */
  @Test
  void anUngatedEntryTypeNeverQueriesThePeers() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();

    SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY, "bootstrap fingerprint");

    assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
  }

  // -------------------------------------------------------------------------------------------------------
  // 3. The decision
  // -------------------------------------------------------------------------------------------------------

  // Regression for #7511: a cluster where every peer advertises the capability is not refused
  @Test
  void aClusterWhereEveryPeerAdvertisesTheCapabilityIsNotRefused() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_GROUPS_ENTRY) ? List.of() : List.of());

    assertThatCode(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document")).doesNotThrowAnyException();
  }

  // Regression for #7511: a peer that has not advertised the capability refuses the group change
  @Test
  void aPeerThatHasNotAdvertisedTheCapabilityRefusesTheGroupChange() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_GROUPS_ENTRY) ? List.of(LAGGING_PEER) : List.of());
    raft.peerCapabilityRegistry(registryWhereThePeerAnswered404());

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(LAGGING_PEER)
        .hasMessageContaining(PeerCapabilities.SECURITY_GROUPS_ENTRY);
  }

  // Regression for #7511: a peer that has not advertised the capability refuses the token change too
  @Test
  void aPeerThatHasNotAdvertisedTheCapabilityRefusesTheTokenChangeToo() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_API_TOKENS_ENTRY) ? List.of(LAGGING_PEER) : List.of());

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(true), raft,
        RaftLogEntryType.SECURITY_API_TOKENS_ENTRY, "API-token document"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(PeerCapabilities.SECURITY_API_TOKENS_ENTRY);
  }

  // Regression for #7511: the gate is on when no server configuration can be read
  /**
   * The interlock is on by default and stays on when nothing was wired: a gate that fails open because a field
   * was null is the failure this class exists to prevent.
   */
  @Test
  void theGateIsOnWhenNoServerConfigurationCanBeRead() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_GROUPS_ENTRY) ? List.of(LAGGING_PEER) : List.of());

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(null, raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);
  }

  // Regression for #7511: the interlock can be turned off for an operator who knows better
  /** Turned off, the operation goes through as it did before #7511 - and the peers are not even asked. */
  @Test
  void theInterlockCanBeTurnedOffForAnOperatorWhoKnowsBetter() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_GROUPS_ENTRY) ? List.of(LAGGING_PEER) : List.of());

    assertThatCode(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(false), raft,
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, "group document")).doesNotThrowAnyException();

    assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
  }

  // Regression for #7511: a database configuration entry is refused even with the security override switched off
  /**
   * Issue #11195: the override is the security operations' escape hatch, so switching it off must not unlock an
   * ALTER DATABASE, which can wait for the peer. The gate asks the peers and refuses, whatever the setting says.
   */
  @Test
  void aDatabaseConfigurationEntryIsRefusedEvenWithTheSecurityOverrideSwitchedOff() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.DATABASE_CONFIGURATION_ENTRY) ? List.of(LAGGING_PEER) : List.of());

    assertThatThrownBy(() -> SecurityEntryCapabilityGate.requireEveryPeerCanDecode(serverWithGate(false), raft,
        RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY, "configuration of database 'x'"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(LAGGING_PEER)
        .hasMessageNotContaining(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey().concat("=false"));

    assertThat(raft.calls("peersMissingCapabilityNow")).isNotEmpty();
  }

  // Regression for #7511: the refusal of a database configuration entry does not name the security override
  /** The refusal of a non-overridable entry must not send an operator to the security setting. */
  @Test
  void theRefusalOfADatabaseConfigurationEntryDoesNotNameTheSecurityOverride() {
    final String message = SecurityEntryCapabilityGate.refusal("configuration of database 'x'",
        RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY, PeerCapabilities.DATABASE_CONFIGURATION_ENTRY,
        List.of(LAGGING_PEER), registryWhereThePeerAnswered404());

    assertThat(message).contains(LAGGING_PEER).contains("no setting");
    assertThat(message).doesNotContain(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey());
  }

  // Regression for #7511: only the security entries are overridable
  /** Only the two security entries carry the operator override. */
  @Test
  void onlyTheSecurityEntriesAreOverridable() {
    assertThat(SecurityEntryCapabilityGate.isOverridable(RaftLogEntryType.SECURITY_GROUPS_ENTRY)).isTrue();
    assertThat(SecurityEntryCapabilityGate.isOverridable(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY)).isTrue();
    assertThat(SecurityEntryCapabilityGate.isOverridable(RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY)).isFalse();
    assertThat(SecurityEntryCapabilityGate.isOverridable(RaftLogEntryType.SCHEMA_ENTRY)).isFalse();
    assertThat(SecurityEntryCapabilityGate.isOverridable(RaftLogEntryType.TX_ENTRY)).isFalse();
  }

  // -------------------------------------------------------------------------------------------------------
  // 4. The sentence an operator is left holding
  // -------------------------------------------------------------------------------------------------------

  // Regression for #7511: the refusal names the peer the reason and the override
  /**
   * The refusal is the entire remedy this issue delivers. "The cluster is not ready" sends an operator nowhere;
   * the peer's name, the reason its answer is missing, and the knob that overrides the decision are the three
   * things they can act on, so all three are pinned.
   */
  @Test
  void theRefusalNamesThePeerTheReasonAndTheOverride() {
    final String message = SecurityEntryCapabilityGate.refusal("group document",
        RaftLogEntryType.SECURITY_GROUPS_ENTRY, PeerCapabilities.SECURITY_GROUPS_ENTRY, List.of(LAGGING_PEER),
        registryWhereThePeerAnswered404());

    assertThat(message).contains(LAGGING_PEER);
    assertThat(message).contains(PeerCapabilities.SECURITY_GROUPS_ENTRY);
    assertThat(message).contains("SECURITY_GROUPS_ENTRY");
    assertThat(message)
        .as("an operator must be told the reason the peer is unknown: an old build and an unreachable node have "
            + "nothing in common as remedies")
        .contains("404");
    assertThat(message).contains(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey());
    assertThat(message)
        .as("nothing was submitted is the half that decides whether the operator has to clean anything up")
        .containsIgnoringCase("nothing was submitted");
  }

  // Regression for #7511: the refusal still names a peer whose reason was never recorded
  /** A peer with no recorded reason still has to be named; only the explanatory clause is dropped. */
  @Test
  void theRefusalStillNamesAPeerWhoseReasonWasNeverRecorded() {
    final String message = SecurityEntryCapabilityGate.refusal("API-token document",
        RaftLogEntryType.SECURITY_API_TOKENS_ENTRY, PeerCapabilities.SECURITY_API_TOKENS_ENTRY,
        List.of(LAGGING_PEER), new PeerCapabilityRegistry());

    assertThat(message).contains(LAGGING_PEER);
    assertThat(message).contains(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE.getKey());
  }

  // Regression for #7511: a refusal is reported once and then throttled per capability
  /**
   * The refusal is also reported in this node's own log, because the caller may be a deployment script that
   * swallows the 409 while the operator watching the rolling upgrade is the one who has to act (PR #7555 review).
   * Throttled per capability, so a script retrying in a loop cannot bury the line that matters - and per
   * CAPABILITY rather than globally, so a group refusal does not silence the token refusal behind it.
   */
  @Test
  void aRefusalIsReportedOnceAndThenThrottledPerCapability() {
    SecurityEntryCapabilityGate.resetRefusalLogThrottle();
    final long t0 = 1_000_000L;

    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_GROUPS_ENTRY, t0))
        .as("the first refusal is always reported").isTrue();
    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_GROUPS_ENTRY, t0 + 1_000L))
        .as("a retry a second later is the same condition and is not reported again").isFalse();
    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_API_TOKENS_ENTRY, t0 + 1_000L))
        .as("but the other capability's first refusal is its own event, not a repeat of this one").isTrue();
    assertThat(SecurityEntryCapabilityGate.shouldLogRefusal(PeerCapabilities.SECURITY_GROUPS_ENTRY, t0 + 60_001L))
        .as("and once the window has passed the condition is reported again").isTrue();
  }

  // -------------------------------------------------------------------------------------------------------
  // 5. The caller: nothing is submitted on a refusal
  // -------------------------------------------------------------------------------------------------------

  // Regression for #7511: a refused group change submits nothing to the raft log
  /**
   * The bug is not "the gate says no", it is "the entry reaches the Raft log". Driven through the plugin method
   * every HTTP and gRPC entry point ends at, so the assertion is the one that matters: the broker - the only
   * thing in {@code src/main} that encodes either entry type - was never handed the payload.
   */
  @Test
  void aRefusedGroupChangeSubmitsNothingToTheRaftLog() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_GROUPS_ENTRY) ? List.of(LAGGING_PEER) : List.of());

    final RaftHAPlugin plugin = pluginOn(raft, true);

    assertThatThrownBy(() -> plugin.replicateSecurityGroups("{\"databases\":{}}"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    assertThat(broker.calls("replicateSecurityGroups")).isEmpty();
  }

  // Regression for #7511: a refused api token change submits nothing to the raft log
  /**
   * The same for a token document, which is the case that matters most: a REVOCATION whose entry halts the nodes
   * still serving the token has not revoked anything.
   */
  @Test
  void aRefusedApiTokenChangeSubmitsNothingToTheRaftLog() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    raft.on("peersMissingCapabilityNow", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_API_TOKENS_ENTRY) ? List.of(LAGGING_PEER) : List.of());

    final RaftHAPlugin plugin = pluginOn(raft, true);

    assertThatThrownBy(() -> plugin.replicateSecurityApiTokens("{\"version\":1,\"tokens\":[]}"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class);

    assertThat(broker.calls("replicateSecurityApiTokens")).isEmpty();
  }

  // Regression for #7511: a fully upgraded cluster still replicates both documents
  /** The other direction, so the test above cannot pass because the plugin submits nothing under any condition. */
  @Test
  void aFullyUpgradedClusterStillReplicatesBothDocuments() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapabilityNow", List.of());

    final RaftHAPlugin plugin = pluginOn(raft, true);

    plugin.replicateSecurityGroups("{\"databases\":{}}");
    plugin.replicateSecurityApiTokens("{\"version\":1,\"tokens\":[]}");

    assertThat(broker.calls("replicateSecurityGroups")).containsOnlyOnce(Arrays.asList("{\"databases\":{}}", null));
    assertThat(broker.calls("replicateSecurityApiTokens")).containsOnlyOnce(Arrays.asList("{\"version\":1,\"tokens\":[]}", null));
  }

  // Regression for #7511: the users entry is not gated and still replicates on a mixed cluster
  /**
   * {@code SECURITY_USERS_ENTRY} predates every build this cluster can contain, so the users path must not have
   * acquired a gate along with the other two - a rolling upgrade would otherwise stop being able to change a
   * password, which is strictly worse than what #7511 set out to fix.
   */
  @Test
  void theUsersEntryIsNotGatedAndStillReplicatesOnAMixedCluster() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapabilityNow", List.of(LAGGING_PEER));

    final RaftHAPlugin plugin = pluginOn(raft, true);

    plugin.replicateSecurityUsers("[{\"name\":\"root\"}]");

    assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList("[{\"name\":\"root\"}]", null));
  }

  // -------------------------------------------------------------------------------------------------------

  private static RaftHAPlugin pluginOn(final RaftHAServer raft, final boolean gateEnabled) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, gateEnabled);
    plugin.configure(serverWith(configuration), configuration);
    plugin.setRaftHAServer(raft);
    return plugin;
  }

  private static ArcadeDBServer serverWithGate(final boolean enabled) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, enabled);
    return serverWith(configuration);
  }

  private static ArcadeDBServer serverWith(final ContextConfiguration configuration) {
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
    return server;
  }

  /** A registry holding the answer a node running a build without the capability route actually gives: a 404. */
  private static PeerCapabilityRegistry registryWhereThePeerAnswered404() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.forget(registry.generation(), LAGGING_PEER, "the capability route answered HTTP 404");
    return registry;
  }

  private static final String FINGERPRINT = "0f".repeat(32);

  // Regression for #7509: this build advertises that it reads a security precondition
  @Test
  void thisBuildAdvertisesThatItReadsASecurityPrecondition() {
    // Without this a cluster of nodes that can all read one would never agree to write one.
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.SECURITY_PRECONDITION);
  }

  // Regression for #7509: the fingerprint is written when no peer is missing the capability
  @Test
  void theFingerprintIsWrittenWhenNoPeerIsMissingTheCapability() {
    assertThat(new RaftHAPlugin().preconditionForPeers(FINGERPRINT, List.of())).isEqualTo(FINGERPRINT);
  }

  // Regression for #7509: the fingerprint is withheld while any peer is missing the capability
  @Test
  void theFingerprintIsWithheldWhileAnyPeerIsMissingTheCapability() {
    assertThat(new RaftHAPlugin().preconditionForPeers(FINGERPRINT, List.of("arcadedb2"))).isNull();
  }

  // Regression for #7509: a submission with no precondition stays without one
  /** A seed carries no fingerprint to begin with, gate or no gate. */
  @Test
  void aSubmissionWithNoPreconditionStaysWithoutOne() {
    assertThat(new RaftHAPlugin().preconditionForPeers(null, List.of())).isNull();
  }

  // Regression for #7509: an unknown peer counts as missing the capability
  /**
   * What "missing" means is the registry's answer, and it is fail-closed: a peer that is unknown - never probed,
   * unreachable, or probed and stale - counts as missing exactly like one that answered without the token. That is
   * what makes the gate safe without an operator sequencing the upgrade by hand.
   */
  @Test
  void anUnknownPeerCountsAsMissingTheCapability() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();
    registry.record(generation, "arcadedb0", Set.of(PeerCapabilities.SECURITY_PRECONDITION), "26.10.1");

    assertThat(registry.peersMissing(List.of("arcadedb0"), PeerCapabilities.SECURITY_PRECONDITION)).isEmpty();
    assertThat(registry.peersMissing(List.of("arcadedb0", "arcadedb1"), PeerCapabilities.SECURITY_PRECONDITION))
        .as("a peer that has never advertised anything must not be credited with the capability")
        .containsExactly("arcadedb1");
  }

  // Regression for #7509: a peer that advertises without the token counts as missing it
  @Test
  void aPeerThatAdvertisesWithoutTheTokenCountsAsMissingIt() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();
    registry.record(generation, "arcadedb1", Set.of(PeerCapabilities.SCHEMA_DELTA), "26.9.1");

    assertThat(registry.peersMissing(List.of("arcadedb1"), PeerCapabilities.SECURITY_PRECONDITION))
        .containsExactly("arcadedb1");
  }
}
