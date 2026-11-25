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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for when a node refuses, supersedes or answers a security entry (follower precondition, removed node, superseded reply).
 *
 * <p>Regression coverage for #7559:
 * Issue #7559: the compare-and-set of issue #7509 engaged only when the mutation happened to be submitted on the
 * leader, because the answer it was gated on came from a registry only a leader fills.
 * <p>
 * {@code RaftHAServer.startCapabilityMonitor} used to run on gaining leadership and stop on losing it, and what it
 * recorded was deliberately left to age out (#7301). That is the right shape for #7219's consumer - only the leader
 * writes a schema delta - but a security entry is not a schema delta: <b>any node can submit one</b>. On a follower
 * the registry was empty, and on a recently demoted leader it was stale, so
 * {@code peersMissingCapability(SECURITY_PRECONDITION)} named peers that are in fact perfectly capable, the
 * precondition was withheld, and the mutation replicated unconditionally - the pre-#7509 behaviour, announced by
 * nothing louder than a throttled INFO line.
 * <p>
 * The inversion is what made it worth a fix rather than a caveat: the entry points the gate switched off - the
 * REST group and API-token routes, and openCypher {@code CREATE USER} / {@code ALTER USER} / {@code DROP USER}
 * over Bolt or {@code /api/v1/command} - are exactly the ones that do NOT forward to the leader, so they are the
 * ones not already serialised onto a single node, and therefore the ones that needed a compare-and-set most.
 * ({@code DeleteDropUserHandler}, which issue #7559's own table names as a fourth such door, turned out to be
 * constructed nowhere, and was removed by issue #7829.)
 *
 * <h2>What the fix is</h2>
 *
 * The decision now reads {@link RaftHAServer#peersMissingCapabilityNow} - the ask-now variant #7511 already added
 * for exactly this reason - instead of the cached, leader-only {@link RaftHAServer#peersMissingCapability}. The
 * cached answer is still consulted first inside it, so a warm leader pays nothing; a follower pays one bounded
 * probe round on an operation that is rare by construction. (Issue #7549 has since moved the monitor onto every
 * node, so the follower's cached answer is normally warm too and the round below is only the cold-start case.)
 *
 * <h2>What these tests pin</h2>
 *
 * Both directions, because only one of them is about engaging the check:
 * <ul>
 *   <li>a node whose cached answer says "peers missing" while the cluster is in fact uniform must still write the
 *       precondition - the follower case, the bug;</li>
 *   <li>a node whose cached answer says "nothing missing" while a peer genuinely cannot read a precondition must
 *       still withhold it - the stale-registry case, and the half #7540 contributed: every node has to reach the
 *       SAME verdict on the same entry, or a mixed cluster diverges its security state instead of merely losing an
 *       update.</li>
 * </ul>
 *
 * <p>Regression coverage for #9589:
 * Issue #9589: the security entries take the route #9510 closed for transactions, drops and installs. A node removed
 * from the Raft configuration still reaches the leader through its Raft client, the leader accepts the entry from a
 * non-member and commits it on every member, and this node's own division never receives it. A user created there is
 * therefore created everywhere except on the node that answered the request.
 * <p>
 * {@code RaftHAPlugin.replicateSecurityUsers/Groups/ApiTokens} is the single chokepoint every security mutation and
 * every seed ends at, so the refusal lives there and is pinned through it, for each document and for both the
 * fingerprinted (user-initiated) and the bare (seed) form. The live-cluster half is
 * {@code HaRemovedNodeIT}.
 *
 * <p>Regression coverage for #7509:
 * Issue #7509, the half a {@code ServerSecurity} test cannot reach: what the state machine ANSWERS for a
 * security entry whose compare-and-set precondition no longer holds.
 * <p>
 * Refusing the entry is only half a fix. The node that submitted it has already answered its operator, and
 * unless the refusal travels back that operator is told a change landed that was thrown away - the silent
 * success the issue is about, just relocated. The verdict has to ride the REPLY rather than be observed
 * locally, because the submitter is not necessarily the leader and a follower's own apply of the entry can lag
 * the reply it gets back.
 * <p>
 * So what is pinned here is the contract between the apply and {@code RaftTransactionBroker.wasApplied}: a
 * refused entry completes NORMALLY - the applied index must advance, it is a no-op on every node, not a
 * failure - carrying {@link ArcadeStateMachine#SECURITY_ENTRY_SUPERSEDED_REPLY}, and an installed one carries
 * something else.
 */
class SecurityPreconditionRefusalTest {

  private static final String FINGERPRINT = "7f".repeat(32);

  private static final String LAGGING_PEER = "arcadedb2";

  private static final String USERS = "[{\"name\":\"root\"}]";

  private static final String GROUPS = "{\"databases\":{}}";

  private static final String API_TOKENS = "{\"version\":1,\"tokens\":[]}";

  // ---------------------------------------------------------------------------------------------------------
  // 1. The follower case: an empty cache must not be mistaken for an incapable cluster
  // ---------------------------------------------------------------------------------------------------------

  // Regression for #7559: a follower still writes the precondition on a users entry
  @Test
  void aFollowerStillWritesThePreconditionOnAUsersEntry() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(followerWhoseCacheIsEmptyButWhoseClusterIsUniform(broker));

    plugin.replicateSecurityUsers(USERS, FINGERPRINT);

    assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, FINGERPRINT));
  }

  // Regression for #7559: a follower still writes the precondition on a groups entry
  @Test
  void aFollowerStillWritesThePreconditionOnAGroupsEntry() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(followerWhoseCacheIsEmptyButWhoseClusterIsUniform(broker));

    plugin.replicateSecurityGroups(GROUPS, FINGERPRINT);

    assertThat(broker.calls("replicateSecurityGroups")).containsOnlyOnce(Arrays.asList(GROUPS, FINGERPRINT));
  }

  // Regression for #7559: a follower still writes the precondition on an api tokens entry
  @Test
  void aFollowerStillWritesThePreconditionOnAnApiTokensEntry() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(followerWhoseCacheIsEmptyButWhoseClusterIsUniform(broker));

    plugin.replicateSecurityApiTokens(API_TOKENS, FINGERPRINT);

    assertThat(broker.calls("replicateSecurityApiTokens")).containsOnlyOnce(Arrays.asList(API_TOKENS, FINGERPRINT));
  }

  // ---------------------------------------------------------------------------------------------------------
  // 2. The stale case: a cache that says "all clear" must not outrank a peer that cannot read a precondition
  // ---------------------------------------------------------------------------------------------------------

  // Regression for #7559: a stale all clear cache does not outrank a peer that cannot read a precondition
  /**
   * The other direction, and the one #7540 contributed. A node demoted from leadership keeps the advertisements it
   * collected until they age out, so its cache can answer "nobody is missing" about a cluster that has since taken
   * on a node predating the section. Writing the precondition on that answer is worse than not writing it: the
   * upgraded nodes refuse the superseded entry and the older one installs it, so the cluster's security state
   * DIVERGES rather than losing an update.
   */
  @Test
  void aStaleAllClearCacheDoesNotOutrankAPeerThatCannotReadAPrecondition() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    // What a recently demoted leader still believes...
    raft.on("peersMissingCapability", a -> Objects.equals(a[0], PeerCapabilities.SECURITY_PRECONDITION) ? List.of() : List.of());
    // ...and what the cluster actually answers when asked now.
    raft.returns("peersMissingCapabilityNow", List.of(LAGGING_PEER));

    pluginOn(raft).replicateSecurityUsers(USERS, FINGERPRINT);

    assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, null));
  }

  // Regression for #7559: the decision is never taken on the leader only cached answer
  /**
   * A gate helper nothing calls is a gate that is not there, and a gate called through the WRONG accessor is the
   * same thing one indirection further down. The cached, leader-only answer must not be what the decision is made
   * on - on any node, in any direction.
   */
  @Test
  void theDecisionIsNeverTakenOnTheLeaderOnlyCachedAnswer() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(new FakeRaftTransactionBroker());
    raft.returns("peersMissingCapabilityNow", List.of());

    pluginOn(raft).replicateSecurityUsers(USERS, FINGERPRINT);

    assertThat(raft.calls("peersMissingCapability")).doesNotContain(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
    assertThat(raft.calls("peersMissingCapabilityNow")).containsOnlyOnce(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
  }

  // ---------------------------------------------------------------------------------------------------------
  // 3. What the fix must NOT cost
  // ---------------------------------------------------------------------------------------------------------

  // Regression for #7559: a seed submission with no fingerprint never asks the cluster about a precondition
  /**
   * {@code peersMissingCapabilityNow} can fan out to every peer, bounded but not free, and it runs with the
   * {@code ServerSecurity} monitor held. A submission that carries no fingerprint has no precondition to decide
   * about, so it must not pay for one: that is every seed path - {@code PostAddPeerHandler},
   * {@code ServerControlPlane.connectCluster}, {@code ServerSecurity}'s bootstrap republish - which run at
   * cluster formation, the moment peers are least likely to answer a probe.
   */
  @Test
  void aSeedSubmissionWithNoFingerprintNeverAsksTheClusterAboutAPrecondition() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(new FakeRaftTransactionBroker());
    raft.returns("peersMissingCapabilityNow", List.of());

    pluginOn(raft).replicateSecurityUsers(USERS);

    assertThat(raft.calls("peersMissingCapabilityNow")).doesNotContain(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
    assertThat(raft.calls("peersMissingCapability")).doesNotContain(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
  }

  // Regression for #7559: a seed groups submission asks only about the entry type and not about a precondition
  /**
   * The same for the two gated documents, whose #7511 gate does query the cluster: it must keep asking about the
   * ENTRY TYPE and still not ask about the precondition, so a seed republish of the group document costs one round
   * rather than two.
   */
  @Test
  void aSeedGroupsSubmissionAsksOnlyAboutTheEntryTypeAndNotAboutAPrecondition() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(new FakeRaftTransactionBroker());
    raft.returns("peersMissingCapabilityNow", List.of());

    pluginOn(raft).replicateSecurityGroups(GROUPS);

    assertThat(raft.calls("peersMissingCapabilityNow")).containsOnlyOnce(Arrays.asList(PeerCapabilities.SECURITY_GROUPS_ENTRY));
    assertThat(raft.calls("peersMissingCapabilityNow")).doesNotContain(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
  }

  // Regression for #7559: a gated document with a fingerprint asks about both tokens
  /**
   * And the two rounds a gated document WITH a fingerprint does make are not two fan-outs in practice: the entry
   * gate's round has just refreshed the registry, so the precondition's own call finds a warm cache and returns
   * without dialling anything. Pinned here as the call shape, since the warm-cache short circuit itself lives in
   * {@code peersMissingCapabilityNow} and is pinned by #7511's tests.
   */
  @Test
  void aGatedDocumentWithAFingerprintAsksAboutBothTokens() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapability", List.of(LAGGING_PEER));
    raft.returns("peersMissingCapabilityNow", List.of());

    pluginOn(raft).replicateSecurityApiTokens(API_TOKENS, FINGERPRINT);

    assertThat(raft.calls("peersMissingCapabilityNow")).containsOnlyOnce(Arrays.asList(PeerCapabilities.SECURITY_API_TOKENS_ENTRY));
    assertThat(raft.calls("peersMissingCapabilityNow")).containsOnlyOnce(Arrays.asList(PeerCapabilities.SECURITY_PRECONDITION));
    assertThat(broker.calls("replicateSecurityApiTokens")).containsOnlyOnce(Arrays.asList(API_TOKENS, FINGERPRINT));
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * A node whose capability cache holds nothing - a follower, which is every node that has not been elected - in a
   * cluster where every peer does in fact answer with the capability when asked.
   */
  private static FakeRaftHAServer followerWhoseCacheIsEmptyButWhoseClusterIsUniform(
      final RaftTransactionBroker broker) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapability", List.of(LAGGING_PEER, "arcadedb1"));
    raft.returns("peersMissingCapabilityNow", List.of());
    return raft;
  }

  private static RaftHAPlugin pluginOn(final RaftHAServer raft) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, true);
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
    plugin.configure(server, configuration);
    plugin.setRaftHAServer(raft);
    return plugin;
  }

  // Regression for #9589: a users entry is refused retryably and nothing is submitted
  @Test
  void aUsersEntryIsRefusedRetryablyAndNothingIsSubmitted() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityUsers(USERS, FINGERPRINT))
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member of the Raft cluster's configuration");
    assertThat(broker.calls("replicateSecurityUsers")).isEmpty();
  }

  // Regression for #9589: a groups entry is refused retryably and nothing is submitted
  @Test
  void aGroupsEntryIsRefusedRetryablyAndNothingIsSubmitted() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityGroups(GROUPS, FINGERPRINT)).isInstanceOf(NeedRetryException.class);
    assertThat(broker.calls("replicateSecurityGroups")).isEmpty();
  }

  // Regression for #9589: an api tokens entry is refused retryably and nothing is submitted
  @Test
  void anApiTokensEntryIsRefusedRetryablyAndNothingIsSubmitted() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityApiTokens(API_TOKENS, FINGERPRINT))
        .isInstanceOf(NeedRetryException.class);
    assertThat(broker.calls("replicateSecurityApiTokens")).isEmpty();
  }

  // Regression for #9589: the seed form is refused too for every document
  /**
   * The seed form (no fingerprint) is refused too, deliberately. Since issues #7531 and #7834 a seed runs only on the
   * leader's {@code MembershipSecuritySeeder}, which the joining node never is, so the refusal cannot reach a join; on a
   * removed node it would commit documents everywhere but here, exactly like a user-initiated change.
   * {@code ServerSecurity.seedSecurityStateClusterWide} reports a refused document as failed and retries it.
   */
  @Test
  void theSeedFormIsRefusedTooForEveryDocument() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftHAPlugin plugin = pluginOn(removedNode(broker));

    assertThatThrownBy(() -> plugin.replicateSecurityUsers(USERS)).isInstanceOf(NeedRetryException.class);
    assertThatThrownBy(() -> plugin.replicateSecurityGroups(GROUPS)).isInstanceOf(NeedRetryException.class);
    assertThatThrownBy(() -> plugin.replicateSecurityApiTokens(API_TOKENS)).isInstanceOf(NeedRetryException.class);
    assertThat(broker.calls("replicateSecurityUsers")).isEmpty();
    assertThat(broker.calls("replicateSecurityGroups")).isEmpty();
    assertThat(broker.calls("replicateSecurityApiTokens")).isEmpty();
  }

  // Regression for #9589: the refusal comes before the capability probe
  /**
   * Refused before the #7511 capability gate, which may dial every peer: a node outside the configuration must not
   * pay for a probe round on a submission it is going to refuse anyway.
   */
  @Test
  void theRefusalComesBeforeTheCapabilityProbe() {
    final FakeRaftHAServer raft = removedNode(new FakeRaftTransactionBroker());
    final RaftHAPlugin plugin = pluginOn(raft);

    assertThatThrownBy(() -> plugin.replicateSecurityGroups(GROUPS, FINGERPRINT)).isInstanceOf(NeedRetryException.class);
    assertThat(raft.calls("peersMissingCapabilityNow")).isEmpty();
  }

  // Regression for #9589: a member still submits every document
  /** The other direction: a member submits exactly as before. */
  @Test
  void aMemberStillSubmitsEveryDocument() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().removedFromConfiguration(false);
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapabilityNow", List.of());
    final RaftHAPlugin plugin = pluginOn(raft);

    plugin.replicateSecurityUsers(USERS);
    plugin.replicateSecurityGroups(GROUPS);
    plugin.replicateSecurityApiTokens(API_TOKENS);

    assertThat(broker.calls("replicateSecurityUsers")).containsOnlyOnce(Arrays.asList(USERS, null));
    assertThat(broker.calls("replicateSecurityGroups")).containsOnlyOnce(Arrays.asList(GROUPS, null));
    assertThat(broker.calls("replicateSecurityApiTokens")).containsOnlyOnce(Arrays.asList(API_TOKENS, null));
  }

  private static FakeRaftHAServer removedNode(final RaftTransactionBroker broker) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().removedFromConfiguration(true);
    raft.transactionBroker(broker);
    raft.returns("peersMissingCapabilityNow", List.of());
    return raft;
  }

  private static final String USERS_JSON  = "[{\"name\":\"root\",\"password\":\"x\"}]";

  private static final String FINGERPRINT_SUPERSEDED = "0f".repeat(32);

  // Regression for #7509: a refused users entry answers the superseded marker and does not fail
  @Test
  void aRefusedUsersEntryAnswersTheSupersededMarkerAndDoesNotFail() {
    final ServerSecurity security = securityThatRefuses();
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(
        entry(sm, 5L, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, FINGERPRINT_SUPERSEDED)));

    assertThat(future.isCompletedExceptionally())
        .as("a refusal is a no-op apply, not a failed one: the applied index must still advance").isFalse();
    assertThat(future.join().getContent().toStringUtf8())
        .isEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  // Regression for #7509: an installed users entry answers something else
  @Test
  void anInstalledUsersEntryAnswersSomethingElse() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.returns("applyReplicatedUsers", true);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(
        entry(sm, 5L, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, FINGERPRINT_SUPERSEDED)));

    assertThat(future.join().getContent().toStringUtf8())
        .isNotEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  // Regression for #7509: a refused groups entry answers the superseded marker
  @Test
  void aRefusedGroupsEntryAnswersTheSupersededMarker() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.returns("applyReplicatedGroups", false);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(entry(sm, 5L,
        RaftLogEntryCodec.encodeSecurityGroupsEntry("{\"databases\":{},\"version\":2}", FINGERPRINT_SUPERSEDED)));

    assertThat(future.join().getContent().toStringUtf8())
        .isEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  // Regression for #7509: a refused api tokens entry answers the superseded marker
  @Test
  void aRefusedApiTokensEntryAnswersTheSupersededMarker() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.returns("applyReplicatedApiTokens", false);
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(entry(sm, 5L,
        RaftLogEntryCodec.encodeSecurityApiTokensEntry("{\"version\":1,\"tokens\":[]}", FINGERPRINT_SUPERSEDED)));

    assertThat(future.join().getContent().toStringUtf8())
        .isEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  // Regression for #7509: an entry without a precondition takes the unconditional apply
  /**
   * An entry with no precondition - a seed, and every entry a node that predates issue #7509 wrote - must not
   * even reach the compare-and-set overload: it goes to the unconditional apply, unchanged.
   */
  @Test
  void anEntryWithoutAPreconditionTakesTheUnconditionalApply() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    final ArcadeStateMachine sm = stateMachine(security);

    final CompletableFuture<Message> future = sm.applyTransaction(
        entry(sm, 5L, RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON)));

    assertThat(security.calls("applyReplicatedUsers")).containsOnlyOnce(Arrays.asList(USERS_JSON));
    assertThat(future.join().getContent().toStringUtf8())
        .isNotEqualTo(ArcadeStateMachine.SECURITY_ENTRY_SUPERSEDED_REPLY);
  }

  private static ServerSecurity securityThatRefuses() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.returns("applyReplicatedUsers", false);
    return security;
  }

  private static ArcadeStateMachine stateMachine(final ServerSecurity security) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.security(security);
    server.returns("getConfiguration", new ContextConfiguration());

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    return sm;
  }

  private static TransactionContext entry(final ArcadeStateMachine sm, final long index, final ByteString payload) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
