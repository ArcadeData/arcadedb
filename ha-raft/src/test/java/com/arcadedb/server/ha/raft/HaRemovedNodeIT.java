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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.protocol.RaftPeer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node removed from the Raft configuration, against real Ratis. Merged from {@code Issue9510*}, {@code Issue9589*},
 * {@code Issue9590*}, {@code Issue9606*} and {@code Issue7040RemovedPeerStatusIT}, which ran on the same fixture: three
 * servers, {@code majority} quorum, a 10 s quorum timeout (the default, which #9589 and #7040 relied on implicitly).
 * <p>
 * A removed node ({@code leaveCluster()}, i.e. a {@code setConfiguration} without it, or a {@code removePeer} on the
 * leader) keeps an open division and state machine that no longer receive appends, while its Raft client still reaches
 * the cluster's leader, which accepts a request from a non-member:
 * <ul>
 * <li><b>#9510</b>: a commit on such a node was committed cluster-wide and then waited the whole quorum timeout for a
 * local apply that never comes. It is now refused, retryably, before anything is prepared or submitted; so are an
 * install and a drop, while a read-only transaction still commits.</li>
 * <li><b>#9589</b>, the security half of #9510: a user created, a group saved or an API token minted on such a node was
 * committed on every member and never applied on the node that answered. It is now refused and nothing reaches the
 * log. Each test then commits a marker change from a member and waits for it everywhere, so "the refused change is
 * absent" is read after a later entry has been applied, not merely before the refused one could have arrived.</li>
 * <li><b>#9590</b>: a READ_YOUR_WRITES read carrying a bookmark the node never reaches waited the whole quorum timeout
 * and then degraded to EVENTUAL, serving data missing the write; a LINEARIZABLE read waited for a read index the
 * division never applies and failed only at the timeout. Both are now refused at once, retryably. EVENTUAL reads, and a
 * READ_YOUR_WRITES read whose bookmark the node already applied, are served.</li>
 * <li><b>#9606</b>: a READ_YOUR_WRITES read already waiting when its follower is removed used to wait out the quorum
 * timeout before #9590's re-check refused it. It is now refused as soon as the follower learns of its removal: at once
 * when it removed itself, and within one recheck interval when the leader removed it, since a removed node is cut off
 * before it applies the entry that removes it and nothing tells its state machine.</li>
 * <li><b>#7040</b> (follow-up to #5275): {@code /api/v1/cluster} kept reporting a removed peer as a healthy
 * {@code FOLLOWER} with an address, because the peer list was built from the static group only. It must now flag the
 * peer as out of the configuration and raise the membership alert, while the members still in the configuration read
 * as before.</li>
 * </ul>
 * Every test removes a node, so the class stays on the per-test {@link BaseRaftHATest} fixture: the merge is for
 * organization only and saves no cluster boot. {@code Issue5275MembershipSelfHealIT} stays a class of its own because
 * it needs {@link #persistentRaftStorage()}.
 * <p>
 * The tests tagged {@code slow} were tagged so in their source class; #7040's was not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaRemovedNodeIT extends BaseRaftHATest {

  private static final String TYPE_NAME      = "HaRemovedNode";
  private static final int    RECORDS_BEFORE = 5;
  private static final long   QUORUM_TIMEOUT = 10_000L;
  private static final String PASSWORD       = "ThisIsAStrongPassword1!";

  /** The server that left the cluster: excluded from the end-of-test comparison, it diverges by design. */
  private volatile int removedIndex = -1;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, QUORUM_TIMEOUT);
  }

  @Override
  protected int[] getServerToCheck() {
    return serversMatching(i -> i != removedIndex && getServer(i) != null && getServer(i).isStarted());
  }

  @AfterEach
  void clearContext() {
    RaftReplicatedDatabase.removeReadConsistencyContext();
  }

  // ------------------------------------------------------------------ writes, installs and drops (#9510)

  /** Issue #9510, from {@code Issue9510NodeRemovedFromConfigurationRefusesWritesIT}. */
  @Tag("slow")
  @Test
  void aFollowerThatLeftTheClusterRefusesCommitsInsteadOfStalling() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, false);
    assertCommitRefusedAndNotReplicated(followerIndex);
  }

  /** Issue #9510, from {@code Issue9510NodeRemovedFromConfigurationRefusesWritesIT}. */
  @Tag("slow")
  @Test
  void aLeaderThatLeftTheClusterRefusesCommitsInsteadOfStalling() {
    final int leaderIndex = prepareType();
    leaveAndAwaitRemoval(leaderIndex, false);
    assertThat(getRaftPlugin(leaderIndex).getRaftHAServer().isLeader()).as("the leader stepped down on leaving").isFalse();
    assertCommitRefusedAndNotReplicated(leaderIndex);
  }

  /** Issue #9510, from {@code Issue9510NodeRemovedFromConfigurationRefusesWritesIT}. */
  @Tag("slow")
  @Test
  void aNodeThatLeftTheClusterStillCommitsAReadOnlyTransaction() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, false);

    // Every HTTP query runs in a transaction: a removed node submits nothing for it, so it has nothing to refuse.
    final Database removedDb = getServerDatabase(followerIndex, getDatabaseName());
    removedDb.begin();
    try (final ResultSet rs = removedDb.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(RECORDS_BEFORE);
    }
    removedDb.commit();
    assertThat(removedDb.isTransactionActive()).isFalse();
  }

  /** Issue #9510, from {@code Issue9510NodeRemovedFromConfigurationRefusesWritesIT}. */
  @Tag("slow")
  @Test
  void aNodeThatLeftTheClusterRefusesAnInstall() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, false);

    final HAReplicatedDatabase removedDb = (HAReplicatedDatabase) getServer(followerIndex).getDatabaseForOperator(getDatabaseName())
        .getWrappedDatabaseInstance();
    assertThatThrownBy(removedDb::createInReplicas).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member");
    assertThatThrownBy(() -> removedDb.createInReplicas(true)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member");
  }

  /** Issue #9510, from {@code Issue9510NodeRemovedFromConfigurationRefusesWritesIT}. */
  @Tag("slow")
  @Test
  void aNodeThatLeftTheClusterRefusesADropInsteadOfDroppingTheDatabaseClusterWide() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, false);

    final HAReplicatedDatabase removedDb = (HAReplicatedDatabase) getServer(followerIndex).getDatabaseForOperator(getDatabaseName())
        .getWrappedDatabaseInstance();
    assertThatThrownBy(removedDb::dropInReplicas).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member");

    // The members still hold the database: the drop never reached the log.
    for (final int index : getServerToCheck())
      assertThat(getServer(index).existsDatabase(getDatabaseName())).as("database on server %d", index).isTrue();
  }

  // ------------------------------------------------------------------ security mutations (#9589)

  /** Issue #9589, from {@code Issue9589RemovedNodeRefusesSecurityMutationsIT}. */
  @Tag("slow")
  @Test
  void aUserCreatedOnARemovedNodeIsRefusedAndReachesNoMember() {
    final int removed = leaveFromAFollower();

    assertThatThrownBy(() -> security(removed).createUserClusterWide(user("issue9589refused")))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    assertThat(security(removed).existsUser("issue9589refused")).isFalse();

    final int member = getServerToCheck()[0];
    security(member).createUserClusterWide(user("issue9589marker"));
    for (final int index : getServerToCheck()) {
      awaitOn(index, () -> security(index).existsUser("issue9589marker"));
      assertThat(security(index).existsUser("issue9589refused")).as("refused user on server %d", index).isFalse();
    }
  }

  /** Issue #9589, from {@code Issue9589RemovedNodeRefusesSecurityMutationsIT}. */
  @Tag("slow")
  @Test
  void aGroupSavedOnARemovedNodeIsRefusedAndReachesNoMember() {
    final int removed = leaveFromAFollower();

    assertThatThrownBy(() -> security(removed).saveGroupClusterWide("*", "issue9589refused", readerGroup()))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    assertThat(hasGroup(removed, "issue9589refused")).isFalse();

    final int member = getServerToCheck()[0];
    security(member).saveGroupClusterWide("*", "issue9589marker", readerGroup());
    for (final int index : getServerToCheck()) {
      awaitOn(index, () -> hasGroup(index, "issue9589marker"));
      assertThat(hasGroup(index, "issue9589refused")).as("refused group on server %d", index).isFalse();
    }
  }

  /** Issue #9589, from {@code Issue9589RemovedNodeRefusesSecurityMutationsIT}. */
  @Tag("slow")
  @Test
  void anApiTokenMintedOnARemovedNodeIsRefusedAndReachesNoMember() {
    final int removed = leaveFromAFollower();

    assertThatThrownBy(() -> security(removed).createApiTokenClusterWide("issue9589refused", "*", 0, new JSONObject()))
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    assertThat(hasToken(removed, "issue9589refused")).isFalse();

    final int member = getServerToCheck()[0];
    security(member).createApiTokenClusterWide("issue9589marker", "*", 0, new JSONObject());
    for (final int index : getServerToCheck()) {
      awaitOn(index, () -> hasToken(index, "issue9589marker"));
      assertThat(hasToken(index, "issue9589refused")).as("refused token on server %d", index).isFalse();
    }
  }

  // ------------------------------------------------------------------ consistent reads (#9590)

  /** Issue #9590, from {@code Issue9590RemovedNodeRefusesConsistentReadsIT}. */
  @Tag("slow")
  @Test
  void aReadYourWritesReadWithABookmarkTheRemovedNodeNeverReachesIsRefusedAtOnce() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, true);

    // A write the removed node never receives: its bookmark is past anything it will ever apply.
    final long bookmark = writeOnMemberAndGetBookmark();
    assertThat(getRaftPlugin(followerIndex).getRaftHAServer().getLastAppliedIndex())
        .as("the removed node did not apply the new write").isLessThan(bookmark);

    assertConsistentReadRefused(followerIndex, Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
  }

  /** Issue #9590, from {@code Issue9590RemovedNodeRefusesConsistentReadsIT}. */
  @Tag("slow")
  @Test
  void aLinearizableReadOnARemovedNodeIsRefusedAtOnce() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, true);
    writeOnMemberAndGetBookmark();

    assertConsistentReadRefused(followerIndex, Database.READ_CONSISTENCY.LINEARIZABLE, -1L);
  }

  /** Issue #9590, from {@code Issue9590RemovedNodeRefusesConsistentReadsIT}. */
  @Tag("slow")
  @Test
  void aLeaderThatLeftTheClusterRefusesALinearizableRead() {
    final int leaderIndex = prepareType();
    leaveAndAwaitRemoval(leaderIndex, true);
    writeOnMemberAndGetBookmark();

    assertConsistentReadRefused(leaderIndex, Database.READ_CONSISTENCY.LINEARIZABLE, -1L);
  }

  /** Issue #9590, from {@code Issue9590RemovedNodeRefusesConsistentReadsIT}. */
  @Tag("slow")
  @Test
  void aRemovedNodeStillServesEventualReadsAndBookmarksItAlreadyApplied() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex, true);
    writeOnMemberAndGetBookmark();

    final Database removedDb = getServerDatabase(followerIndex, getDatabaseName());
    assertThat(getRaftPlugin(followerIndex).getRaftHAServer().isRemovedFromConfiguration())
        .as("the reads below are served by a node that knows it was removed").isTrue();

    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.EVENTUAL, -1L);
    assertThat(countBefore(removedDb)).isEqualTo(RECORDS_BEFORE);

    // A bookmark this node has already applied needs no wait, so membership does not matter to it.
    final long applied = getRaftPlugin(followerIndex).getRaftHAServer().getTrustedAppliedIndex(getDatabaseName());
    assertThat(applied).isPositive();
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, applied);
    assertThat(countBefore(removedDb)).isEqualTo(RECORDS_BEFORE);
  }

  // ------------------------------------------------------------------ a read waiting when the node is removed (#9606)

  /** Issue #9606, from {@code Issue9606ReadWaitingWhenRemovedIT}. */
  @Tag("slow")
  @Test
  void aWaitingReadIsRefusedPromptlyWhenTheFollowerLeavesTheCluster() throws Exception {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final CompletableFuture<Throwable> read = readWaitingOn(followerIndex, leaderIndex);

    removedIndex = followerIndex;
    getRaftPlugin(followerIndex).getRaftHAServer().leaveCluster(false);
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertWaitingReadRefused(read.get(60, TimeUnit.SECONDS));
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a read refused once its node removed itself, rather than one waiting out the quorum timeout");
  }

  /** Issue #9606, from {@code Issue9606ReadWaitingWhenRemovedIT}. */
  @Tag("slow")
  @Test
  void aWaitingReadIsRefusedPromptlyWhenTheLeaderRemovesTheFollower() throws Exception {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final CompletableFuture<Throwable> read = readWaitingOn(followerIndex, leaderIndex);

    removedIndex = followerIndex;
    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    getRaftPlugin(leaderIndex).getRaftHAServer().removePeer(follower.getLocalPeerId().toString());
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertWaitingReadRefused(read.get(60, TimeUnit.SECONDS));
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a read refused within a membership recheck of its node's removal, rather than one waiting out the quorum timeout");
  }

  // ------------------------------------------------------------------ the cluster status of a removed peer (#7040)

  /** Issue #7040, from {@code Issue7040RemovedPeerStatusIT}. */
  @Test
  void removedPeerIsReportedOutOfConfigurationWithAnAlert() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int removedPeerIndex = (leaderIndex + 1) % getServerCount();
    final String removedPeerId = peerIdForIndex(removedPeerIndex);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(3);

    // Before the removal every declared peer is a member: no flag, no alert.
    final JSONObject before = queryClusterEndpoint(leaderIndex);
    assertThat(before.getJSONArray("peers").length()).isEqualTo(3);
    for (int i = 0; i < 3; i++) {
      final JSONObject peer = before.getJSONArray("peers").getJSONObject(i);
      assertThat(peer.getBoolean("inConfiguration")).isTrue();
      assertThat(peer.getString("role")).isIn("LEADER", "FOLLOWER");
    }
    assertThat(alertIds(before)).doesNotContain("peers-not-in-configuration");

    // The reported scenario: the peer is dropped from the committed configuration while it is down.
    getServer(removedPeerIndex).stop();
    leaderRaft.removePeer(removedPeerId, true);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderRaft.getLivePeers()).hasSize(2));

    final JSONObject after = queryClusterEndpoint(leaderIndex);
    final JSONArray peers = after.getJSONArray("peers");
    assertThat(peers.length()).as("the removed peer stays listed: it is still declared").isEqualTo(3);

    boolean removedSeen = false;
    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      if (removedPeerId.equals(peer.getString("id"))) {
        removedSeen = true;
        assertThat(peer.getBoolean("inConfiguration")).isFalse();
        assertThat(peer.getString("role")).isEqualTo(GetClusterHandler.ROLE_NOT_IN_CONFIGURATION);
      } else {
        assertThat(peer.getBoolean("inConfiguration")).isTrue();
        assertThat(peer.getString("role")).isIn("LEADER", "FOLLOWER");
      }
    }
    assertThat(removedSeen).isTrue();

    final JSONArray alerts = after.getJSONArray("alerts");
    JSONObject membershipAlert = null;
    for (int i = 0; i < alerts.length(); i++)
      if ("peers-not-in-configuration".equals(alerts.getJSONObject(i).getString("id")))
        membershipAlert = alerts.getJSONObject(i);
    assertThat(membershipAlert).as("the divergence is told to the operator, not left to diff by eye").isNotNull();
    assertThat(membershipAlert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    assertThat(membershipAlert.getJSONObject("details").getJSONArray("peers").toList()).containsExactly(removedPeerId);

    // The sibling views agree (module CLAUDE.md, "Peer-list filtering"): the removed peer is no replica in the server
    // stats, nor in the replica address list a client or a peer-to-peer transfer would dial.
    final List<?> replicas = (List<?>) leaderRaft.getStats().get("replicas");
    for (final Object replica : replicas)
      assertThat(((Map<?, ?>) replica).get("id")).isNotEqualTo(removedPeerId);
    assertThat(replicas).hasSize(1);
    assertThat(leaderRaft.getReplicaAddresses()).doesNotContain("localhost:" + (getServerHttpPort(removedPeerIndex)));

    // A follower that is still a member sees the same picture: the live configuration is committed cluster-wide.
    final int otherFollower = (leaderIndex + 2) % getServerCount();
    final JSONObject fromFollower = queryClusterEndpoint(otherFollower);
    assertThat(alertIds(fromFollower)).contains("peers-not-in-configuration");
  }

  // ------------------------------------------------------------------ helpers

  /** Creates the type and commits {@link #RECORDS_BEFORE} records on the leader; returns the leader's index. */
  private int prepareType() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(TYPE_NAME));
    for (int i = 0; i < RECORDS_BEFORE; i++) {
      final int id = i;
      leaderDb.transaction(() -> leaderDb.newDocument(TYPE_NAME).set("id", id).save());
    }
    waitForAllServers();
    return leaderIndex;
  }

  /**
   * Removes {@code index} from the configuration and waits until its own division has seen the new configuration and
   * it is not the leader. {@code alsoAwaitRemovedFlag} adds the wait #9590 had and #9510 did not: that the node also
   * reports {@code isRemovedFromConfiguration()}.
   */
  private void leaveAndAwaitRemoval(final int index, final boolean alsoAwaitRemovedFlag) {
    removedIndex = index;
    final RaftHAServer raft = getRaftPlugin(index).getRaftHAServer();
    final String peerId = raft.getLocalPeerId().toString();
    raft.leaveCluster(false);
    Awaitility.await("server " + index + " sees its own removal from the Raft configuration and is not the leader")
        .atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> !containsPeer(raft.getCommittedPeersOrNull(), peerId) && !raft.isLeader()
            && (!alsoAwaitRemovedFlag || raft.isRemovedFromConfiguration()));
  }

  /** Removes a follower from the configuration and waits until its own division has seen the removal (#9589). */
  private int leaveFromAFollower() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int index = (leaderIndex + 1) % getServerCount();
    removedIndex = index;
    final RaftHAServer raft = getRaftPlugin(index).getRaftHAServer();
    final String peerId = raft.getLocalPeerId().toString();
    raft.leaveCluster(false);
    Awaitility.await("server " + index + " sees its own removal from the Raft configuration")
        .atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> !containsPeer(raft.getCommittedPeersOrNull(), peerId) && raft.isRemovedFromConfiguration());
    return index;
  }

  private static boolean containsPeer(final Collection<RaftPeer> peers, final String peerId) {
    if (peers == null)
      return true;
    for (final RaftPeer peer : peers)
      if (peer.getId().toString().equals(peerId))
        return true;
    return false;
  }

  private void assertCommitRefusedAndNotReplicated(final int removedNode) {
    final Database removedDb = getServerDatabase(removedNode, getDatabaseName());

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    removedDb.begin();
    removedDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE).save();
    assertThatThrownBy(removedDb::commit).isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a refusal before replication, rather than a wait for an apply this removed node will never run");
    assertThat(removedDb.isTransactionActive()).as("the refused transaction is rolled back").isFalse();

    // The refused write reached no member of the cluster.
    final int member = getServerToCheck()[0];
    final Database memberDb = getServerDatabase(member, getDatabaseName());
    memberDb.transaction(() -> memberDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE + 1).save());
    for (final int index : getServerToCheck()) {
      waitForReplicationIsCompleted(index);
      try (final ResultSet rs = getServerDatabase(index, getDatabaseName()).query("sql",
          "SELECT count(*) AS cnt FROM " + TYPE_NAME + " WHERE id = " + RECORDS_BEFORE)) {
        assertThat(rs.next().<Number>getProperty("cnt").longValue()).as("refused record on server %d", index).isZero();
      }
    }
  }

  private ServerSecurity security(final int index) {
    return getServer(index).getSecurity();
  }

  private boolean hasGroup(final int index, final String group) {
    final JSONObject databases = new JSONObject(security(index).getGroupsJsonPayload()).getJSONObject("databases",
        new JSONObject());
    final JSONObject any = databases.getJSONObject("*", new JSONObject());
    return any.getJSONObject("groups", new JSONObject()).has(group);
  }

  private boolean hasToken(final int index, final String name) {
    final JSONArray tokens = new JSONObject(security(index).getApiTokensJsonPayload()).getJSONArray("tokens",
        new JSONArray());
    for (int i = 0; i < tokens.length(); i++)
      if (name.equals(tokens.getJSONObject(i).getString("name", null)))
        return true;
    return false;
  }

  private static void awaitOn(final int index, final Callable<Boolean> condition) {
    Awaitility.await("the marker change is applied on server " + index).atMost(30, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS).until(condition);
  }

  private static JSONObject user(final String name) {
    return new JSONObject().put("name", name).put("password", PASSWORD).put("databases", new JSONObject());
  }

  private static JSONObject readerGroup() {
    return new JSONObject()
        .put("access", new JSONArray().put("readRecord"))
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));
  }

  private void assertConsistentReadRefused(final int removedNode, final Database.READ_CONSISTENCY consistency,
      final long bookmark) {
    final Database removedDb = getServerDatabase(removedNode, getDatabaseName());
    RaftReplicatedDatabase.applyReadConsistencyContext(consistency, bookmark);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> countBefore(removedDb)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member").hasMessageContaining(consistency.name());
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a refusal on a removed node, rather than a wait for an index its division will never apply");
  }

  private static long countBefore(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME + " WHERE id < " + RECORDS_BEFORE)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }

  /** Commits one record on a member of the cluster and returns the leader's commit index, a bookmark past it. */
  private long writeOnMemberAndGetBookmark() {
    final int newLeader = awaitLeaderAmong(getServerToCheck());
    final Database memberDb = getServerDatabase(newLeader, getDatabaseName());
    memberDb.transaction(() -> memberDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE).save());
    return getRaftPlugin(newLeader).getRaftHAServer().getCommitIndex();
  }

  private int awaitLeaderAmong(final int[] candidates) {
    final int[] found = { -1 };
    Awaitility.await("a leader among the remaining members").atMost(60, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
          for (final int i : candidates)
            if (getRaftPlugin(i).getRaftHAServer().isLeader()) {
              found[0] = i;
              return true;
            }
          return false;
        });
    return found[0];
  }

  /**
   * Starts a READ_YOUR_WRITES read on the follower with a bookmark far past the leader's commit index, so it waits, and
   * returns once it is waiting: still running while the follower is a member and the bookmark is ahead.
   */
  private CompletableFuture<Throwable> readWaitingOn(final int followerIndex, final int leaderIndex) {
    final long bookmark = getRaftPlugin(leaderIndex).getRaftHAServer().getCommitIndex() + 1_000_000L;
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final AtomicReference<Thread> reader = new AtomicReference<>();
    final CompletableFuture<Throwable> read = CompletableFuture.supplyAsync(() -> {
      reader.set(Thread.currentThread());
      RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
      try (final ResultSet rs = followerDb.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
        rs.next();
        return null;
      } catch (final Throwable t) {
        return t;
      } finally {
        RaftReplicatedDatabase.removeReadConsistencyContext();
      }
    });
    // Parked in the apply wait, not merely unfinished: a read that had not reached it yet would be refused by the entry check
    // and pass without exercising a removal DURING the wait
    Awaitility.await("the read parks in the apply wait").atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> isParkedInApplyWait(reader.get()));
    assertThat(read).as("the read is waiting for its bookmark").isNotDone();
    return read;
  }

  private static boolean isParkedInApplyWait(final Thread thread) {
    if (thread == null || thread.getState() != Thread.State.TIMED_WAITING)
      return false;
    for (final StackTraceElement frame : thread.getStackTrace())
      if (frame.getMethodName().equals("awaitAppliedIndex") && frame.getClassName().equals(RaftHAServer.class.getName()))
        return true;
    return false;
  }

  private static void assertWaitingReadRefused(final Throwable failure) {
    assertThat(failure).isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member")
        .hasMessageContaining(Database.READ_CONSISTENCY.READ_YOUR_WRITES.name());
  }

  private static List<String> alertIds(final JSONObject response) {
    final List<String> ids = new ArrayList<>();
    final JSONArray alerts = response.getJSONArray("alerts");
    for (int i = 0; i < alerts.length(); i++)
      ids.add(alerts.getJSONObject(i).getString("id"));
    return ids;
  }

  private JSONObject queryClusterEndpoint(final int serverIndex) throws Exception {
    final int httpPort = getServerHttpPort(serverIndex);
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/cluster");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }
}
