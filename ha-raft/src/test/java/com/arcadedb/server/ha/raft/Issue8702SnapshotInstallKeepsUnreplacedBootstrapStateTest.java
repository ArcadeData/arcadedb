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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8702: a leader-driven snapshot install ({@code notifyInstallSnapshotFromLeader}) must not
 * settle the pending bootstrap replacement (issue #8367) nor clear the bootstrap-divergence mark (issue #6124) of a
 * database it did not reinstall.
 * <p>
 * The reconcile can leave a present database untouched two ways: it gave up on it ({@code notInstalled}, issue #6760)
 * or the leader does not hold it ({@code leaderMissing}, issues #8559/#8588). Either way the copy on disk is still the
 * one the committed bootstrap baseline rejected, so the readiness holder that keeps the node out of the Service and the
 * #8363 request gate must stay, and so must the alert. Before the fix the install settled every pending replacement
 * except the given-up ones, and cleared every divergence mark including the given-up ones - the full-resync sibling
 * ({@code downloadAllDatabasesFrom}) already excluded both sets.
 * <p>
 * The pending replacements and marks are seeded through the persisted {@code bootstrap-baselines} file, the same way a
 * node restarted with a replacement still pending finds them (issue #8411), so each one owns its readiness holder.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8702SnapshotInstallKeepsUnreplacedBootstrapStateTest {
  private static final String LEADER_PEER_ID  = "peer-b_2434";
  private static final long   FIRST_LOG_INDEX = 5_000L;
  private static final String MISSING_DB      = "db-leader-missing";
  private static final String GIVEN_UP_DB     = "db-given-up";
  private static final String REINSTALLED_DB  = "db-reinstalled";

  /**
   * The issue as reported: the reconcile reports a database LEADER_MISSING while its bootstrap replacement is pending.
   * The install must leave the replacement pending with its readiness holder, and the divergence mark standing.
   */
  @Test
  void aLeaderMissingDatabaseKeepsItsPendingReplacementAndDivergenceMark(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(MISSING_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(), Set.of(MISSING_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      assertThat(sm.getPendingBootstrapReplacements())
          .as("precondition: both replacements are pending before the install")
          .containsExactly(MISSING_DB, REINSTALLED_DB);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(MISSING_DB, REINSTALLED_DB);

      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(sm.getPendingBootstrapReplacements())
          .as("the leader does not hold it, so nothing replaced the rejected copy: still pending")
          .containsExactly(MISSING_DB);
      assertThat(sm.getBootstrapInstallsInFlight())
          .as("and its readiness holder still keeps the node out of the Service")
          .containsExactly(MISSING_DB);
      assertThat(sm.getBootstrapUnreconciledDatabases())
          .as("the divergence alert stays for the copy nothing replaced")
          .containsExactly(MISSING_DB);
      assertThat(sm.hasLeaderServiceGap())
          .as("the node must not advertise itself as a healthy leadership hand-off target (issues #8529/#8665)")
          .isTrue();
      assertPersisted(tempDir, MISSING_DB, true, true);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  /**
   * The older instance of the same shape the issue names: a database the install gave up on already kept its pending
   * replacement, but lost its divergence mark.
   */
  @Test
  void aGivenUpDatabaseKeepsItsDivergenceMark(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(GIVEN_UP_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(GIVEN_UP_DB), Set.of()));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(sm.getPendingBootstrapReplacements()).containsExactly(GIVEN_UP_DB);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(GIVEN_UP_DB);
      assertThat(sm.getBootstrapUnreconciledDatabases())
          .as("the install gave up on it, so the copy the overwrite guard kept is still there")
          .containsExactly(GIVEN_UP_DB);
      assertPersisted(tempDir, GIVEN_UP_DB, true, true);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  /** Both outcomes in one install: each database keeps its own state, the reinstalled one is settled. */
  @Test
  void bothUnreplacedOutcomesInOneInstall(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(MISSING_DB, GIVEN_UP_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(GIVEN_UP_DB), Set.of(MISSING_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(sm.getPendingBootstrapReplacements()).containsExactly(GIVEN_UP_DB, MISSING_DB);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(GIVEN_UP_DB, MISSING_DB);
      assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(GIVEN_UP_DB, MISSING_DB);
    } finally {
      sm.close();
    }
  }

  /**
   * The two exclusions pinned independently: one LEADER_MISSING database carries only the divergence mark, another
   * only the pending replacement. Each must keep exactly what it had.
   */
  @Test
  void eachExclusionHoldsOnItsOwn(@TempDir final Path tempDir) throws Exception {
    final String markOnlyDb = "db-mark-only";
    final String pendingOnlyDb = "db-pending-only";
    final JSONObject json = new JSONObject();
    json.put(markOnlyDb, baselineEntry().put("unreconciled", true));
    json.put(pendingOnlyDb, baselineEntry().put("pendingReplacement", true));
    json.put(REINSTALLED_DB, baselineEntry().put("unreconciled", true).put("pendingReplacement", true));
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(markOnlyDb, pendingOnlyDb, REINSTALLED_DB), json);
    replaceReconciler(sm, new StubReconciler(Set.of(), Set.of(markOnlyDb, pendingOnlyDb)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(markOnlyDb);
      assertThat(sm.getPendingBootstrapReplacements()).containsExactly(pendingOnlyDb);
      assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(pendingOnlyDb);
      assertPersisted(tempDir, markOnlyDb, false, true);
      assertPersisted(tempDir, pendingOnlyDb, true, false);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  /** Control: an install that replaced every database settles every replacement and clears every mark, as before. */
  @Test
  void aFullInstallStillSettlesEverything(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(MISSING_DB, REINSTALLED_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(), Set.of()));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
      assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
      assertThat(sm.getBootstrapUnreconciledDatabases()).isEmpty();
      assertThat(sm.hasLeaderServiceGap()).isFalse();
      assertPersisted(tempDir, MISSING_DB, false, false);
      assertPersisted(tempDir, REINSTALLED_DB, false, false);
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  /** A reconciler that touches nothing and reports exactly the outcome the test wants. */
  private static class StubReconciler extends DatabaseReconciler {
    private final Set<String> notInstalled;
    private final Set<String> leaderMissing;

    private StubReconciler(final Set<String> notInstalled, final Set<String> leaderMissing) {
      this.notInstalled = notInstalled;
      this.leaderMissing = leaderMissing;
    }

    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderPeerId, final String leaderHttpAddr,
        final String leaderHttpsAddr, final String clusterToken, final long installedBoundaryIndex) {
      return new ReconcileFromLeaderResult(notInstalled, leaderMissing, null);
    }
  }

  private static void assertPersisted(final Path tempDir, final String dbName, final boolean pendingReplacement,
      final boolean unreconciled) throws IOException {
    final JSONObject entry = new JSONObject(Files.readString(baselinesFile(tempDir)).trim()).getJSONObject(dbName);
    assertThat(entry.getBoolean("pendingReplacement", false)).as("persisted pendingReplacement of '%s'", dbName)
        .isEqualTo(pendingReplacement);
    assertThat(entry.getBoolean("unreconciled", false)).as("persisted unreconciled mark of '%s'", dbName)
        .isEqualTo(unreconciled);
  }

  private static Path baselinesFile(final Path tempDir) {
    return tempDir.resolve("databases").resolve(".raft").resolve("bootstrap-baselines");
  }

  private static JSONObject baselineEntry() {
    return new JSONObject().put("fingerprint", "0".repeat(64)).put("lastTxId", 5L);
  }

  /** Every database starts with a baseline, a divergence mark and a pending replacement, as after a restart. */
  private static JSONObject markedAndPending(final Set<String> databaseNames) {
    final JSONObject json = new JSONObject();
    for (final String dbName : databaseNames)
      json.put(dbName, baselineEntry().put("unreconciled", true).put("pendingReplacement", true));
    return json;
  }

  private static RaftHAServer followerRaft() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(RaftPeerId.valueOf(LEADER_PEER_ID));
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn("peer-b:2480");
    when(raft.getLocalHttpAddress()).thenReturn("localhost:2480");
    return raft;
  }

  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir, final Set<String> databaseNames)
      throws IOException {
    return newStateMachine(tempDir, databaseNames, markedAndPending(databaseNames));
  }

  /** Seeds the persisted bootstrap state first, so the state machine loads it the way a restarted node does. */
  private static ArcadeStateMachine newStateMachine(final Path tempDir, final Set<String> databaseNames,
      final JSONObject bootstrapState) throws IOException {
    Files.createDirectories(baselinesFile(tempDir).getParent());
    Files.writeString(baselinesFile(tempDir), bootstrapState.toString());

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(databaseNames);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
            .build());
    return sm;
  }

  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue8702SnapshotInstallKeepsUnreplacedBootstrapStateTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  private static void replaceReconciler(final ArcadeStateMachine sm, final DatabaseReconciler reconciler)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("reconciler");
    f.setAccessible(true);
    f.set(sm, reconciler);
  }
}
