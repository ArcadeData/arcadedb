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
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
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
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11531: a leader-driven snapshot install that gives up on a database must not register its
 * snapshot marker before the quarantine and the read floor of that database are on disk.
 * <p>
 * The install registered the marker first and only then wrote {@code .raft/applied-index} with the final quarantine
 * ({@code SNAPSHOT_INSTALL_INCOMPLETE}) and read floors. A failed write was logged and ignored, the install reported
 * success, and Ratis purged the log up to the marker. A restart before a later write succeeded brought the given-up
 * database back with no quarantine and no floor, and nothing left in the log to replay into it.
 * <p>
 * What must hold instead: the final state is written before the marker, and an install that cannot make the quarantine
 * durable fails, so Ratis neither records the boundary nor purges the log and retries the install.
 */
class Issue11531SnapshotInstallQuarantineWriteTest {
  private static final String LEADER_PEER_ID    = "peer-b_2434";
  private static final String LOCAL_HTTP        = "localhost:2480";
  private static final long   FIRST_LOG_INDEX   = 5_000L;
  private static final long   SNAPSHOT_INDEX    = FIRST_LOG_INDEX - 1;
  private static final long   PERSISTED_APPLIED = 100L;
  private static final String STALE_DB          = "db-stale";
  private static final String HEALTHY_DB        = "db-ok";

  /**
   * The core regression: the quarantine cannot be written, so the install fails and leaves no marker behind. Before the
   * fix the future completed with the snapshot index and the marker was registered.
   */
  @Test
  void anInstallThatCannotPersistItsQuarantineFailsBeforeTheMarker(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
      blockAppliedIndexFile(tempDir);

      assertThatThrownBy(() -> sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get())
          .as("an install whose quarantine is in memory only must not be reported to Ratis as installed")
          .isInstanceOf(ExecutionException.class)
          .hasStackTraceContaining(STALE_DB);

      final SnapshotInfo latest = sm.getLatestSnapshot();
      assertThat(latest == null ? -1L : latest.getIndex())
          .as("and no marker may be registered: Ratis would purge the log up to it")
          .isNotEqualTo(SNAPSHOT_INDEX);

      assertThat(sm.isDatabaseDiverged(STALE_DB))
          .as("the quarantine still holds in memory: the failure is fail-closed")
          .isTrue();
      assertThat(sm.getDatabaseAppliedFloor(STALE_DB)).isEqualTo(PERSISTED_APPLIED);
    } finally {
      sm.close();
    }
  }

  /**
   * Once the disk recovers, the retried install registers the marker, and by then the quarantine and the floor of the
   * given-up database are already on disk, where a restart reads them back.
   */
  @Test
  void theRetriedInstallRegistersTheMarkerWithTheQuarantineAlreadyOnDisk(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
      final Path appliedIndex = blockAppliedIndexFile(tempDir);

      assertThatThrownBy(() -> sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get()).isInstanceOf(ExecutionException.class);

      FileUtils.deleteRecursively(appliedIndex.toFile());

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
      assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.getLatestSnapshot().getIndex()).isEqualTo(SNAPSHOT_INDEX);

      final JSONObject persisted = new JSONObject(Files.readString(appliedIndex));
      assertThat(persisted.getJSONObject("quarantine", new JSONObject()).getString(STALE_DB, null))
          .as("the given-up database is quarantined on disk")
          .isEqualTo("SNAPSHOT_INSTALL_INCOMPLETE");
      assertThat(persisted.getJSONObject("floors", new JSONObject()).getLong(STALE_DB, -1L))
          .as("with the floor of the position it genuinely reached")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(persisted.getJSONObject("db").getLong(HEALTHY_DB, -1L))
          .as("and the refreshed database is recorded at the snapshot index")
          .isEqualTo(SNAPSHOT_INDEX);
      assertThat(persisted.getJSONObject("db").getLong(STALE_DB, -1L)).isEqualTo(PERSISTED_APPLIED);
    } finally {
      sm.close();
    }
  }

  /**
   * Control: an install that gave up on nothing publishes no quarantine, so a failed write of its positions does not fail
   * it. The marker it registers is ahead of the persisted position, which {@code reinitialize()} detects on the next
   * restart as a stale-snapshot gap and resyncs (issue #6111): failing it here would only loop Ratis on a full disk.
   */
  @Test
  void aCleanInstallStillCompletesWhenOnlyItsPositionsCannotBeWritten(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    replaceReconciler(sm, new StubReconciler(Set.of()));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
      blockAppliedIndexFile(tempDir);

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
      assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.getLatestSnapshot().getIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.isResyncInProgress()).isFalse();
    } finally {
      sm.close();
    }
  }

  /** A non-empty directory where the applied-index file goes: the atomic replace of it fails on every write. */
  private static Path blockAppliedIndexFile(final Path tempDir) throws IOException {
    final Path appliedIndex = tempDir.resolve("databases").resolve(".raft").resolve("applied-index");
    Files.deleteIfExists(appliedIndex);
    Files.writeString(Files.createDirectories(appliedIndex).resolve("blocker"), "x");
    return appliedIndex;
  }

  private static class StubReconciler extends DatabaseReconciler {
    private final Set<String> givenUp;

    StubReconciler(final Set<String> givenUp) {
      this.givenUp = givenUp;
    }

    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderPeerId, final String leaderHttpAddr,
        final String leaderHttpsAddr, final String clusterToken, final long installedBoundaryIndex) {
      return new ReconcileFromLeaderResult(givenUp, null);
    }
  }

  private static FakeRaftHAServer followerRaft() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.leaderId(RaftPeerId.valueOf(LEADER_PEER_ID));
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress(LOCAL_HTTP);
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
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", config);
    server.returns("getDatabaseNames", databaseNames);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        RaftStorage.newBuilder()
            .setDirectory(tempDir.resolve("raft").toFile())
            .setOption(RaftStorage.StartupOption.FORMAT)
            .build());
    return sm;
  }

  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue11531SnapshotInstallQuarantineWriteTest.class.getClassLoader(),
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
