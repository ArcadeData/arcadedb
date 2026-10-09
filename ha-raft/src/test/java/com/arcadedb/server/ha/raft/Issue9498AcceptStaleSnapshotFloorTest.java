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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9498: the node-wide stale-snapshot read floor of issue #6111 - published when the Ratis
 * snapshot marker runs ahead of the entries this node applied - is lifted only by a full resync from a peer, and a leader
 * refuses to resync from itself. A sole voter is always the leader, so the floor kept it not-ready and its LINEARIZABLE
 * reads clamped for good, with no operator action short of rebuilding the node. {@code POST
 * /api/v1/cluster/accept-stale-snapshot} (and its gRPC twin, both through {@link RaftHAPlugin#acceptStaleSnapshot(String)})
 * is the audited override.
 * <p>
 * Driven through the shared core with a real {@link ArcadeStateMachine}, a real Ratis storage holding a real snapshot
 * marker and a real {@code .raft/applied-index} file, so the gap is raised by {@link ArcadeStateMachine#reinitialize()}
 * exactly as a restart raises it; the voter count is the one input chosen by the test.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9498AcceptStaleSnapshotFloorTest {

  /** Comfortably beyond {@link GlobalConfiguration#HA_SNAPSHOT_GAP_TOLERANCE} (10). */
  private static final long   PERSISTED_APPLIED = 100L;
  private static final long   SNAPSHOT_INDEX    = 5_000L;
  private static final long   SNAPSHOT_TERM     = 7L;
  private static final String HEALTHY_DB        = "db9498";
  private static final String QUARANTINED_DB    = "quarantined9498";
  private static final String SERVER            = "ArcadeDB_9498";

  @TempDir
  Path root;

  private RaftStorage         raftStorage;
  private ArcadeStateMachine  sm;
  private FakeRaftHAServer    raft;

  @BeforeEach
  void raiseTheFloorTheWayARestartDoes() throws Exception {
    raftStorage = RaftStorage.newBuilder()
        .setDirectory(root.resolve("raft-storage").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    sm = newStateMachine();
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
    // Only entries up to PERSISTED_APPLIED were ever applied here, but the marker on disk claims SNAPSHOT_INDEX
    sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
    registerMarkerAt(sm, SNAPSHOT_TERM, SNAPSHOT_INDEX);
    raft = FakeRaftHAServer.detached();
    sm.setRaftHAServer(raft);
    sm.reinitialize();

    assertThat(sm.getStaleSnapshotAppliedFloor()).as("the gap raises the node-wide floor").isEqualTo(PERSISTED_APPLIED);
    assertThat(sm.isResyncInProgress()).as("which keeps the node out of the ready set").isTrue();
  }

  @AfterEach
  void closeStateMachine() throws IOException {
    sm.close();
    raftStorage.close();
  }

  /**
   * The issue as reported: on a sole voter nothing lifts the floor. Accepting it does, the node is ready again, the
   * waiters it held back are woken, and the gap is not raised again by the next {@code reinitialize()} - the restart
   * path - because the marker index is now the persisted applied position.
   */
  @Test
  void aSoleVoterLiftsTheFloorAndTheNextReinitializeDoesNotRaiseItAgain() throws Exception {
    final List<String> warnings = new CopyOnWriteArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(capturingWarningsOf(ArcadeStateMachine.class, warnings));
    final JSONObject result;
    try {
      result = RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root' (from 10.0.0.9)");
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(result.getString("localServer")).isEqualTo(SERVER);
    assertThat(result.getLong("readFloor")).isEqualTo(PERSISTED_APPLIED);
    assertThat(result.getLong("snapshotIndex")).isEqualTo(SNAPSHOT_INDEX);
    assertThat(result.getLong("appliedIndex")).isEqualTo(SNAPSHOT_INDEX);
    assertThat(result.getString("result")).contains("not replayed");

    assertThat(sm.getStaleSnapshotAppliedFloor()).as("the floor is lifted").isNegative();
    assertThat(sm.isSnapshotDownloadPending()).as("the download the gap queued has nothing left to fill").isFalse();
    assertThat(sm.isResyncInProgress()).as("the node is ready again").isFalse();
    assertThat(sm.hasLeaderServiceGap()).as("and no longer hands the leadership off for it").isFalse();
    assertThat(sm.readAppliedIndexCounter()).as("the checkpoint counter follows the accepted position")
        .isEqualTo(SNAPSHOT_INDEX);
    assertThat(raft.calls("notifyApplied")).as("the reads the floor held back are woken").hasSize(1);

    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains("user 'root' (from 10.0.0.9)").contains(String.valueOf(PERSISTED_APPLIED))
        .contains(String.valueOf(SNAPSHOT_INDEX)).contains("#9498");

    // The restart path: the same marker, read against what is now persisted
    sm.reinitialize();
    assertThat(sm.getStaleSnapshotAppliedFloor()).as("the override was persisted, the gap is not raised again")
        .isNegative();
    assertThat(sm.isResyncInProgress()).isFalse();

    // And the file itself says so, for a state machine that has never seen this one's memory
    final ArcadeStateMachine fresh = newStateMachine();
    try {
      assertThat(fresh.readPersistedAppliedIndex()).isEqualTo(SNAPSHOT_INDEX);
    } finally {
      fresh.close();
    }
  }

  /**
   * The counter-case: with a peer, the floor is lifted by a resync from it (a leader hands the leadership off first).
   * Accepting the gap by hand would leave this node silently short of entries the others applied. Refused, and the
   * floor stays.
   */
  @Test
  void aNodeThatIsNotTheSoleVoterIsRefusedAndKeepsItsFloor() {
    assertThatThrownBy(() -> RaftHAPlugin.acceptStaleSnapshot(sm, false, SERVER, "user 'root'"))
        .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
        .hasMessageContaining("not the only voter");

    assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(PERSISTED_APPLIED);
    assertThat(sm.isResyncInProgress()).isTrue();
    assertThat(sm.readPersistedAppliedIndex()).isEqualTo(PERSISTED_APPLIED);
  }

  /** Nothing to accept: a node with no floor is a 404, never a silent 200, whatever its voter count. */
  @Test
  void aNodeWithNoFloorIsNotFound() throws Exception {
    RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root'");

    assertThatThrownBy(() -> RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root'"))
        .isInstanceOf(ServerControlPlane.NotFoundException.class)
        .hasMessageContaining("no stale-snapshot read floor");
    assertThatThrownBy(() -> RaftHAPlugin.acceptStaleSnapshot(sm, false, SERVER, "user 'root'"))
        .isInstanceOf(ServerControlPlane.NotFoundException.class);
  }

  /**
   * A download that is running owns the floor: it clears it when it lands, or re-arms it when it fails. The override
   * steps aside rather than racing it, and lifts nothing.
   */
  @Test
  void aRunningDownloadIsNotOverridden() throws Exception {
    setAtomicBoolean(sm, "snapshotDownloadInProgress", true);
    try {
      assertThatThrownBy(() -> RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root'"))
          .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
          .hasMessageContaining("download is running");
      assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(PERSISTED_APPLIED);
    } finally {
      setAtomicBoolean(sm, "snapshotDownloadInProgress", false);
    }
  }

  /**
   * An override that does not reach the disk would be undone by the next restart, which raises the floor again from the
   * marker and the file. So a failed write is reported, and neither the floor nor the in-memory positions move.
   */
  @Test
  void aFailedWriteLiftsNothing() throws Exception {
    // The atomic rename cannot replace a non-empty directory
    final Path file = root.resolve("databases").resolve(".raft").resolve("applied-index");
    Files.delete(file);
    Files.createDirectories(file);
    Files.writeString(file.resolve("blocker"), "x");

    assertThatThrownBy(() -> RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root'"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("restart");

    assertThat(sm.getStaleSnapshotAppliedFloor()).as("the floor stays").isEqualTo(PERSISTED_APPLIED);
    assertThat(sm.isResyncInProgress()).isTrue();
    assertThat(sm.readPersistedAppliedIndex()).as("the in-memory position is put back").isEqualTo(PERSISTED_APPLIED);
    assertThat(sm.readPersistedAppliedIndex(HEALTHY_DB)).isEqualTo(PERSISTED_APPLIED);
    assertThat(raft.calls("notifyApplied")).isEmpty();
  }

  /**
   * The floor is node-wide; a per-database quarantine is not this override's to lift (that is accept-diverged, #9449). A
   * quarantined database keeps its quarantine and its own applied position, while every other present database is
   * recorded at the accepted position.
   */
  @Test
  void aQuarantinedDatabaseKeepsItsQuarantineAndItsOwnPosition() throws Exception {
    sm.close();
    sm = newStateMachine(Set.of(HEALTHY_DB, QUARANTINED_DB));
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
    sm.writePersistedAppliedIndex(PERSISTED_APPLIED - 5, QUARANTINED_DB);
    sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
    sm.setRaftHAServer(raft);
    sm.reinitialize();
    sm.markStateDiverged(QUARANTINED_DB, DivergenceCause.APPLY_ERROR);
    assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(PERSISTED_APPLIED);

    RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root'");

    assertThat(sm.getStaleSnapshotAppliedFloor()).isNegative();
    assertThat(sm.readPersistedAppliedIndex(HEALTHY_DB)).isEqualTo(SNAPSHOT_INDEX);
    assertThat(sm.isDatabaseDiverged(QUARANTINED_DB)).as("the quarantine is not lifted by this override").isTrue();
    assertThat(sm.readPersistedAppliedIndex(QUARANTINED_DB)).isEqualTo(PERSISTED_APPLIED - 5);
    assertThat(sm.isResyncInProgress()).as("the quarantine still keeps the node out of the ready set").isTrue();
  }

  /**
   * The apply thread keeps going past the marker while the floor stands. The accepted position is the higher of the
   * two, so the override never moves the persisted position backwards.
   */
  @Test
  void anApplyPositionPastTheMarkerIsNeverRegressed() throws Exception {
    sm.writePersistedAppliedIndex(SNAPSHOT_INDEX + 40, HEALTHY_DB);

    final JSONObject result = RaftHAPlugin.acceptStaleSnapshot(sm, true, SERVER, "user 'root'");

    assertThat(result.getLong("appliedIndex")).isEqualTo(SNAPSHOT_INDEX + 40);
    assertThat(sm.readPersistedAppliedIndex()).isEqualTo(SNAPSHOT_INDEX + 40);
    assertThat(sm.readPersistedAppliedIndex(HEALTHY_DB)).isEqualTo(SNAPSHOT_INDEX + 40);
  }

  /** A node running a non-Raft HA implementation answers the gRPC and HTTP callers with a refusal, never a no-op. */
  @Test
  void theDefaultHaImplementationRefuses() {
    final HAServerPlugin minimal = new MinimalHaPlugin();
    assertThatThrownBy(() -> minimal.acceptStaleSnapshot("user 'root'"))
        .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class);
  }

  /** The sole-voter "no peer" report names the new way out, next to accept-diverged's. */
  @Test
  void theSoleVoterReportNamesTheOverride() {
    assertThat(RaftHAServer.noHandoffPeerReport("a node-wide read floor at 100", true))
        .contains(PostAcceptStaleSnapshotHandler.ROUTE);
  }

  private ArcadeStateMachine newStateMachine() {
    return newStateMachine(null);
  }

  /** {@code presentDatabases} stands in for the databases a started server would hold; {@code null} keeps the real set. */
  private ArcadeStateMachine newStateMachine(final Set<String> presentDatabases) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    final ArcadeDBServer server = presentDatabases == null ? new ArcadeDBServer(config) : new ArcadeDBServer(config) {
      @Override
      public Set<String> getDatabaseNames() {
        return presentDatabases;
      }
    };
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    stateMachine.setServer(server);
    return stateMachine;
  }

  /** Writes a real {@code snapshot.<term>_<index>} marker through the state machine's own registration path. */
  private static void registerMarkerAt(final ArcadeStateMachine stateMachine, final long term, final long index)
      throws Exception {
    final Method m = ArcadeStateMachine.class.getDeclaredMethod("registerSnapshotMarker", long.class, long.class);
    m.setAccessible(true);
    assertThat((Boolean) m.invoke(stateMachine, term, index)).as("snapshot marker written").isTrue();
  }

  private static void setAtomicBoolean(final ArcadeStateMachine stateMachine, final String name, final boolean value)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField(name);
    f.setAccessible(true);
    ((AtomicBoolean) f.get(stateMachine)).set(value);
  }

  /** {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue9498AcceptStaleSnapshotFloorTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  private static Logger capturingWarningsOf(final Class<?> requesterType, final List<String> warnings) {
    return new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (level == Level.WARNING && requesterType.isInstance(requester))
          warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        log(requester, level, message, exception, context,
            new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
                arg15, arg16, arg17 });
      }

      @Override
      public void flush() {
      }
    };
  }

  /** An HA implementation that predates the override: everything it is asked for is the interface's default. */
  private static class MinimalHaPlugin implements HAServerPlugin {
    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return true;
    }

    @Override
    public String getLeaderName() {
      return null;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 1;
    }

    @Override
    public String getLeaderAddress() {
      return null;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }
}
