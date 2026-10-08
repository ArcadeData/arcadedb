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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9449: #9308 stopped a sole voter from RAISING a quarantine, but one that already stands -
 * restored from {@code .raft/applied-index} (#7735), or raised while the cluster still had peers - never lifted: nothing
 * clears a quarantine but a resync from a peer or a DROP, and a sole voter has no peer. The node stayed not-ready and its
 * Raft log un-checkpointed for good. {@code POST /api/v1/cluster/accept-diverged/{database}} (and its gRPC twin, both
 * through {@link RaftHAPlugin#acceptDivergedDatabase(String, String)}) is the audited override.
 * <p>
 * Driven through the shared core with a real {@link ArcadeStateMachine} persisting to a real {@code .raft} directory, so a
 * "restart" is a second state machine reading the same file; the voter count is the one input chosen by the test.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9449AcceptDivergedDatabaseTest {

  private static final String DB_NAME = "db9449";
  private static final String SERVER  = "ArcadeDB_9449";

  @TempDir
  Path root;

  /**
   * The issue as reported: a quarantine written to disk by an earlier run (here, one with its read floor, as an
   * incomplete snapshot install leaves it) comes back on a sole voter and keeps it not-ready. Accepting the copy lifts
   * both, the node is ready again, and the next restart does not bring the quarantine back.
   */
  @Test
  void aQuarantineRestoredFromDiskIsLiftedAndStaysLiftedAfterARestart() throws IOException {
    writeAppliedIndexFile("{\"global\":120,\"db\":{\"" + DB_NAME + "\":117},\"quarantine\":{\"" + DB_NAME
        + "\":\"SNAPSHOT_INSTALL_INCOMPLETE\"},\"floors\":{\"" + DB_NAME + "\":119}}");

    final ArcadeStateMachine restarted = newStateMachine();
    try {
      assertThat(restarted.isDatabaseDiverged(DB_NAME)).as("the quarantine came back from disk").isTrue();
      assertThat(restarted.isResyncInProgress()).as("which keeps the node out of the ready set").isTrue();

      final JSONObject result = RaftHAPlugin.acceptDivergedDatabase(restarted, true, SERVER, DB_NAME, "user 'root'");

      assertThat(result.getString("database")).isEqualTo(DB_NAME);
      assertThat(result.getString("localServer")).isEqualTo(SERVER);
      assertThat(result.getLong("appliedIndex")).isEqualTo(117L);
      assertThat(result.getString("divergenceCause")).isEqualTo(DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE.name());
      assertThat(result.getLong("readFloor")).isEqualTo(119L);
      assertThat(result.getString("result")).contains("not replayed");

      assertThat(restarted.isDatabaseDiverged(DB_NAME)).isFalse();
      assertThat(restarted.getDatabaseAppliedFloor(DB_NAME)).as("the read floor goes with it").isNegative();
      assertThat(restarted.isResyncInProgress()).as("the node is ready again").isFalse();
    } finally {
      restarted.close();
    }

    final ArcadeStateMachine again = newStateMachine();
    try {
      assertThat(again.isDatabaseDiverged(DB_NAME)).as("the override was persisted, not only applied in memory").isFalse();
      assertThat(again.getDatabaseAppliedFloor(DB_NAME)).isNegative();
      assertThat(again.isResyncInProgress()).isFalse();
      assertThat(again.readPersistedAppliedIndex(DB_NAME)).as("the applied position is kept").isEqualTo(117L);
    } finally {
      again.close();
    }
  }

  /**
   * The other shape the issue names: a quarantine raised while the cluster still had peers (the apply-error route), after
   * which the configuration shrank to this one voter. The override is logged at WARNING with who made it, at which
   * applied index, over which cause.
   */
  @Test
  void aQuarantineRaisedWithPeersIsLiftedOnceTheNodeIsTheSoleVoterAndTheOverrideIsAudited() throws IOException {
    final ArcadeStateMachine sm = newStateMachine();
    try {
      sm.writePersistedAppliedIndex(42L, DB_NAME);
      sm.markStateDiverged(DB_NAME, DivergenceCause.APPLY_ERROR);
      assertThat(sm.isResyncInProgress()).isTrue();

      final List<String> warnings = new CopyOnWriteArrayList<>();
      final Logger previous = LogManager.instance().getLogger();
      LogManager.instance().setLogger(capturingWarningsOf(ArcadeStateMachine.class, warnings));
      final JSONObject result;
      try {
        result = RaftHAPlugin.acceptDivergedDatabase(sm, true, SERVER, DB_NAME, "user 'root' (from 10.0.0.9)");
      } finally {
        LogManager.instance().setLogger(previous);
      }

      assertThat(result.getLong("appliedIndex")).isEqualTo(42L);
      assertThat(result.getString("divergenceCause")).isEqualTo(DivergenceCause.APPLY_ERROR.name());
      assertThat(result.has("readFloor")).as("no read floor stood").isFalse();
      assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();

      assertThat(warnings).hasSize(1);
      assertThat(warnings.getFirst()).contains(DB_NAME).contains("user 'root' (from 10.0.0.9)").contains("42")
          .contains(DivergenceCause.APPLY_ERROR.getDescription()).contains("#9449");
    } finally {
      sm.close();
    }
  }

  /**
   * The counter-case: with peers, a resync is the way out, and lifting the quarantine by hand would leave this copy
   * silently different from the others'. Refused, and the quarantine stays.
   */
  @Test
  void aNodeThatIsNotTheSoleVoterIsRefusedAndKeepsItsQuarantine() throws IOException {
    final ArcadeStateMachine sm = newStateMachine();
    try {
      sm.markStateDiverged(DB_NAME, DivergenceCause.WAL_VERSION_GAP);

      assertThatThrownBy(() -> RaftHAPlugin.acceptDivergedDatabase(sm, false, SERVER, DB_NAME, "user 'root'"))
          .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
          .hasMessageContaining("not the only voter")
          .hasMessageContaining("/api/v1/cluster/resync/" + DB_NAME);

      assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
      assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.WAL_VERSION_GAP);
    } finally {
      sm.close();
    }
  }

  /** Nothing to accept: a database with no quarantine and no read floor is a 404, never a silent 200. */
  @Test
  void aDatabaseWithNothingStandingIsNotFound() throws IOException {
    final ArcadeStateMachine sm = newStateMachine();
    try {
      assertThatThrownBy(() -> RaftHAPlugin.acceptDivergedDatabase(sm, true, SERVER, DB_NAME, "user 'root'"))
          .isInstanceOf(ServerControlPlane.NotFoundException.class)
          .hasMessageContaining("not quarantined");
      // Checked before the voter count: on a node with peers the answer is still "nothing here"
      assertThatThrownBy(() -> RaftHAPlugin.acceptDivergedDatabase(sm, false, SERVER, DB_NAME, "user 'root'"))
          .isInstanceOf(ServerControlPlane.NotFoundException.class);
    } finally {
      sm.close();
    }
  }

  /** The name reaches a log line and the applied-index file's keys: a malformed one is refused before anything else. */
  @Test
  void aMalformedDatabaseNameIsRefused() throws IOException {
    final ArcadeStateMachine sm = newStateMachine();
    try {
      assertThatThrownBy(() -> RaftHAPlugin.acceptDivergedDatabase(sm, true, SERVER, "../etc", "user 'root'"))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> RaftHAPlugin.acceptDivergedDatabase(sm, true, SERVER, "", "user 'root'"))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      sm.close();
    }
  }

  /**
   * An override that does not reach the disk would be undone by the next restart, which restores the quarantine from the
   * file. So a failed write is reported and nothing is lifted, unlike the quarantine's own best-effort write.
   */
  @Test
  void aFailedWriteLiftsNothing() throws IOException {
    final ArcadeStateMachine sm = newStateMachine();
    try {
      sm.markStateDiverged(DB_NAME, DivergenceCause.APPLY_ERROR);
      assertThat(sm.isResyncInProgress()).isTrue();

      // The atomic rename cannot replace a non-empty directory
      final Path file = appliedIndexFile();
      Files.delete(file);
      Files.createDirectories(file);
      Files.writeString(file.resolve("blocker"), "x");

      assertThatThrownBy(() -> RaftHAPlugin.acceptDivergedDatabase(sm, true, SERVER, DB_NAME, "user 'root'"))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("restart");

      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the quarantine stays").isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    } finally {
      sm.close();
    }
  }

  /** A node running a non-Raft HA implementation answers the gRPC and HTTP callers with a refusal, never a no-op. */
  @Test
  void theDefaultHaImplementationRefuses() {
    final HAServerPlugin minimal = new MinimalHaPlugin();
    assertThatThrownBy(() -> minimal.acceptDivergedDatabase(DB_NAME, "user 'root'"))
        .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class);
  }

  private ArcadeStateMachine newStateMachine() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  private Path appliedIndexFile() {
    return root.resolve("databases").resolve(".raft").resolve("applied-index");
  }

  private void writeAppliedIndexFile(final String content) throws IOException {
    final Path file = appliedIndexFile();
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
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
