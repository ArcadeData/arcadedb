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
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7735.
 * <p>
 * A quarantine ({@code divergedDatabases}) deliberately does NOT advance {@code lastAppliedIndex} for the entry
 * that tripped it - {@code applyTransaction} returns a failed future before the {@code getAndSet} - while every
 * LATER committed entry does advance it. The only thing standing between that and a permanently short replica
 * was an in-memory {@code ConcurrentHashMap} that nothing persisted and {@code reinitialize()} did not
 * reconstruct, so a restart produced a node that was {@code RUNNING}, answered 200 on {@code /api/v1/ready},
 * reported {@code alerts: []} and was missing a committed mutation. {@code takeSnapshot()} made that permanent
 * rather than self-correcting: it had no divergence guard, so a quarantined node still checkpointed past the
 * skipped index and authorised Ratis to purge the log through it.
 * <p>
 * The invariant these tests pin: <b>a database quarantined from the committed Raft log stays quarantined across
 * a restart, and the Raft log is never checkpointed past the entry the quarantine skipped.</b>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7735QuarantineSurvivesRestartTest {

  private static final String DB_A = "orders";
  private static final String DB_B = "invoices";

  // ---------------------------------------------------------------------------------------------
  // One test per quarantine entry point: each must survive the restart under its own cause
  // ---------------------------------------------------------------------------------------------

  /**
   * {@code handleUnexpectedApplyError} on an entry this node cannot decode: the trigger issue #7495 added and
   * the one the report was filed against, because it is the trigger most likely to get a pod restarted.
   */
  @Test
  void anUndecodableEntryQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir) throws Exception {
    quarantineSurvives(tempDir, DivergenceCause.UNDECODABLE_LOG_ENTRY);
  }

  /** {@code applyReplicatedTransaction}'s WAL version gap (issues #4740, #4797). */
  @Test
  void aWalVersionGapQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir) throws Exception {
    quarantineSurvives(tempDir, DivergenceCause.WAL_VERSION_GAP);
  }

  /** {@code handleUnexpectedApplyError}'s general apply failure (issue #4797). */
  @Test
  void anApplyErrorQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir) throws Exception {
    quarantineSurvives(tempDir, DivergenceCause.APPLY_ERROR);
  }

  /** {@code settleDivergedStateAfterInstall}'s incomplete install (issue #6760). */
  @Test
  void anIncompleteSnapshotInstallQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir)
      throws Exception {
    quarantineSurvives(tempDir, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
  }

  /**
   * The whole point of persisting it: the restarted node must refuse readiness and say so in the status
   * document, instead of coming back {@code alerts: []} on a database that is missing a committed entry.
   */
  @Test
  void theRestartedNodeIsUnreadyAndSaysWhyRatherThanReportingNoAlerts(@TempDir final Path tempDir)
      throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      assertThat(before.isResyncInProgress()).isTrue();
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.isResyncInProgress())
          .as("readiness must still answer 503 after the restart, not 200 on a short database")
          .isTrue();

      final ArcadeStateMachine.LocalResyncState state = after.getLocalResyncState();
      assertThat(state.inProgress()).isTrue();
      assertThat(state.divergedDatabases()).containsExactly(DB_A);
      assertThat(state.divergenceCauses()).containsEntry(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
    } finally {
      after.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // takeSnapshot(): the log must stay replayable past the entry the quarantine skipped
  // ---------------------------------------------------------------------------------------------

  /**
   * Before the fix this returned the applied index, which covers entries that came AFTER one this node never
   * applied, and Ratis was free to purge the log through it. That is what turned a replayable skip into a
   * permanent one.
   */
  @Test
  void takeSnapshotIsRefusedWhileADatabaseIsQuarantined(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      setLastApplied(sm, 7L, 4242L);

      sm.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);

      assertThat(sm.takeSnapshot())
          .as("a checkpoint here would authorise Ratis to purge the entry the quarantine skipped")
          .isEqualTo(RaftLog.INVALID_LOG_INDEX);
      assertThat(sm.getStateMachineStorage().getLatestSnapshot())
          .as("and no marker may be left behind either")
          .isNull();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /** The refusal is not a one-way door: once the resync has cleared the quarantine, checkpointing resumes. */
  @Test
  void takeSnapshotResumesOnceTheResyncClearedTheQuarantine(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      setLastApplied(sm, 7L, 4242L);

      sm.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
      assertThat(sm.takeSnapshot()).isEqualTo(RaftLog.INVALID_LOG_INDEX);

      sm.clearDivergedDatabase(DB_A);

      assertThat(sm.takeSnapshot()).isEqualTo(4242L);
      assertThat(sm.getStateMachineStorage().getLatestSnapshot()).isNotNull();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The inverse bug: a healed database must not come back quarantined
  // ---------------------------------------------------------------------------------------------

  /** A targeted resync clears one database; the other one's quarantine must survive, and only that one. */
  @Test
  void aTargetedResyncDropsOnlyItsOwnDatabaseFromTheDurableQuarantine(@TempDir final Path tempDir)
      throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
      before.markStateDiverged(DB_B, DivergenceCause.APPLY_ERROR);
      before.clearDivergedDatabase(DB_A);
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.isDatabaseDiverged(DB_A)).as("the resync healed it, so it must not come back").isFalse();
      assertThat(after.isDatabaseDiverged(DB_B)).isTrue();
      assertThat(after.getLocalResyncState().divergedDatabases()).containsExactly(DB_B);
    } finally {
      after.close();
    }
  }

  /** A full resync reinstalls every database, so nothing may be quarantined after the restart. */
  @Test
  void aFullResyncLeavesNoQuarantineBehindForTheNextStart(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
      before.markStateDiverged(DB_B, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      before.clearDivergedState();
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.getLocalResyncState().divergedDatabases()).isEmpty();
      assertThat(after.isResyncInProgress()).as("a healed node must be ready again").isFalse();
    } finally {
      after.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The file it shares with the applied index
  // ---------------------------------------------------------------------------------------------

  /**
   * The quarantine and the applied position it qualifies travel in the SAME atomic write, so a crash cannot
   * leave a file that says "applied up to N" without saying "and this database was skipped on the way".
   */
  @Test
  void theQuarantineAndTheAppliedPositionAreRestoredFromOneFile(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.writePersistedAppliedIndex(4242L, DB_A);
      before.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
    } finally {
      before.close();
    }

    final JSONObject persisted = new JSONObject(Files.readString(appliedIndexFile(tempDir)));
    assertThat(persisted.getLong("global", -1)).isEqualTo(4242L);
    assertThat(persisted.getJSONObject("quarantine").getString(DB_A, null))
        .isEqualTo(DivergenceCause.UNDECODABLE_LOG_ENTRY.name());

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.readPersistedAppliedIndex()).isEqualTo(4242L);
      assertThat(after.readPersistedAppliedIndex(DB_A)).isEqualTo(4242L);
      assertThat(after.isDatabaseDiverged(DB_A)).isTrue();
    } finally {
      after.close();
    }
  }

  /** A healthy node writes the file it always wrote: the key only appears when something is quarantined. */
  @Test
  void aHealthyNodeWritesNoQuarantineKeyAtAll(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.writePersistedAppliedIndex(11L, DB_A);

      final JSONObject persisted = new JSONObject(Files.readString(appliedIndexFile(tempDir)));
      assertThat(persisted.has("quarantine")).isFalse();
    } finally {
      sm.close();
    }
  }

  /** A file written by a build that predates this change carries no quarantine, and reads as none. */
  @Test
  void aFileWrittenBeforeThisFixRestoresNoQuarantine(@TempDir final Path tempDir) throws Exception {
    writeAppliedIndexFile(tempDir, "{\"global\":99,\"db\":{\"" + DB_A + "\":99}}");

    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(99L);
      assertThat(sm.isDatabaseDiverged(DB_A)).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();
    } finally {
      sm.close();
    }
  }

  /**
   * A cause name this build does not know - written by a newer node - degrades to {@code APPLY_ERROR} rather
   * than dropping the entry. The cause only changes what the alert SAYS; dropping it would reinstate exactly
   * the silent divergence the persistence exists to prevent.
   */
  @Test
  void anUnknownCauseKeepsTheQuarantineRatherThanDroppingIt(@TempDir final Path tempDir) throws Exception {
    writeAppliedIndexFile(tempDir,
        "{\"global\":99,\"db\":{},\"quarantine\":{\"" + DB_A + "\":\"A_CAUSE_FROM_THE_FUTURE\"}}");

    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      assertThat(sm.isDatabaseDiverged(DB_A)).as("an unreadable cause must never silence the quarantine").isTrue();
      assertThat(sm.getLocalResyncState().divergenceCauses())
          .containsEntry(DB_A, DivergenceCause.APPLY_ERROR);
    } finally {
      sm.close();
    }
  }

  /**
   * A restored quarantine keeps the cause it was recorded with, and a later mark on the same database does not
   * overwrite it - the same first-cause-wins rule {@code markStateDiverged} has had since issue #7741, extended
   * across the restart. The restored cause IS the first one: it describes the failure that quarantined the
   * database, while every mark after it comes from an entry that hit the same wall on a database already
   * waiting for a resync.
   */
  @Test
  void aRestoredQuarantineKeepsTheCauseItWasRecordedWith(@TempDir final Path tempDir) throws Exception {
    writeAppliedIndexFile(tempDir,
        "{\"global\":99,\"db\":{},\"quarantine\":{\"" + DB_A + "\":\"WAL_VERSION_GAP\"}}");

    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);

      assertThat(sm.getLocalResyncState().divergenceCauses())
          .as("the first cause is the one that describes what went wrong")
          .containsEntry(DB_A, DivergenceCause.WAL_VERSION_GAP);
      assertThat(sm.isDatabaseDiverged(DB_A)).isTrue();
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The batch writer the snapshot-install path uses
  // ---------------------------------------------------------------------------------------------

  /**
   * A snapshot install learns about every database it gave up on at once, so it quarantines them in one write
   * rather than rewriting the applied-index file once per database (code review on PR #8146). The batch has to
   * be as durable as the single-database writer.
   */
  @Test
  void aBatchQuarantineIsWrittenOnceAndRestoresEveryDatabase(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.settleDivergedStateAfterInstall(Set.of(DB_A, DB_B), 100L);
    } finally {
      before.close();
    }

    final JSONObject quarantine = new JSONObject(Files.readString(appliedIndexFile(tempDir)))
        .getJSONObject("quarantine");
    assertThat(quarantine.keySet()).as("one file, both databases").containsExactlyInAnyOrder(DB_A, DB_B);

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.getLocalResyncState().divergedDatabases()).containsExactlyInAnyOrder(DB_A, DB_B);
      assertThat(after.getLocalResyncState().divergenceCauses())
          .containsEntry(DB_A, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE)
          .containsEntry(DB_B, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
    } finally {
      after.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Self-heal: a restored quarantine has no in-JVM resync behind it, so the tick must re-drive one
  // ---------------------------------------------------------------------------------------------

  /**
   * {@code triggerDatabaseResync} runs once, at the mark, in the JVM that raised the quarantine. A quarantine
   * restored from disk has no such attempt behind it, so without the HealthMonitor tick the persisted mark
   * would hold the node out of the ready set for good. The tick used to return immediately unless a read floor
   * was outstanding, which a quarantine on its own never publishes.
   */
  @Test
  void theHealthTickRetriesAResyncForAQuarantineWithNoReadFloor(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      assertThat(readLastRetryMs(sm)).as("nothing outstanding yet").isZero();

      sm.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm))
          .as("the tick must take its throttle slot and drive a resync for the quarantined database")
          .isNotZero();
    } finally {
      sm.close();
    }
  }

  /** ...and a node with nothing quarantined and no floor still does nothing, as it always did. */
  @Test
  void theHealthTickStillDoesNothingOnAHealthyNode(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm)).isZero();
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** Marks {@code DB_A} under {@code cause}, restarts, and asserts the mark and its cause both came back. */
  private static void quarantineSurvives(final Path tempDir, final DivergenceCause cause) throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.markStateDiverged(DB_A, cause);
      assertThat(before.isDatabaseDiverged(DB_A)).isTrue();
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.isDatabaseDiverged(DB_A))
          .as("the quarantine was in-memory only, so the restart used to forget it")
          .isTrue();
      assertThat(after.getLocalResyncState().divergenceCauses())
          .as("and the operator must still be told WHY, not just that something is wrong")
          .containsEntry(DB_A, cause);
    } finally {
      after.close();
    }
  }

  /**
   * A real (unstarted) {@link ArcadeDBServer} rooted at {@code tempDir}, so the state machine resolves
   * {@code .raft/applied-index} under it. Constructing a second one over the same directory is the restart.
   */
  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  private static Path appliedIndexFile(final Path tempDir) {
    return tempDir.resolve("databases").resolve(".raft").resolve("applied-index");
  }

  private static void writeAppliedIndexFile(final Path tempDir, final String content) throws IOException {
    final Path file = appliedIndexFile(tempDir);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }

  private static RaftHAServer followerRaftHAServerMock() {
    final RaftPeerId leader = RaftPeerId.valueOf("peer-b_2434");
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    when(mockRaft.isLeader()).thenReturn(false);
    when(mockRaft.getLeaderId()).thenReturn(leader);
    when(mockRaft.getUnambiguousPeerHttpAddress(leader)).thenReturn("peer-b:2480");
    return mockRaft;
  }

  private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
    f.setAccessible(true);
    return ((AtomicLong) f.get(sm)).get();
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.resolve("raft-storage").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  /**
   * Advances the applied position the way {@code applyTransaction} does: the private {@code lastAppliedIndex}
   * counter {@code takeSnapshot()} reads, plus the BaseStateMachine term/index that supplies the marker term.
   */
  private static void setLastApplied(final ArcadeStateMachine sm, final long term, final long index)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(index);

    final Method m = findMethod(sm.getClass(), "updateLastAppliedTermIndex", long.class, long.class);
    m.setAccessible(true);
    m.invoke(sm, term, index);
  }

  private static Method findMethod(final Class<?> type, final String name, final Class<?>... params)
      throws NoSuchMethodException {
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      try {
        return c.getDeclaredMethod(name, params);
      } catch (final NoSuchMethodException ignored) {
        // walk up to the superclass
      }
    }
    throw new NoSuchMethodException(name);
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
   * {@code getId()}; nothing else is reached on this path.
   */
  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue7735QuarantineSurvivesRestartTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
