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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of the production wiring behind the periodic Raft log compaction (issue #5345),
 * against a real {@link RaftHAServer} cluster.
 * <p>
 * {@link RaftPeriodicSnapshotCompactionIT} proves the Ratis mechanism itself but builds the request
 * itself against a mini-cluster; the unit tests drive {@link RaftLogCompactionScheduler} against a
 * fake. This test covers the seam between them - {@code RaftHAServer.takeLocalSnapshot()} assembling
 * the request from the real client id, local peer id and group id, and the compaction target resolving
 * the Raft storage volume for the free-space probe. A wrong group id or an unresolvable storage volume
 * would be invisible to both of the other suites.
 * <p>
 * It also covers where the checkpoint's data-file forcing runs (issue #11989). The checkpoint forces the data files of
 * every open database (issue #11258) and Ratis runs it on its StateMachineUpdater, which is also the apply thread, so
 * every commit of the node waited for that forcing. The compaction target forces them on its own thread before it asks
 * for the checkpoint, and leaves the apply thread only what was written in between.
 */
@Tag("slow")
class RaftLogCompactionWiringIT extends BaseRaftHATest {

  @AfterEach
  void clearCheckpointHook() {
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = null;
  }

  @Test
  void takeLocalSnapshotSucceedsOnEveryNodeThroughTheProductionRequestPath() throws Exception {
    // Give the state machine something to snapshot, and let every peer apply it.
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      assertThat(plugin).as("server %d must expose a Raft plugin", i).isNotNull();

      final RaftHAServer haServer = plugin.getRaftHAServer();
      assertThat(haServer).as("server %d must expose a Raft HA server", i).isNotNull();

      // A creation gap of 1 is what the scheduler uses under disk pressure. Leader and followers alike
      // must accept the request: each node purges its own log against its own snapshot index.
      final long snapshotIndex = haServer.takeLocalSnapshot(1L);
      assertThat(snapshotIndex)
          .as("server %d (leader=%s) must take a local snapshot through the production request path",
              i, haServer.isLeader())
          .isNotNegative();
    }
  }

  @Test
  void compactionTargetResolvesTheRaftStorageVolumeForTheFreeSpaceProbe() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAServer haServer = getRaftPlugin(i).getRaftHAServer();
      final RaftLogCompactionScheduler.CompactionTarget target = haServer.newCompactionTargetForTesting();

      // A zero or negative total would make the scheduler read the volume as "size unknown" and disable
      // the disk-pressure escalation entirely, silently losing the guard against the log filling the disk.
      assertThat(target.getRaftStorageTotalSpaceBytes())
          .as("server %d must resolve a real Raft storage volume size", i)
          .isPositive();
      assertThat(target.getRaftStorageUsableSpaceBytes())
          .as("server %d must report free space on the Raft storage volume", i)
          .isNotNegative();
      assertThat(target.getRaftStorageDescription())
          .as("server %d must name the Raft storage location for the disk-pressure warning", i)
          .isNotBlank()
          .isNotEqualTo("<unknown>");
      assertThat(target.isShutdownRequested())
          .as("server %d is running, so the tick must not be suppressed", i)
          .isFalse();
    }
  }

  @Test
  void theCompactionForcesTheDataFilesBeforeTheApplyThreadCheckpoints() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final LocalDatabase local = writeAccountsLeavingFilesToForce(leaderIndex);

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final ArcadeStateMachine leaderStateMachine = leader.getStateMachine();
    final AtomicBoolean checkpointed = new AtomicBoolean();
    final List<String> leftToTheApplyThread = new CopyOnWriteArrayList<>();
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = stateMachine -> {
      if (stateMachine != leaderStateMachine)
        return;
      checkpointed.set(true);
      leftToTheApplyThread.addAll(unsyncedFiles(local));
    };

    final long index = leader.newCompactionTargetForTesting().triggerSnapshot(1);

    assertThat(index).as("precondition: the compaction took a checkpoint").isPositive();
    assertThat(checkpointed.get()).as("precondition: the checkpoint reached its data-file sync").isTrue();
    assertThat(leftToTheApplyThread)
        .as("the compaction forced the data files on its own thread: the apply thread had none left to force (issue #11989)")
        .isEmpty();
    assertThat(unsyncedFiles(local)).as("and the checkpoint's guarantee holds: nothing it covers still owes the disk").isEmpty();
  }

  @Test
  void aTickBelowTheCreationGapForcesNoDataFile() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final LocalDatabase local = writeAccountsLeavingFilesToForce(leaderIndex);

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final ArcadeStateMachine leaderStateMachine = leader.getStateMachine();
    final AtomicBoolean checkpointed = new AtomicBoolean();
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = stateMachine -> {
      if (stateMachine == leaderStateMachine)
        checkpointed.set(true);
    };

    // A gap no node reaches: Ratis answers the request without a checkpoint, so nothing is there to force ahead of
    final long index = leader.newCompactionTargetForTesting().triggerSnapshot(Long.MAX_VALUE);

    assertThat(index).as("precondition: Ratis answered the request below the gap").isNotNegative();
    assertThat(checkpointed.get()).as("precondition: Ratis took no checkpoint below the creation gap").isFalse();
    assertThat(unsyncedFiles(local))
        .as("a tick that takes no checkpoint must not pay the data-file forcing either (issue #11989)")
        .isNotEmpty();
  }

  /** Commits on the leader, waits for the pages to reach the files and asserts that the files still owe their fsync. */
  private LocalDatabase writeAccountsLeavingFilesToForce(final int leaderIndex) {
    final Database db = getServerDatabase(leaderIndex, getDatabaseName());
    db.transaction(() -> db.getSchema().createDocumentType("Account"));
    db.transaction(() -> {
      for (int i = 0; i < 500; i++)
        db.newDocument("Account").set("i", i).save();
    });

    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) db).getEmbedded();
    assertThat(local.getPageManager().waitAllPagesOfDatabaseAreFlushed(local)).as("the fixture: the pages reached the files")
        .isTrue();
    assertThat(unsyncedFiles(local)).as("the fixture: the commits left data files to force").isNotEmpty();
    return local;
  }

  private static List<String> unsyncedFiles(final LocalDatabase db) {
    final List<String> unsynced = new ArrayList<>();
    for (final ComponentFile f : db.getFileManager().getFiles())
      if (f instanceof PaginatedComponentFile pcf && pcf.isModifiedSinceLastSync())
        unsynced.add(pcf.getFileName());
    return unsynced;
  }
}
