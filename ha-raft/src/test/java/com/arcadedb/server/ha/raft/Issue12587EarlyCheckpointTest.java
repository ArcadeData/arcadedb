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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #12587: two callers of the checkpoint still forced every data file written since the previous one on the apply
 * thread, where every commit of the node waits for it, after #11989 had moved that forcing off it for the periodic
 * compaction only.
 * <ul>
 *   <li>the Ratis entry-count auto-trigger ({@code arcadedb.ha.snapshotThreshold}), which runs the checkpoint straight from
 *   the StateMachineUpdater: the apply path now asks the compaction scheduler for the checkpoint before the threshold is
 *   reached, and the scheduler forces the files on its own thread first;</li>
 *   <li>the Raft log purge before a snapshot install under disk pressure, which now forces them on the installer's thread
 *   first.</li>
 * </ul>
 * What is asserted is what the apply thread was left to force (the files still owing their fsync when the checkpoint
 * reaches its own forcing pass), never a duration.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12587EarlyCheckpointTest extends BaseRaftHATest {

  private static final long SNAPSHOT_THRESHOLD = 40L;

  @Override
  protected int getServerCount() {
    return 1;
  }

  /** The base class turns HA on only for two servers or more; this cluster is one voter on purpose. */
  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, getServerAddresses());
    config.setValue(GlobalConfiguration.HA_ENABLED, true);
    // A threshold the test reaches with a few dozen commits; the periodic compaction keeps its 5 minutes, so it cannot be
    // what takes the checkpoint below
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD, SNAPSHOT_THRESHOLD);
  }

  @AfterEach
  void clearCheckpointHook() {
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = null;
  }

  // Regression for #12587: the entry-count checkpoint is taken off the apply thread, early pass first
  @Test
  @Timeout(120)
  void theEntryCountCheckpointFindsTheDataFilesAlreadyForced() {
    final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
    final ArcadeStateMachine sm = raft.getStateMachine();
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> db.getSchema().createDocumentType("Account"));
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) db).getEmbedded();

    final long gap = sm.getEarlyCheckpointGap();
    assertThat(gap).as("the early checkpoint is asked at three quarters of the threshold").isEqualTo(30L);

    // Out of any window the fixture's own entries left in flight: the checkpoint it asked for has landed and re-armed
    await().atMost(Duration.ofSeconds(30)).until(() -> sm.getEarlyCheckpointWakeIndex() != Long.MAX_VALUE
        && sm.lastAppliedIndex.get() < sm.getEarlyCheckpointWakeIndex());

    final List<List<String>> leftToTheApplyThread = new CopyOnWriteArrayList<>();
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = stateMachine -> {
      if (stateMachine == sm)
        leftToTheApplyThread.add(unsyncedFiles(local));
    };

    final long snapshotBefore = latestSnapshotIndex(sm);
    // One entry per commit, up to the entry that asks for the checkpoint, and not one more: what the apply thread is left
    // to force is then only what the early pass could not see, which is nothing
    int commits = 0;
    while (sm.getEarlyCheckpointWakeIndex() != Long.MAX_VALUE && latestSnapshotIndex(sm) == snapshotBefore) {
      assertThat(commits).as("the early checkpoint is asked within the gap").isLessThan((int) gap + 5);
      final int i = commits++;
      db.transaction(() -> db.newDocument("Account").set("i", i).save());
    }

    // The marker is registered, then the next window armed from it
    await().atMost(Duration.ofSeconds(30))
        .until(() -> latestSnapshotIndex(sm) > snapshotBefore && sm.getEarlyCheckpointWakeIndex() != Long.MAX_VALUE);

    assertThat(sm.lastAppliedIndex.get() - snapshotBefore)
        .as("the checkpoint was taken below the threshold, so not by the Ratis auto-trigger")
        .isLessThan(SNAPSHOT_THRESHOLD);
    assertThat(leftToTheApplyThread).as("precondition: the checkpoint reached its data-file sync").isNotEmpty();
    assertThat(leftToTheApplyThread)
        .as("the data files were forced off the apply thread before the entry-count checkpoint (issue #12587)")
        .allSatisfy(files -> assertThat(files).isEmpty());
    assertThat(sm.getEarlyCheckpointWakeIndex())
        .as("the next window is armed from the new marker")
        .isEqualTo(Math.max(latestSnapshotIndex(sm), sm.lastAppliedIndex.get()) + gap);
  }

  // Regression for #12587: the Raft log purge before a snapshot install forces the data files before it asks for the
  // checkpoint
  @Test
  @Timeout(120)
  void thePurgeBeforeASnapshotInstallForcesTheDataFilesFirst() {
    final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
    final ArcadeStateMachine sm = raft.getStateMachine();
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> db.getSchema().createDocumentType("Account"));
    db.transaction(() -> {
      for (int i = 0; i < 500; i++)
        db.newDocument("Account").set("i", i).save();
    });
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) db).getEmbedded();
    assertThat(local.getPageManager().waitAllPagesOfDatabaseAreFlushed(local)).as("the fixture: the pages reached the files")
        .isTrue();
    assertThat(unsyncedFiles(local)).as("the fixture: the commits left data files to force").isNotEmpty();

    final List<List<String>> leftToTheApplyThread = new CopyOnWriteArrayList<>();
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = stateMachine -> {
      if (stateMachine == sm)
        leftToTheApplyThread.add(unsyncedFiles(local));
    };

    // Every volume is below 100% free, so the purge runs
    final int previous = raft.configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC);
    raft.configuration.setValue(GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC, 100);
    try {
      raft.compactRaftLogBeforeSnapshotInstall(getDatabaseName());
    } finally {
      raft.configuration.setValue(GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC, previous);
    }

    assertThat(leftToTheApplyThread).as("precondition: the purge took a checkpoint").isNotEmpty();
    assertThat(leftToTheApplyThread)
        .as("the purge forced the data files on its own thread: the apply thread had none left to force (issue #12587)")
        .allSatisfy(files -> assertThat(files).isEmpty());
  }

  private static long latestSnapshotIndex(final ArcadeStateMachine sm) {
    final var latest = sm.storage.getLatestSnapshot();
    return latest != null ? latest.getIndex() : -1L;
  }

  private static List<String> unsyncedFiles(final LocalDatabase db) {
    final List<String> unsynced = new ArrayList<>();
    for (final ComponentFile f : db.getFileManager().getFiles())
      if (f instanceof PaginatedComponentFile pcf && pcf.isModifiedSinceLastSync())
        unsynced.add(pcf.getFileName());
    return unsynced;
  }
}
