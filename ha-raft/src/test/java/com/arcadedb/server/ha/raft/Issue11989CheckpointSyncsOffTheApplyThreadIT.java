/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The periodic log compaction's checkpoint forces the data files of every open database (issue #11258), and Ratis runs
 * the checkpoint on its StateMachineUpdater, which is also the apply thread: every commit of the node waited for that
 * forcing, up to 1.6 s in an application's ingest (issue #11989). The compaction now forces them on its own thread before
 * it asks for the checkpoint, and leaves the apply thread only what was written in between.
 */
class Issue11989CheckpointSyncsOffTheApplyThreadIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHook() {
    ArcadeStateMachine.TEST_BEFORE_CHECKPOINT_SYNC = null;
  }

  @Test
  void theCompactionForcesTheDataFilesBeforeTheApplyThreadCheckpoints() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
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

  private static List<String> unsyncedFiles(final LocalDatabase db) {
    final List<String> unsynced = new ArrayList<>();
    for (final ComponentFile f : db.getFileManager().getFiles())
      if (f instanceof PaginatedComponentFile pcf && pcf.isModifiedSinceLastSync())
        unsynced.add(pcf.getFileName());
    return unsynced;
  }
}
