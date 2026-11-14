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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11746: with {@code arcadedb.txWalFlush} 1, the leader forced the WAL record of every replicated commit on the Raft apply
 * thread, one entry at a time, although the entry is durable in the Raft log a majority flushed before it committed and every
 * other node applies it with no WAL record at all. The leader now writes the record without forcing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11746LeaderWalNotForcedIT extends BaseRaftHATest {
  private static final int COMMITS = 50;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void theLeaderWritesTheWalOfAReplicatedCommitWithoutForcingIt() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database db = getServerDatabase(leaderIndex, getDatabaseName());
    final DatabaseInternal embedded = ((DatabaseInternal) db).getEmbedded();
    // the database-wide setting, which every transaction context reads at commit (txWalFlush 1, what production mode sets)
    embedded.setWALFlush(WALFile.FlushType.YES_NOMETADATA);
    assertThat(embedded.getTransaction().getWALFlush()).as("the setting this test is about must be in force on the leader")
        .isEqualTo(WALFile.FlushType.YES_NOMETADATA);

    db.transaction(() -> db.getSchema().getOrCreateDocumentType("Issue11746"));

    final TransactionManager transactionManager = embedded.getTransactionManager();
    final Map<String, Object> before = transactionManager.getStats();

    for (int i = 0; i < COMMITS; i++) {
      final int k = i;
      db.transaction(() -> db.newDocument("Issue11746").set("k", k).save());
    }

    final Map<String, Object> after = transactionManager.getStats();
    assertThat((Long) after.get("bytesWritten")).as("the leader still writes the WAL record of each commit")
        .isGreaterThan((Long) before.get("bytesWritten"));
    assertThat((Long) after.get("forcedWALWrites")).as("no replicated commit forces its WAL record on the leader")
        .isEqualTo((Long) before.get("forcedWALWrites"));

    assertClusterConsistency();
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("Issue11746", true)).isEqualTo(COMMITS);
  }
}
