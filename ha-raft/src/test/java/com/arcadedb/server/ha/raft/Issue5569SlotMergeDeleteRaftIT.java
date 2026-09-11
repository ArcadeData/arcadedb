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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * HA replication regression for the DELETE half of the disjoint-slot merge (#5569), the counterpart of
 * {@link Issue5381SlotMergeRaftIT}. A delete rebased on the LEADER frees a slot on a page image the followers never
 * computed themselves: they replay the leader's post-merge page verbatim in Raft log order. If a merged delete
 * produced a page the leader alone considered correct - a freed slot, a wiped record body, a shifted layout after the
 * commit-time compression - the replicas would diverge on the very first one.
 * <p>
 * The workload packs everything onto a single bucket page (BUCKETS 1): survivor records are hammered with same-width
 * in-place updates while other threads keep inserting and deleting short-lived records on the same page. Every
 * page-version conflict between the two is provably false, so the merge must fire on the leader, and afterwards the
 * three nodes must be byte-for-byte identical and hold exactly the survivors with their last written value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5569SlotMergeDeleteRaftIT extends BaseRaftHATest {
  private static final int SURVIVORS          = 10;
  private static final int UPDATES_PER_RECORD = 40;
  private static final int CHURN_THREADS      = 5;
  private static final int CHURN_ROUNDS       = 20;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
  }

  @Test
  void mergedDeletesReplicateIntact() throws Exception {
    final int leaderIndex = 0;

    executeCommand(leaderIndex, "sql", "CREATE document TYPE Doc BUCKETS 1");
    executeCommand(leaderIndex, "sql", "CREATE PROPERTY Doc.id INTEGER");
    executeCommand(leaderIndex, "sql", "CREATE PROPERTY Doc.tag STRING");
    for (int r = 0; r < SURVIVORS; r++)
      executeCommand(leaderIndex, "sql", "INSERT INTO Doc SET id = " + r + ", tag = '" + String.format("%08d", 0) + "'");
    waitForReplicationIsCompleted(leaderIndex);

    final AtomicInteger updates = new AtomicInteger();
    final AtomicInteger deletes = new AtomicInteger();
    final ExecutorService executor = Executors.newFixedThreadPool(SURVIVORS + CHURN_THREADS);
    final List<Future<?>> futures = new ArrayList<>();

    // Fixed-width tag => every update is a same-size in-place overwrite of ONE slot.
    for (int t = 0; t < SURVIVORS; t++) {
      final int recordId = t;
      futures.add(executor.submit(() -> {
        for (int i = 1; i <= UPDATES_PER_RECORD; i++) {
          try {
            assertThat(executeCommand(leaderIndex, "sqlscript",
                "BEGIN;UPDATE Doc SET tag = '" + String.format("%08d", i) + "' WHERE id = " + recordId + ";commit retry 100;"))
                .withFailMessage("Update returned null: record=%d seq=%d", recordId, i).isNotNull();
            updates.incrementAndGet();
          } catch (final Exception e) {
            fail("Update failed: record=" + recordId + " seq=" + i + " error=" + e.getMessage());
          }
        }
      }));
    }

    // Short-lived records created and then deleted on the same page the updaters are writing to. The delete is the
    // operation under test; the insert only keeps feeding the page fresh victims.
    for (int t = 0; t < CHURN_THREADS; t++) {
      final int churnId = 1000 + t * CHURN_ROUNDS;
      futures.add(executor.submit(() -> {
        for (int i = 0; i < CHURN_ROUNDS; i++) {
          final int id = churnId + i;
          try {
            executeCommand(leaderIndex, "sqlscript",
                "BEGIN;INSERT INTO Doc SET id = " + id + ", tag = '" + String.format("%08d", id) + "';commit retry 100;");
            executeCommand(leaderIndex, "sqlscript", "BEGIN;DELETE FROM Doc WHERE id = " + id + ";commit retry 100;");
            deletes.incrementAndGet();
          } catch (final Exception e) {
            fail("Churn failed: id=" + id + " error=" + e.getMessage());
          }
        }
      }));
    }

    for (final Future<?> f : futures)
      f.get(180, TimeUnit.SECONDS);
    executor.shutdown();
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(updates.get()).isEqualTo(SURVIVORS * UPDATES_PER_RECORD);
    assertThat(deletes.get()).isEqualTo(CHURN_THREADS * CHURN_ROUNDS);

    // The merge must actually have fired on the leader (0 would mean no false conflict ever reached the merge path).
    final long merges = ((DatabaseInternal) getServerDatabase(leaderIndex,
        getDatabaseName())).getPageManager().getStats().txPageSlotMerges;
    assertThat(merges).as("disjoint-slot merge must fire on the leader").isGreaterThan(0);

    // Byte-for-byte replica equality: the property a merged delete could break on its own.
    assertClusterConsistency();

    // PER-REPLICA CHECK: only the survivors are left, each with EXACTLY its owner's last written value.
    final String expected = String.format("%08d", UPDATES_PER_RECORD);
    for (int s = 0; s < getServerCount(); s++) {
      final JSONObject count = executeCommand(s, "sql", "SELECT count(@rid) AS total FROM Doc");
      assertThat(count.getJSONObject("result").getJSONArray("records").getJSONObject(0).getLong("total"))
          .as("server %d must hold only the survivors", s).isEqualTo(SURVIVORS);

      for (int r = 0; r < SURVIVORS; r++) {
        final JSONObject result = executeCommand(s, "sql", "SELECT tag FROM Doc WHERE id = " + r);
        final String tag = result.getJSONObject("result").getJSONArray("records").getJSONObject(0).getString("tag");
        assertThat(tag).as("server %d record %d", s, r).isEqualTo(expected);
      }
    }
  }
}
