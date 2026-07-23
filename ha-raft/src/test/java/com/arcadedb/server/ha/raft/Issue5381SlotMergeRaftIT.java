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
 * HA replication regression for the disjoint-slot merge (#5381). The whole fix rests on one property: a page rebased
 * on the LEADER at commit ships its post-merge image in the WAL buffer, and followers replay that image verbatim in
 * Raft log order, so no replica can diverge. Every other test for this fix runs embedded/single-JVM; this one locks
 * the property in against a real 3-node cluster.
 * <p>
 * The workload mirrors the engine-level
 * {@code Issue5381FalseConflictTest#concurrentInPlaceUpdatesOfDistinctCoLocatedRecords}: several records packed onto
 * a single bucket page (BUCKETS 1), one owner thread per record hammering same-width in-place updates - the same
 * shape as #5381's fixed-width RID->RID head-pointer flip. Because owners are disjoint, every page-version conflict
 * is provably false, and with 3-phase commit's lock-free replication window those conflicts really do occur on the
 * leader. The merge must fire there (asserted via the leader's slotMerges counter). Afterwards every node must hold,
 * for every record, EXACTLY the last value written - checked per-replica by value and byte-for-byte by
 * {@link #assertClusterConsistency()}: a merged page that replayed wrong on a follower would break both.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5381SlotMergeRaftIT extends BaseRaftHATest {
  private static final int RECORDS            = 12;
  private static final int UPDATES_PER_RECORD = 60;

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
  void distinctSlotUpdatesMergeAndReplicateIntact() throws Exception {
    final int leaderIndex = 0;

    // Single bucket: every Doc lands on the same page(s), so in-place updates to DIFFERENT records collide falsely
    // at page granularity - the exact #5381 shape. Fixed-width tag => every update is a same-size overwrite (the
    // merge deliberately does not cover record growth).
    executeCommand(leaderIndex, "sql", "CREATE document TYPE Doc BUCKETS 1");
    executeCommand(leaderIndex, "sql", "CREATE PROPERTY Doc.id INTEGER");
    executeCommand(leaderIndex, "sql", "CREATE PROPERTY Doc.tag STRING");
    for (int r = 0; r < RECORDS; r++)
      executeCommand(leaderIndex, "sql", "INSERT INTO Doc SET id = " + r + ", tag = '" + String.format("%08d", 0) + "'");
    waitForReplicationIsCompleted(leaderIndex);

    final AtomicInteger successCount = new AtomicInteger();
    final ExecutorService executor = Executors.newFixedThreadPool(RECORDS);
    final List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < RECORDS; t++) {
      final int recordId = t;
      futures.add(executor.submit(() -> {
        for (int i = 1; i <= UPDATES_PER_RECORD; i++) {
          try {
            // "commit retry" keeps the workload robust under HA hiccups (leader step-down etc.); the disjoint-slot
            // merge is what actually absorbs the false page conflicts, proven below by the leader's merge counter.
            final JSONObject response = executeCommand(leaderIndex, "sqlscript",
                "BEGIN;UPDATE Doc SET tag = '" + String.format("%08d", i) + "' WHERE id = " + recordId + ";commit retry 100;");
            assertThat(response).withFailMessage("Update returned null: record=%d seq=%d", recordId, i).isNotNull();
            successCount.incrementAndGet();
          } catch (final Exception e) {
            fail("Update failed: record=" + recordId + " seq=" + i + " error=" + e.getMessage());
          }
        }
      }));
    }

    for (final Future<?> f : futures)
      f.get(120, TimeUnit.SECONDS);
    executor.shutdown();
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(successCount.get()).isEqualTo(RECORDS * UPDATES_PER_RECORD);

    // The merge must actually have fired on the leader (0 would mean no false conflict ever hit the merge path).
    final long merges = ((DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName())).getPageManager().getStats().slotMerges;
    assertThat(merges).as("disjoint-slot merge must fire on the leader").isGreaterThan(0);

    // Byte-for-byte replica equality (the property the whole fix rests on).
    assertClusterConsistency();

    // PER-REPLICA VALUE CHECK: every record holds EXACTLY its owner's last written value on EVERY node. A merged
    // page that replayed wrong on a follower (lost/torn/cross-record byte) would fail here.
    final String expected = String.format("%08d", UPDATES_PER_RECORD);
    for (int s = 0; s < getServerCount(); s++)
      for (int r = 0; r < RECORDS; r++) {
        final JSONObject result = executeCommand(s, "sql", "SELECT tag FROM Doc WHERE id = " + r);
        final String tag = result.getJSONObject("result").getJSONArray("records").getJSONObject(0).getString("tag");
        assertThat(tag).as("server %d record %d", s, r).isEqualTo(expected);
      }
  }
}
