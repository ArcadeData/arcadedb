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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the HA safety valve for oversized TimeSeries sealed stores (issue #4382): when the
 * rewritten sealed store would be too large to ship inline in a single Raft entry, the leader skips
 * compaction entirely rather than producing an un-shippable entry. The samples then stay in the
 * fully replicated mutable bucket, so every node still has all the data and no divergence occurs -
 * the cluster simply does not seal until the cap is raised.
 *
 * <p>The {@code arcadedb.ha.tsMaxSealedInlineSize} cap is set to an impossibly small value so any
 * non-empty shard trips the guard.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftTimeSeriesOversizedSealedIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Sealed stores use direct file I/O, not page-level replication; logical equality is asserted in-test.
  }

  @Test
  @Tag("slow")
  void oversizedSealedStoreSkipsCompactionAndKeepsDataInMutable() throws Exception {
    // 10 bytes is smaller than even an empty sealed header (27 bytes), so any shard with data trips the
    // oversized guard. The cap is read live from the global configuration at compaction time, so setting
    // it here (before the maintenance scheduler's first run) is enough; restore it for other test classes.
    final Object previousCap = GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE.getValue();
    GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE.setValue(10L);
    try {
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> findLeaderIndex() >= 0);
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);

      executeCommand(leaderIndex,
          "sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 1");
      waitForReplicationIsCompleted(leaderIndex);

      final int samples = 30;
      for (int i = 0; i < samples; i++)
        executeCommand(leaderIndex, "sql",
            "INSERT INTO weather SET ts = " + (1000 + i) + ", location = 'us-east', temperature = " + (20.0 + i));

      awaitAllServersReportSamples(samples);

      // Force compaction on the leader: it must be a no-op because the projected sealed size exceeds the cap.
      timeSeriesEngine(findLeaderIndex()).compactAll();
      waitForAllServers();

      // Data is intact and identical on every node, the sealed store stayed empty, and the data is still
      // in the (replicated) mutable bucket - i.e. compaction was skipped, not silently dropped.
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        for (int i = 0; i < getServerCount(); i++) {
          assertThat(countSamples(i)).as("sample count on server %d", i).isEqualTo((long) samples);
          final TimeSeriesEngine e = timeSeriesEngine(i);
          assertThat(e.getShard(0).getSealedStore().getBlockCount()).as("sealed blocks on server %d", i).isZero();
          assertThat(e.getShard(0).getMutableBucket().getSampleCount()).as("mutable samples on server %d", i)
              .isEqualTo((long) samples);
        }
      });
    } finally {
      GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE.setValue(previousCap);
    }
  }

  private void awaitAllServersReportSamples(final long expected) {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(countSamples(i)).as("sample count on server %d", i).isEqualTo(expected);
    });
  }

  private long countSamples(final int serverIndex) {
    final Database db = getServer(serverIndex).getDatabase(getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM weather")) {
      return rs.hasNext() ? rs.next().<Number>getProperty("cnt").longValue() : 0L;
    }
  }

  private TimeSeriesEngine timeSeriesEngine(final int serverIndex) {
    final DatabaseInternal db = (DatabaseInternal) getServer(serverIndex).getDatabase(getDatabaseName());
    return ((LocalTimeSeriesType) db.getSchema().getType("weather")).getEngine();
  }
}
