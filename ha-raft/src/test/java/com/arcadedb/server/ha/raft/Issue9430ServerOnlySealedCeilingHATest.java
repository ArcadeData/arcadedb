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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9430: {@code TimeSeriesShard}'s HA compaction guard must measure the projected sealed store against the
 * replicated ceiling derived from the cap the replication layer enforces - the server's configuration - and not
 * against the database's own configuration, which falls back to the JVM-global values and never sees a cap set
 * only in {@code config/server-configuration.json}.
 * <p>
 * The cap is set below the per-slice framing, so no slice can be built and the ceiling is a single entry of that
 * size. Any mutable page already projects above it, so the guard must skip the compaction. Before the fix the
 * engine read the 48MB default, found the store far under its ~2GB ceiling and compacted it.
 */
class Issue9430ServerOnlySealedCeilingHATest extends BaseRaftHATest {

  private static final long SEALED_ENTRY_CAP = 1024;
  private static final int  SAMPLES          = 20;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Sealed stores use direct file I/O, not page-level replication.
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, SEALED_ENTRY_CAP);
  }

  @Test
  void aSealedCeilingSetOnlyOnTheServerSkipsTheCompaction() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());
    assertThat(GlobalConfiguration.maxReplicatedSealedStoreSize(leaderDb.getReplicationConfiguration()))
        .as("premise: under the server cap the ceiling is one entry of that size")
        .isEqualTo(SEALED_ENTRY_CAP);

    executeCommand(leaderIndex, "sql",
        "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 1");
    waitForReplicationIsCompleted(leaderIndex);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    leader.transaction(() -> {
      for (int i = 0; i < SAMPLES; i++)
        leader.command("sql", "INSERT INTO weather SET ts = ?, location = ?, temperature = ?", 1_000 + i, "loc-" + (i % 5),
            20.0 + i);
    });

    final TimeSeriesEngine engine = timeSeriesEngine(leaderIndex);
    final long sealedBefore = sealedFileLength(leaderIndex);
    engine.compactAll();

    long mutable = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      mutable += engine.getShard(s).getMutableBucket().getSampleCount();
    assertThat(mutable).as("the guard must have skipped the compaction: every sample still in the mutable bucket")
        .isEqualTo(SAMPLES);
    assertThat(sealedFileLength(leaderIndex)).as("and nothing sealed on the leader").isEqualTo(sealedBefore);
  }

  private TimeSeriesEngine timeSeriesEngine(final int serverIndex) {
    final DatabaseInternal db = (DatabaseInternal) getServer(serverIndex).getDatabase(getDatabaseName());
    return ((LocalTimeSeriesType) db.getSchema().getType("weather")).getEngine();
  }

  /**
   * The sealed file is created with its header together with the type, before anything seals, so the test compares
   * its length before and after the compaction instead of expecting zero. A missing file counts as zero bytes so the
   * comparison still holds if a future layout creates it lazily.
   */
  private long sealedFileLength(final int serverIndex) throws IOException {
    final File sealed = new File(getDatabasePath(serverIndex), "weather_shard_0.ts.sealed");
    return sealed.exists() ? sealed.length() : 0L;
  }
}
