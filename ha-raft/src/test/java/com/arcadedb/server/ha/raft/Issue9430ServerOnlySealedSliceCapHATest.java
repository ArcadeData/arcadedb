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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9430: the replicated sealed-store sizing must honour a cap set ONLY in the server configuration.
 * <p>
 * A database's {@code ContextConfiguration} falls back to the JVM-global values, not to the server's, so a cap
 * that lives only in {@code config/server-configuration.json} (here: {@link #onServerConfiguration}, never a
 * global {@code setValue}) used to be invisible to the code that cuts a sealed store into replicated slices. The
 * slice budget was then computed from the 48MB default and the store shipped whole. This pins that the slicing
 * now reads the cap the replication layer is built from: a store larger than one slice travels as a sequence.
 * The companion {@code Issue9430ServerOnlySealedCeilingHATest} covers the compaction guard in the engine.
 */
class Issue9430ServerOnlySealedSliceCapHATest extends BaseRaftHATest {

  /** Just over the per-slice framing, as in {@code Issue4416SlicedSealedStoreIT}, so a modest store needs several slices. */
  private static final long SEALED_ENTRY_CAP = GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES + 256;
  private static final int  SAMPLES          = 200;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Sealed stores use direct file I/O, not page-level replication; equality is asserted in-test instead.
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, SEALED_ENTRY_CAP);
  }

  @Test
  void aSealedSliceCapSetOnlyOnTheServerSlicesTheStore() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());
    assertThat(leaderDb.getConfiguration().getValueAsLong(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE))
        .as("premise: the database's own configuration does not see a server-only setting")
        .isNotEqualTo(SEALED_ENTRY_CAP);
    assertThat(leaderDb.getReplicationConfiguration().getValueAsLong(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE))
        .as("the replication configuration is the server's")
        .isEqualTo(SEALED_ENTRY_CAP);
    assertThat(leaderDb.getWrappedDatabaseInstance().getReplicationConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE))
        .as("and the Raft wrapper the engine reaches answers the same")
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
    awaitAllServers(() -> {
      for (int i = 0; i < getServerCount(); i++)
        assertThat(countSamples(i)).as("sample count on server %d", i).isEqualTo(SAMPLES);
    });

    timeSeriesEngine(leaderIndex).compactAll();

    awaitAllServers(() -> {
      final byte[] leaderSealed = readSealedFile(leaderIndex);
      assertThat(leaderSealed.length).as("the leader must have sealed something").isGreaterThan(0);
      for (int i = 0; i < getServerCount(); i++) {
        assertThat(countSamples(i)).as("sample count on server %d", i).isEqualTo(SAMPLES);
        assertThat(mutableSampleCount(i)).as("mutable bucket on server %d after compaction", i).isZero();
        if (i != leaderIndex)
          assertThat(readSealedFile(i)).as("sealed store on server %d", i).isEqualTo(leaderSealed);
      }
    });

    final long sliceBudget = GlobalConfiguration.replicatedSealedChunkBudget(leaderDb.getReplicationConfiguration());
    assertThat((long) readSealedFile(leaderIndex).length)
        .as("the sealed store must be larger than one slice under the server cap, or nothing had to be sliced")
        .isGreaterThan(sliceBudget);
    assertThat(((RaftReplicatedDatabase) leaderDb.getWrappedDatabaseInstance()).getSealedStoreChunksShipped())
        .as("the store must travel as a SEQUENCE of slices sized by the server-only cap, not whole")
        .isGreaterThan(1L);
  }

  private static void awaitAllServers(final ThrowingRunnable check) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(check);
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

  private long mutableSampleCount(final int serverIndex) throws IOException {
    final TimeSeriesEngine engine = timeSeriesEngine(serverIndex);
    long total = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      total += engine.getShard(s).getMutableBucket().getSampleCount();
    return total;
  }

  private byte[] readSealedFile(final int serverIndex) throws IOException {
    final File sealed = new File(getDatabasePath(serverIndex), "weather_shard_0.ts.sealed");
    return sealed.exists() ? Files.readAllBytes(sealed.toPath()) : new byte[0];
  }
}
