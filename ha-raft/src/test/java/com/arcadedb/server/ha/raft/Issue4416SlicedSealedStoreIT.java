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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4416, end to end: a TimeSeries shard whose sealed store does not fit one Raft entry must keep sealing.
 * <p>
 * Before this, {@code TimeSeriesShard.compactInternal} skipped such a shard outright and logged a warning. That
 * kept the cluster correct - the samples stay in the fully replicated mutable bucket - but the skip was
 * PERMANENT: the store it refused to ship only grows, so the projected size never came back under the cap and the
 * shard never sealed again, never compressed, never retained, never downsampled. This test pins the behaviour
 * that replaces it: the store is sliced across an ordered sequence of entries, and every node ends up with the
 * leader's sealed file and an empty mutable bucket.
 * <p>
 * The cap is set just above the per-slice framing so a modest sealed store needs several slices. That is the
 * whole point of driving it from configuration rather than from volume: the mechanism under test is the sequence,
 * not the megabytes, and a test that had to write a real 48MB store on three in-process nodes would trade the
 * evidence for runtime.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4416SlicedSealedStoreIT extends BaseRaftHATest {

  /**
   * Just enough over the per-slice framing that a slice carries a useful payload, and little enough that a modest
   * sealed store still needs several. It also sets the ceiling this shard must stay under - 512 slices of the
   * budget below, comfortably above anything {@link #SAMPLES} can seal - because a store OVER the ceiling is
   * still skipped, and a test that quietly crossed it would be asserting the old behaviour.
   */
  private static final long SEALED_ENTRY_CAP = GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES + 256;
  private static final int  SAMPLES          = 200;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Sealed stores use direct file I/O, not page-level replication; equality is asserted in-test instead.
  }

  @Test
  @Tag("slow")
  void aSealedStoreTooLargeForOneEntryIsStillSealedAndReplicated() throws Exception {
    // The cap is read live from the configuration at compaction time, and a database's ContextConfiguration falls
    // back to the global value for anything it does not override - so setting it here, before the first
    // compaction, reaches every node in this JVM. Restored for the other test classes, exactly as
    // RaftTimeSeriesOversizedSealedIT does it.
    final Object previousCap = GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE.getValue();
    GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE.setValue(SEALED_ENTRY_CAP);
    try {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);

      executeCommand(leaderIndex, "sql",
          "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 1");
      waitForReplicationIsCompleted(leaderIndex);

      insertSamples(0, SAMPLES);
      awaitAllServersReportSamples(SAMPLES);

      timeSeriesEngine(findLeaderIndex()).compactAll();

      // The regression itself: compaction actually RAN. Under the old cap this shard would have been skipped, the
      // mutable bucket would still hold every sample, and the sealed store would still be empty.
      awaitCompactionLanded(SAMPLES);

      final int leaderAfter = findLeaderIndex();
      assertThat((long) readSealedFile(leaderAfter).length)
          .as("the sealed store must be larger than one slice may carry, or nothing was sliced")
          .isGreaterThan(sliceBudget());
      assertThat((long) readSealedFile(leaderAfter).length)
          .as("and it must stay under the ceiling, or the shard was skipped and this test proves the OLD behaviour")
          .isLessThan(sealedStoreCeiling());
      assertThat(totalSealedSlicesShipped())
          .as("the store must have travelled as a SEQUENCE of slices, not inline: that is the path under test")
          .isGreaterThan(1L);

      // A second cycle over a store that is already large: the sequence has to work when the follower's staging
      // file already existed and the leader's image is a rewrite rather than a first seal.
      insertSamples(SAMPLES, SAMPLES * 2);
      awaitAllServersReportSamples(SAMPLES * 2);
      timeSeriesEngine(findLeaderIndex()).compactAll();
      awaitCompactionLanded(SAMPLES * 2);
    } finally {
      GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE.setValue(previousCap);
    }
  }

  /**
   * Every node holds the leader's sealed file, no node still holds the samples in its mutable bucket, and every
   * node answers the same count. Asserted together because a partial truth here is the failure mode: a follower
   * that applied the clear WAL without installing the sealed slices would report a LOWER count, and one that
   * installed without clearing would report a higher one.
   */
  private void awaitCompactionLanded(final long expectedSamples) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      final int leader = findLeaderIndex();
      final byte[] leaderSealed = readSealedFile(leader);
      assertThat(leaderSealed.length).as("the leader must have sealed something").isGreaterThan(0);

      for (int i = 0; i < getServerCount(); i++) {
        assertThat(countSamples(i)).as("sample count on server %d", i).isEqualTo(expectedSamples);
        assertThat(mutableSampleCount(i)).as("mutable bucket on server %d after compaction", i).isZero();
        if (i != leader)
          assertThat(readSealedFile(i)).as("sealed store on server %d must match leader %d", i, leader)
              .isEqualTo(leaderSealed);
      }
    });
  }

  /** What one slice may carry under {@link #SEALED_ENTRY_CAP}, asked of the production arithmetic itself. */
  private static long sliceBudget() {
    return GlobalConfiguration.replicatedSealedChunkBudget(cappedConfiguration());
  }

  /** The largest sealed store this configuration will replicate at all; above it the shard is still skipped. */
  private static long sealedStoreCeiling() {
    return GlobalConfiguration.maxReplicatedSealedStoreSize(cappedConfiguration());
  }

  private static ContextConfiguration cappedConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, SEALED_ENTRY_CAP);
    return configuration;
  }

  /** Only the node that compacted ships slices, and an election may have moved it; sum rather than guess. */
  private long totalSealedSlicesShipped() {
    long total = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal wrapped =
          ((DatabaseInternal) getServerDatabase(i, getDatabaseName())).getWrappedDatabaseInstance();
      if (wrapped instanceof RaftReplicatedDatabase raft)
        total += raft.getSealedStoreChunksShipped();
    }
    return total;
  }

  private void insertSamples(final int fromInclusive, final int toExclusive) throws Exception {
    for (int i = fromInclusive; i < toExclusive; i++)
      executeCommand(findLeaderIndex(), "sql",
          "INSERT INTO weather SET ts = " + (1_000 + i) + ", location = 'loc-" + (i % 5) + "', temperature = "
              + (20.0 + i));
  }

  private void awaitAllServersReportSamples(final long expected) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
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
