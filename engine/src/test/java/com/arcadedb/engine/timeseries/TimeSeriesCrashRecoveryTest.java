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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests crash recovery for compaction: simulates an interrupted compaction by leaving the
 * compaction-in-progress flag set, then reopens the shard and verifies no duplicate samples.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesCrashRecoveryTest extends TestHelper {

  private List<ColumnDefinition> createTestColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @Test
  void recoveryAfterInterruptedCompaction() throws Exception {
    final List<ColumnDefinition> columns = createTestColumns();

    // Phase 1: Create shard, insert data, compact normally, then insert more data
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_crash_recovery", 0, columns);
    shard.appendSamples(
        new long[] { 1000L, 2000L, 3000L },
        new Object[] { "A", "B", "A" },
        new Object[] { 10.0, 20.0, 30.0 }
    );
    database.commit();

    // Compact successfully first
    shard.compact();

    final long watermarkAfterFirstCompact = shard.getSealedStore().getBlockCount();
    assertThat(watermarkAfterFirstCompact).isGreaterThan(0);

    // Insert more data (these remain in the mutable bucket)
    database.begin();
    shard.appendSamples(
        new long[] { 4000L, 5000L },
        new Object[] { "C", "D" },
        new Object[] { 40.0, 50.0 }
    );
    database.commit();

    // Phase 2: Simulate a crash mid-compaction by manually setting the flag
    // This mimics: compaction started (flag set, watermark saved), some sealed blocks
    // were written, but mutable pages were NOT cleared before crash.
    database.begin();
    shard.getMutableBucket().setCompactionInProgress(true);
    shard.getMutableBucket().setCompactionWatermark(watermarkAfterFirstCompact);
    database.commit();

    // Also simulate partial sealed writes by compacting (writes sealed blocks),
    // but the flag stays set because we set it above after the compact's flag clearing
    // For a simpler test: just verify that reopening with the flag set does recovery

    shard.close();

    // Phase 3: Reopen the shard — constructor should detect the flag and recover
    database.begin();
    final TimeSeriesShard recoveredShard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_crash_recovery", 0, columns);

    // The compaction flag should have been cleared by recovery
    assertThat(recoveredShard.getMutableBucket().isCompactionInProgress()).isFalse();

    // Sealed store should have been truncated to the watermark
    assertThat(recoveredShard.getSealedStore().getBlockCount()).isEqualTo(watermarkAfterFirstCompact);

    // Query all data — should have no duplicates
    final List<Object[]> results = recoveredShard.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();

    // Original 3 samples (sealed) + 2 new samples (mutable) = 5 total, no duplicates
    assertThat(results).hasSize(5);

    // Verify specific values are all present
    final double[] values = results.stream().mapToDouble(r -> (double) r[2]).sorted().toArray();
    assertThat(values).containsExactly(10.0, 20.0, 30.0, 40.0, 50.0);

    recoveredShard.close();
  }

  @Test
  void recoveryWithCleanState() throws Exception {
    final List<ColumnDefinition> columns = createTestColumns();

    // Create, insert, compact normally
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_clean_recovery", 0, columns);
    shard.appendSamples(
        new long[] { 1000L, 2000L },
        new Object[] { "A", "B" },
        new Object[] { 10.0, 20.0 }
    );
    database.commit();

    shard.compact();
    shard.close();

    // Reopen without any crash flag — should work normally
    database.begin();
    final TimeSeriesShard recovered = new TimeSeriesShard(
        (DatabaseInternal) database, "test_clean_recovery", 0, columns);

    assertThat(recovered.getMutableBucket().isCompactionInProgress()).isFalse();

    final List<Object[]> results = recovered.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();
    assertThat(results).hasSize(2);

    recovered.close();
  }

  /**
   * Simulates a crash during file swap where the original .sealed file was replaced by ATOMIC_MOVE
   * but a stale .tmp file remains on disk (e.g., from a prior interrupted maintenance task).
   * Verifies that the stale .tmp file is cleaned up on startup and the shard opens normally.
   */
  @Test
  void recoveryWithStaleTmpFile() throws Exception {
    final List<ColumnDefinition> columns = createTestColumns();

    // Phase 1: Create shard, insert data, compact to create a valid .sealed file
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_tmp_cleanup", 0, columns);
    shard.appendSamples(
        new long[] { 1000L, 2000L, 3000L },
        new Object[] { "A", "B", "A" },
        new Object[] { 10.0, 20.0, 30.0 }
    );
    database.commit();
    shard.compact();

    final long blockCount = shard.getSealedStore().getBlockCount();
    assertThat(blockCount).isGreaterThan(0);
    shard.close();

    // Phase 2: Create a stale .tmp file that simulates a leftover from an interrupted operation
    final String shardPath = database.getDatabasePath() + "/test_tmp_cleanup_shard_0";
    final File tmpFile = new File(shardPath + ".ts.sealed.tmp");
    try (final RandomAccessFile tmp = new RandomAccessFile(tmpFile, "rw")) {
      // Write some garbage data to simulate a partial temp file
      tmp.write(new byte[] { 0x01, 0x02, 0x03, 0x04 });
    }
    assertThat(tmpFile.exists()).isTrue();

    // Phase 3: Reopen shard — constructor should clean up the stale .tmp file
    database.begin();
    final TimeSeriesShard recovered = new TimeSeriesShard(
        (DatabaseInternal) database, "test_tmp_cleanup", 0, columns);

    // Verify .tmp file was cleaned up
    assertThat(tmpFile.exists()).isFalse();

    // Verify sealed store is intact with the original blocks
    assertThat(recovered.getSealedStore().getBlockCount()).isEqualTo(blockCount);

    // Verify data is correct
    final List<Object[]> results = recovered.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();
    assertThat(results).hasSize(3);

    final double[] values = results.stream().mapToDouble(r -> (double) r[2]).sorted().toArray();
    assertThat(values).containsExactly(10.0, 20.0, 30.0);

    recovered.close();
  }

  /**
   * Simulates the most critical failure scenario for the old non-atomic file swap: the original
   * .sealed file has been deleted but the .tmp file was never renamed (e.g., ENOSPC or OS crash
   * between delete() and renameTo()). With ATOMIC_MOVE this path is no longer possible, but this
   * test verifies that even in this degenerate state the sealed store recovers: the .tmp is cleaned
   * up and a fresh empty sealed store is created.
   */
  @Test
  void recoveryWithMissingSealedAndOrphanedTmp() throws Exception {
    final List<ColumnDefinition> columns = createTestColumns();

    // Phase 1: Create shard, insert data, compact to produce sealed blocks
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_orphan_tmp", 0, columns);
    shard.appendSamples(
        new long[] { 1000L, 2000L },
        new Object[] { "A", "B" },
        new Object[] { 10.0, 20.0 }
    );
    database.commit();
    shard.compact();
    shard.close();

    // Phase 2: Simulate the old non-atomic failure: delete .sealed, leave .tmp
    final String shardPath = database.getDatabasePath() + "/test_orphan_tmp_shard_0";
    final File sealedFile = new File(shardPath + ".ts.sealed");
    final File tmpFile = new File(shardPath + ".ts.sealed.tmp");

    // Copy the current sealed file to .tmp (simulating a temp file that was ready to be renamed)
    Files.copy(sealedFile.toPath(), tmpFile.toPath());
    // Delete the original sealed file (simulating delete() succeeded but renameTo() failed)
    assertThat(sealedFile.delete()).isTrue();
    assertThat(sealedFile.exists()).isFalse();
    assertThat(tmpFile.exists()).isTrue();

    // Phase 3: Reopen — sealed store constructor should:
    // 1) Delete the stale .tmp file
    // 2) Create a fresh empty .sealed file
    database.begin();
    final TimeSeriesShard recovered = new TimeSeriesShard(
        (DatabaseInternal) database, "test_orphan_tmp", 0, columns);

    // The .tmp should be cleaned up
    assertThat(tmpFile.exists()).isFalse();
    // The .sealed should exist again (freshly created, empty)
    assertThat(sealedFile.exists()).isTrue();

    // Sealed store is empty (the old data was lost in the simulated crash), but the mutable
    // bucket still has data if not cleared. Since we compacted and closed, the mutable bucket
    // was cleared during compaction, so we expect 0 results from the sealed store.
    assertThat(recovered.getSealedStore().getBlockCount()).isEqualTo(0);

    final List<Object[]> results = recovered.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();

    // In this catastrophic failure scenario (which ATOMIC_MOVE now prevents), the sealed data
    // is lost. The mutable bucket data from before compaction was already consumed by compact().
    // This test primarily verifies the system doesn't crash and can resume accepting writes.
    assertThat(results).isNotNull();

    // Verify the shard can accept new writes after recovery
    database.begin();
    recovered.appendSamples(
        new long[] { 5000L },
        new Object[] { "X" },
        new Object[] { 99.0 }
    );
    database.commit();

    database.begin();
    final List<Object[]> newResults = recovered.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();
    assertThat(newResults.stream().anyMatch(r -> (double) r[2] == 99.0)).isTrue();

    recovered.close();
  }

  /**
   * Regression test: verifies that extra sealed blocks written during an interrupted compaction
   * are truncated back to the watermark on restart.
   * <p>
   * Previously the test only set the flag on an already-empty window, so the truncation was a
   * no-op and the actual block-removal path was never exercised.
   */
  @Test
  void recoveryTruncatesExtraSealedBlocks() throws Exception {
    final List<ColumnDefinition> columns = createTestColumns();

    // Step 1: Insert 3 samples and compact → 1 sealed block
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_truncate_extra", 0, columns);
    shard.appendSamples(
        new long[] { 1000L, 2000L, 3000L },
        new Object[] { "A", "B", "A" },
        new Object[] { 10.0, 20.0, 30.0 }
    );
    database.commit();

    shard.compact();
    final long watermark = shard.getSealedStore().getBlockCount();
    assertThat(watermark).isGreaterThan(0);

    // Step 2: Insert more data into the mutable bucket (will be retained after recovery)
    database.begin();
    shard.appendSamples(
        new long[] { 4000L, 5000L },
        new Object[] { "C", "D" },
        new Object[] { 40.0, 50.0 }
    );
    database.commit();

    // Step 3: Simulate partial compaction — write extra blocks directly to the sealed store,
    // mimicking the state after compact() wrote blocks but crashed before clearing the mutable bucket.
    final double[] nanStats = new double[columns.size()];
    Arrays.fill(nanStats, Double.NaN);
    final double[] mins = new double[columns.size()];
    final double[] maxs = new double[columns.size()];
    final double[] sums = new double[columns.size()];
    Arrays.fill(mins, Double.NaN);
    Arrays.fill(maxs, Double.NaN);
    final long[] extraTs = { 4000L, 5000L };
    final byte[][] compressedExtra = new byte[columns.size()][];
    compressedExtra[0] = DeltaOfDeltaCodec.encode(extraTs);             // ts column
    compressedExtra[1] = DictionaryCodec.encode(new String[]{"C","D"}); // tag column
    final double[] extraVals = { 40.0, 50.0 };
    compressedExtra[2] = GorillaXORCodec.encode(extraVals);             // field column
    mins[2] = 40.0;
    maxs[2] = 50.0;
    sums[2] = 90.0;
    shard.getSealedStore().appendBlock(2, 4000L, 5000L, compressedExtra, mins, maxs, sums, new long[] { 0, 0, 2 }, null);
    shard.getSealedStore().flushHeader();

    final long blocksWithExtra = shard.getSealedStore().getBlockCount();
    assertThat(blocksWithExtra).isGreaterThan(watermark); // extra blocks are present

    // Step 4: Set compaction-in-progress flag with the watermark pointing to BEFORE the extra blocks
    database.begin();
    shard.getMutableBucket().setCompactionInProgress(true);
    shard.getMutableBucket().setCompactionWatermark(watermark);
    database.commit();

    shard.close();

    // Step 5: Reopen — crash recovery must truncate the extra blocks and clear the flag
    database.begin();
    final TimeSeriesShard recovered = new TimeSeriesShard(
        (DatabaseInternal) database, "test_truncate_extra", 0, columns);

    assertThat(recovered.getMutableBucket().isCompactionInProgress()).isFalse();
    // Extra blocks should have been removed; sealed store must be back to the watermark
    assertThat(recovered.getSealedStore().getBlockCount()).isEqualTo(watermark);

    // Mutable bucket still has the 2 samples that were not part of the partial compaction
    final List<Object[]> results = recovered.scanRange(0, Long.MAX_VALUE, null, null);
    database.commit();

    // 3 sealed (original) + 2 mutable = 5 unique samples, no duplicates from the extra blocks
    assertThat(results).hasSize(5);
    final double[] values = results.stream().mapToDouble(r -> (double) r[2]).sorted().toArray();
    assertThat(values).containsExactly(10.0, 20.0, 30.0, 40.0, 50.0);

    recovered.close();
  }

  // #9452
  // Issue #9452: rows of acknowledged commits went missing after a kill while {@code COMPACT TIMESERIES TYPE} ran.
  // <p>
  // The database opens by loading the schema and only then replaying the WAL. The time series engine was built inside
  // the schema load, so everything it read from its pages it read BEFORE the replay, from whatever the dead process had
  // flushed:
  // <ul>
  // <li>the shard's crash repair read the "compaction in progress" flag of a header page that was older than the WAL.
  // A compaction whose last step (swap the sealed file, clear the mutable pages, reset the flag) was committed but not
  // yet flushed looked interrupted, so the repair truncated the sealed store back to the watermark - throwing away the
  // blocks that now held the rows - and then the replay applied the clear of the mutable pages. Both copies of the
  // rows were gone, while every document of the same transactions came back;</li>
  // <li>the tag dictionary built its in-RAM map from pages older than the WAL, so a value interned before the kill was
  // interned again under a second id, and a filter on that value found only the rows of one of the two.</li>
  // </ul>
  // Both states are produced deterministically, as {@code UnflushedDictionaryRecoveryTest} does: the file is put back
  // to the image it had before the unflushed commit, which is what a kill in that window leaves on disk.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final String WAL_REPLAY_TYPE_NAME = "Issue9452Ts";
  private static final int    WAL_REPLAY_ROWS      = 200;

  @AfterEach
  void clearHook() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
  }

  // #9452
  @Test
  void aCompactionCommittedButNotFlushedKeepsItsRowsAfterAKill() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE " + WAL_REPLAY_TYPE_NAME + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    for (int i = 0; i < WAL_REPLAY_ROWS; i++) {
      final long ts = 1_700_000_000_000L + i;
      final double v = i;
      database.transaction(() -> database.command("sql", "INSERT INTO " + WAL_REPLAY_TYPE_NAME + " SET ts = ?, host = ?, v = ?", ts, "h0", v));
    }
    assertThat(countRows()).isEqualTo(WAL_REPLAY_ROWS);

    final File bucketFile = componentFile(WAL_REPLAY_TYPE_NAME + "_shard_0.", "." + TimeSeriesBucket.BUCKET_EXT);

    // Right before the final step of the compaction: Phase 0 (flag set, watermark 0) is committed, and it is flushed
    // here so the file image holds it. The final step commits after this point and is left in the WAL only.
    final AtomicReference<byte[]> imageBeforeTheLastStep = new AtomicReference<>();
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
      PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database);
      try {
        imageBeforeTheLastStep.set(Files.readAllBytes(bucketFile.toPath()));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    };
    engine().compactAll();
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;

    assertThat(imageBeforeTheLastStep.get()).as("the compaction must have reached its last step").isNotNull();
    assertThat(engine().getShard(0).getSealedStore().getBlockCount()).as("the rows must have been sealed").isGreaterThan(0);
    assertThat(countRows()).isEqualTo(WAL_REPLAY_ROWS);

    killAndReopen(() -> Files.write(bucketFile.toPath(), imageBeforeTheLastStep.get()));

    assertThat(countRows()).as("every acknowledged row must survive the kill").isEqualTo(WAL_REPLAY_ROWS);
    assertThat(engine().getShard(0).getMutableBucket().isCompactionInProgress()).isFalse();
    assertThat(corruptWalFiles()).isEmpty();
  }

  // #9452
  @Test
  void aTagInternedButNotFlushedIsNotInternedTwiceAfterAKill() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE " + WAL_REPLAY_TYPE_NAME + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> database.command("sql", "INSERT INTO " + WAL_REPLAY_TYPE_NAME + " SET ts = ?, host = ?, v = ?", 1L, "a", 1.0));

    // A clean close flushes everything: this image is the dictionary before "b" was interned
    reopenDatabase();
    final File dictionaryFile = componentFile(WAL_REPLAY_TYPE_NAME + TimeSeriesTagDictionary.NAME_SUFFIX + ".",
        "." + TimeSeriesTagDictionary.DICT_EXT);
    final byte[] dictionaryBeforeB = Files.readAllBytes(dictionaryFile.toPath());

    database.transaction(() -> database.command("sql", "INSERT INTO " + WAL_REPLAY_TYPE_NAME + " SET ts = ?, host = ?, v = ?", 2L, "b", 2.0));

    killAndReopen(() -> Files.write(dictionaryFile.toPath(), dictionaryBeforeB));

    database.transaction(() -> database.command("sql", "INSERT INTO " + WAL_REPLAY_TYPE_NAME + " SET ts = ?, host = ?, v = ?", 3L, "b", 3.0));

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + WAL_REPLAY_TYPE_NAME + " WHERE host = 'b'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("both rows tagged 'b' must match the filter").isEqualTo(2L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + WAL_REPLAY_TYPE_NAME)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(3L);
    }
  }

  private void killAndReopen(final FileAction whileDown) throws Exception {
    ((DatabaseInternal) database).kill();
    database.close();
    assertThat(walFiles()).as("the killed database must leave WAL files to replay").isNotEmpty();
    whileDown.run();
    database = factory.open();
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType(WAL_REPLAY_TYPE_NAME)).getEngine();
  }

  private long countRows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + WAL_REPLAY_TYPE_NAME)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private File componentFile(final String prefix, final String suffix) {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.startsWith(prefix) && name.endsWith(suffix));
    assertThat(files).as("component file %s*%s", prefix, suffix).isNotNull().hasSize(1);
    return files[0];
  }

  private File[] walFiles() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
    return files == null ? new File[0] : files;
  }

  private File[] corruptWalFiles() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal.corrupt"));
    return files == null ? new File[0] : files;
  }

  @FunctionalInterface
  private interface FileAction {
    void run() throws IOException;
  }

  // #6948
  // Issue #6948: {@code runMaintenance}'s leader-only skip has to read {@code isReplicated()}/{@code isLeader()} off
  // the CURRENT wrapper, not off the reference the task was scheduled with.
  // <p>
  // Every {@code schedule()} call site hands over the instance {@code LocalSchema} was built with, and that field is
  // {@code final}, captured inside {@code LocalDatabase.openInternal()} - which runs before the server wraps the
  // database for HA, and is never updated afterwards. So the scheduled reference is the raw {@code LocalDatabase},
  // whose {@code isReplicated()} is the {@code DatabaseInternal} default {@code false}, and the skip could not fire
  // for anybody. The cost was wasted work rather than divergence, because the three operations underneath already
  // resolve the wrapper per call to reach {@code runWithCompactionReplication} - but the skip exists precisely to
  // avoid that wasted work, and it was doing nothing.
  // <p>
  // The wrapper here is installed exactly the way the HA server installs {@code RaftReplicatedDatabase}: through
  // {@code LocalDatabase.setWrappedDatabaseInstance()}, with the wrapper answering itself for
  // {@code getWrappedDatabaseInstance()} and delegating everything else to the real database. The two flags are the
  // only behaviour it invents, because they are the only ones the engine module cannot get from a real Raft node.
  //
  // @author Roberto Franchini (r.franchini@arcadedata.com)

  private static final String WRAPPER_TYPE_NAME = "Reading";

  /**
   * Runs BEFORE {@code TestHelper.afterTest()} (JUnit runs a subclass's {@code @AfterEach} first), which matters:
   * the harness's closing integrity check reports a warning for a database that says it is replicated, and this
   * test's whole point is a database that says exactly that.
   */
  @AfterEach
  void removeWrapper() {
    if (database != null && database.isOpen())
      ((LocalDatabase) database).setWrappedDatabaseInstance((LocalDatabase) database);
  }

  // #6948
  /**
   * A follower does no maintenance at all. Observed through compaction: the mutable rows are still in the mutable
   * bucket afterwards, because nothing sealed them.
   */
  @Test
  void aFollowerSkipsMaintenanceEvenThoughItWasScheduledWithTheRawInstance() throws Exception {
    final LocalTimeSeriesType tsType = createTypeWithMutableRows();
    installWrapper(true, false);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, WRAPPER_TYPE_NAME);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("a follower must not compact: the leader ships the sealed store").isZero();
  }

  // #6948
  /**
   * The arm that proves the one above is not passing because maintenance is broken outright: the same call on a
   * leader does compact. Also covers the standalone case indirectly - {@code isReplicated() == false} takes the
   * same branch as a leader.
   */
  @Test
  void aLeaderStillRunsMaintenance() throws Exception {
    final LocalTimeSeriesType tsType = createTypeWithMutableRows();
    installWrapper(true, true);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, WRAPPER_TYPE_NAME);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("a leader compacts, and ships the result").isGreaterThan(0);
  }

  // #6948
  /** A standalone database has no wrapper at all and must keep maintaining itself. */
  @Test
  void aStandaloneDatabaseStillRunsMaintenance() throws Exception {
    final LocalTimeSeriesType tsType = createTypeWithMutableRows();

    TimeSeriesMaintenanceScheduler.runMaintenance(database, tsType, WRAPPER_TYPE_NAME);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount()).isGreaterThan(0);
  }

  // ---- Helpers ----

  private LocalTimeSeriesType createTypeWithMutableRows() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + WRAPPER_TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(WRAPPER_TYPE_NAME);

    final int rows = 3_000;
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    tsType.getEngine().appendBatch(timestamps, columns);

    assertThat(tsType.getEngine().getShard(0).getSealedStore().getBlockCount())
        .as("nothing is sealed yet, so the assertions below measure this call alone").isZero();
    return tsType;
  }

  /**
   * Installs a wrapper the way the HA server does, so "raw instance" and "wrapped instance" are two different
   * objects with two different answers to the replication flags.
   */
  private void installWrapper(final boolean replicated, final boolean leader) {
    final LocalDatabase real = (LocalDatabase) database;
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(
        DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
        (proxy, method, args) -> {
          switch (method.getName()) {
          // Mirrors RaftReplicatedDatabase: the wrapper is its own wrapped instance.
          case "getWrappedDatabaseInstance":
            if (method.getParameterCount() == 0)
              return proxy;
            break;
          case "isReplicated":
            return replicated;
          case "isLeader":
            return leader;
          default:
            break;
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    real.setWrappedDatabaseInstance(wrapper);
  }
}
