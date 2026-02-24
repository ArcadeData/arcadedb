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
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

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
  void testRecoveryAfterInterruptedCompaction() throws Exception {
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
  void testRecoveryWithCleanState() throws Exception {
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
  void testRecoveryWithStaleTmpFile() throws Exception {
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
  void testRecoveryWithMissingSealedAndOrphanedTmp() throws Exception {
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
    java.nio.file.Files.copy(sealedFile.toPath(), tmpFile.toPath());
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
  void testRecoveryTruncatesExtraSealedBlocks() throws Exception {
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
    shard.getSealedStore().appendBlock(2, 4000L, 5000L, compressedExtra, mins, maxs, sums, null);
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
}
