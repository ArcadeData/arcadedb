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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

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
}
