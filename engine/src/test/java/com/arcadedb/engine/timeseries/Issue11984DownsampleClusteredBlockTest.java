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
 * Regression test for issue #11984: a downsampling tier skipped every raw sealed block whose AVERAGE sample spacing was at least the
 * granularity, reading it as "already at the target resolution". Nine samples of one series inside one hour plus a tenth nine hours
 * later average one hour of spacing, so the nine stayed raw forever under a one-hour tier, while the same nine alone were downsampled.
 */
class Issue11984DownsampleClusteredBlockTest extends TestHelper {

  private static final long H = 3_600_000L;

  @Test
  void aBlockWhoseSamplesShareABucketIsDownsampledWhateverItsAverageSpacing() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984", columns, 1);
    final long[] timestamps = new long[10];
    final Object[] hosts = new Object[10];
    final Object[] values = new Object[10];
    for (int i = 0; i < 9; i++) {
      timestamps[i] = i * 60_000L;
      hosts[i] = "a";
      values[i] = (double) i;
    }
    timestamps[9] = 9 * H;
    hosts[9] = "a";
    values[9] = 100.0;
    engine.appendSamples(timestamps, hosts, values);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, H));
      engine.applyDownsampling(tiers, 10 * H);

      database.begin();
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();
      assertThat(rows).hasSize(2);
      assertThat((long) rows.get(0)[0]).isEqualTo(0L);
      assertThat((double) rows.get(0)[2]).isEqualTo(4.0); // mean of 0..8
      assertThat((long) rows.get(1)[0]).isEqualTo(9 * H);
      assertThat((double) rows.get(1)[2]).isEqualTo(100.0);

      // the rewritten block carries the marker, so the next pass leaves it alone
      final long rewrites = engine.getShard(0).getSealedStore().getDownsampleRewriteCount();
      engine.applyDownsampling(tiers, 10 * H);
      assertThat(engine.getShard(0).getSealedStore().getDownsampleRewriteCount()).isEqualTo(rewrites);
    } finally {
      engine.close();
    }
  }
}
