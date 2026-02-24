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
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesEngineTest extends TestHelper {

  @Test
  void testMultiShardWriteAndQuery() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_engine", cols, 2);

    // Write data — will go to shard based on current thread
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L }, new Object[] { 10.0, 20.0, 30.0 });
    database.commit();

    database.begin();
    final List<Object[]> results = engine.query(1000L, 3000L, null, null);
    assertThat(results).hasSize(3);

    // Results should be sorted by timestamp
    assertThat((long) results.get(0)[0]).isEqualTo(1000L);
    assertThat((long) results.get(1)[0]).isEqualTo(2000L);
    assertThat((long) results.get(2)[0]).isEqualTo(3000L);
    database.commit();

    engine.close();
  }

  @Test
  void testShardCount() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_shards", cols, 4);
    assertThat(engine.getShardCount()).isEqualTo(4);
    assertThat(engine.getColumns()).hasSize(2);
    assertThat(engine.getTypeName()).isEqualTo("test_shards");
    database.commit();

    engine.close();
  }

  @Test
  void testQueryWithTagFilter() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_filter", cols, 1);

    engine.appendSamples(
        new long[] { 1000L, 2000L, 3000L, 4000L },
        new Object[] { "A", "B", "A", "B" },
        new Object[] { 10.0, 20.0, 30.0, 40.0 }
    );
    database.commit();

    database.begin();
    final TagFilter filter = TagFilter.eq(0, "B");
    final List<Object[]> results = engine.query(1000L, 4000L, null, filter);
    assertThat(results).hasSize(2);
    assertThat((String) results.get(0)[1]).isEqualTo("B");
    assertThat((String) results.get(1)[1]).isEqualTo("B");
    database.commit();

    engine.close();
  }

  @Test
  void testRoundRobinShardDistribution() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_roundrobin", cols, 4);

    // Append 4 batches — round-robin should put one in each shard
    for (int i = 0; i < 4; i++)
      engine.appendSamples(new long[] { (i + 1) * 1000L }, new Object[] { (double) i });

    // Each shard should have exactly 1 sample
    for (int s = 0; s < 4; s++)
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).isEqualTo(1);

    // Append 4 more — second round-robin cycle
    for (int i = 4; i < 8; i++)
      engine.appendSamples(new long[] { (i + 1) * 1000L }, new Object[] { (double) i });

    // Each shard should now have exactly 2 samples
    for (int s = 0; s < 4; s++)
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).isEqualTo(2);

    database.commit();

    // All 8 samples should be queryable
    database.begin();
    final List<Object[]> results = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(results).hasSize(8);
    database.commit();

    engine.close();
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }
}
