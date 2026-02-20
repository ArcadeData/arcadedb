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
class TimeSeriesShardTest extends TestHelper {

  private List<ColumnDefinition> createTestColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @Test
  void testAppendAndScan() throws Exception {
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_shard", 0, createTestColumns());

    shard.appendSamples(
        new long[] { 1000L, 2000L, 3000L },
        new Object[] { "A", "B", "A" },
        new Object[] { 20.0, 21.5, 22.0 }
    );
    database.commit();

    database.begin();
    final List<Object[]> results = shard.scanRange(1000L, 3000L, null, null);
    assertThat(results).hasSize(3);
    assertThat((long) results.get(0)[0]).isEqualTo(1000L);
    assertThat((double) results.get(0)[2]).isEqualTo(20.0);
    database.commit();

    shard.close();
  }

  @Test
  void testCompaction() throws Exception {
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_compact_shard", 0, createTestColumns());

    // Insert out-of-order data
    shard.appendSamples(
        new long[] { 3000L, 1000L, 2000L },
        new Object[] { "C", "A", "B" },
        new Object[] { 30.0, 10.0, 20.0 }
    );
    database.commit();

    // Compact
    shard.compact();

    // Verify sealed data is readable and sorted
    database.begin();
    assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(1);

    final List<Object[]> results = shard.scanRange(1000L, 3000L, null, null);
    assertThat(results).hasSize(3);
    // Sealed results should be sorted
    assertThat((long) results.get(0)[0]).isEqualTo(1000L);
    assertThat((long) results.get(1)[0]).isEqualTo(2000L);
    assertThat((long) results.get(2)[0]).isEqualTo(3000L);
    database.commit();

    shard.close();
  }

  @Test
  void testTagFilter() throws Exception {
    database.begin();
    final TimeSeriesShard shard = new TimeSeriesShard(
        (DatabaseInternal) database, "test_filter_shard", 0, createTestColumns());

    shard.appendSamples(
        new long[] { 1000L, 2000L, 3000L, 4000L },
        new Object[] { "A", "B", "A", "B" },
        new Object[] { 20.0, 21.0, 22.0, 23.0 }
    );
    database.commit();

    database.begin();
    final TagFilter filter = TagFilter.eq(0, "A");
    final List<Object[]> results = shard.scanRange(1000L, 4000L, null, filter);
    assertThat(results).hasSize(2);
    assertThat((String) results.get(0)[1]).isEqualTo("A");
    assertThat((String) results.get(1)[1]).isEqualTo("A");
    database.commit();

    shard.close();
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }
}
