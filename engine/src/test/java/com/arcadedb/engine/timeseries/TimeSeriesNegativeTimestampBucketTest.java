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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4595: bucket alignment broken for negative timestamps.
 * <p>
 * Java integer division truncates toward zero, so {@code (-1500 / 1000) * 1000 == -1000}, while the
 * correct (floor-aligned) bucket is {@code -2000}. The fix uses {@link Math#floorDiv} everywhere the
 * bucket anchor is computed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesNegativeTimestampBucketTest extends TestHelper {

  private List<ColumnDefinition> numericColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @Test
  void singleColumnAggregateAlignsNegativeTimestamps() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_neg_single", numericColumns(), 1);

    // Buckets (interval 1000):
    //   -2500 -> -3000
    //   -1500, -1200 -> -2000
    //   -500, -100 -> -1000
    //    100, 800 -> 0
    engine.appendSamples(
        new long[] { -2500L, -1500L, -1200L, -500L, -100L, 100L, 800L },
        new Object[] { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0 }
    );
    database.commit();

    database.begin();
    final AggregationResult result = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.COUNT, 1000L, null);

    final Map<Long, Long> countByBucket = new HashMap<>();
    for (int i = 0; i < result.size(); i++)
      countByBucket.put(result.getBucketTimestamp(i), result.getCount(i));

    // The buggy truncating division would attribute -1500/-1200 to bucket -1000 (collision with -500/-100).
    assertThat(countByBucket).containsEntry(-3000L, 1L);
    assertThat(countByBucket).containsEntry(-2000L, 2L);
    assertThat(countByBucket).containsEntry(-1000L, 2L);
    assertThat(countByBucket).containsEntry(0L, 2L);
    assertThat(countByBucket).doesNotContainKey(-1500L);
    database.commit();

    engine.close();
  }

  @Test
  void multiColumnAggregateAlignsNegativeTimestamps() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_neg_multi", numericColumns(), 1);

    engine.appendSamples(
        new long[] { -2500L, -1500L, -1200L, -500L, -100L, 100L, 800L },
        new Object[] { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0 }
    );
    database.commit();

    database.begin();
    // columnIndex 1 = "value" (full schema index, including the timestamp column 0)
    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "value"));
    final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 1000L, null);

    final Map<Long, Double> sumByBucket = new HashMap<>();
    final List<Long> bucketTs = result.getBucketTimestamps();
    for (final Long ts : bucketTs)
      sumByBucket.put(ts, result.getValue(ts, 0));

    assertThat(sumByBucket).containsEntry(-3000L, 1.0);
    assertThat(sumByBucket).containsEntry(-2000L, 5.0);  // 2.0 + 3.0
    assertThat(sumByBucket).containsEntry(-1000L, 9.0);  // 4.0 + 5.0
    assertThat(sumByBucket).containsEntry(0L, 13.0);     // 6.0 + 7.0
    assertThat(sumByBucket).doesNotContainKey(-1500L);
    database.commit();

    engine.close();
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }
}
