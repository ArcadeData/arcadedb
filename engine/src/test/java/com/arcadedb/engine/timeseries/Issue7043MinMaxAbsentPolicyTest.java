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
 * Regression test for issue #7043 (follow-up on #4596): an all-NaN bucket leaked the MIN/MAX init sentinel.
 * <p>
 * {@code isUnusedMinMax} keyed off the raw sample count, and a NaN sample increments that count, so a bucket whose
 * samples were all NaN looked "touched" while its accumulator still held the untouched {@code ±Double.MAX_VALUE}.
 * The single-column path seeds its bucket with NaN and answered NaN over the same data, so the two paths disagreed.
 * <p>
 * The fix removes the sentinel rather than guarding it: MIN/MAX start at {@link TimeSeriesNaN#ABSENT} and fold
 * through the one NaN policy of the subsystem, so "no real sample arrived" is the value itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7043MinMaxAbsentPolicyTest extends TestHelper {

  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  /**
   * The issue's own repro: identical all-NaN data, two code paths, two different answers.
   */
  @Test
  void anAllNaNBucketIsAbsentOnBothTheSingleAndTheMultiColumnPath() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_allnan", COLUMNS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L }, new Object[] { Double.NaN, Double.NaN });
    database.commit();

    try {
      database.begin();
      assertThat(engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.MIN, 0, null).getValue(0))
          .as("single-column MIN over an all-NaN window").isNaN();
      assertThat(engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.MAX, 0, null).getValue(0))
          .as("single-column MAX over an all-NaN window").isNaN();

      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      assertThat(multi.size()).isEqualTo(1);
      final long bucketTs = multi.getBucketTimestamps().getFirst();

      // These two were Double.MAX_VALUE and -Double.MAX_VALUE.
      assertThat(multi.getValue(bucketTs, 0)).as("multi-column MIN over an all-NaN window").isNaN();
      assertThat(multi.getValue(bucketTs, 1)).as("multi-column MAX over an all-NaN window").isNaN();
      database.commit();
    } finally {
      engine.drop();
    }
  }

  /**
   * The guard the sentinel needed must not have cost the ordinary answer: a bucket that holds real values among
   * the NaN ones still reports those values, and a real {@code Double.MAX_VALUE} sample is data, not a sentinel.
   */
  @Test
  void realValuesStillWinAndTheExtremeDoubleIsData() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_mixed", COLUMNS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L },
        new Object[] { Double.NaN, Double.MAX_VALUE, -Double.MAX_VALUE });
    database.commit();

    try {
      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      final long bucketTs = multi.getBucketTimestamps().getFirst();

      assertThat(multi.getValue(bucketTs, 0)).isEqualTo(-Double.MAX_VALUE);
      assertThat(multi.getValue(bucketTs, 1)).isEqualTo(Double.MAX_VALUE);

      assertThat(engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.MIN, 0, null).getValue(0))
          .as("the single-column path must agree").isEqualTo(-Double.MAX_VALUE);
      assertThat(engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.MAX, 0, null).getValue(0))
          .as("the single-column path must agree").isEqualTo(Double.MAX_VALUE);
      database.commit();
    } finally {
      engine.drop();
    }
  }

  /**
   * The same question asked of a SEALED block, where MIN/MAX are answered from the block header the compaction
   * wrote rather than from the samples: {@code reduceNumericStats} used to seed the very same
   * {@code ±Double.MAX_VALUE} into that header, so the aggregation push-down handed the sentinel back as a
   * measurement without ever decompressing a column.
   */
  @Test
  void anAllNaNSealedBlockIsAbsentToo() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_sealed", COLUMNS, 1);
    final long[] timestamps = new long[64];
    final Object[] values = new Object[64];
    for (int i = 0; i < timestamps.length; i++) {
      timestamps[i] = 1000L + i;
      values[i] = Double.NaN;
    }
    engine.appendSamples(timestamps, values);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      final long bucketTs = multi.getBucketTimestamps().getFirst();
      assertThat(multi.getValue(bucketTs, 0)).as("sealed-block MIN over an all-NaN column").isNaN();
      assertThat(multi.getValue(bucketTs, 1)).as("sealed-block MAX over an all-NaN column").isNaN();

      // The DEEP check reconciles the declared statistics against the decoded values, and NaN is now a
      // legitimate declaration: a plain '!=' there would report this healthy block as damaged, because
      // NaN != NaN.
      assertThat(engine.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems())
          .as("an all-NaN block declares NaN statistics and that is correct, not damage").isEmpty();
      database.commit();
    } finally {
      engine.drop();
    }
  }

  /**
   * A sealed block whose columns are a mix of numeric and text must still declare each column's statistics against
   * the right column. The writer emitted a triplet per column with non-NaN statistics and the reader consumed one
   * per column that is neither a TIMESTAMP nor a TAG - two different sets as soon as a FIELD is a STRING, which
   * shifted every following column's min/max/sum onto the wrong column.
   */
  @Test
  void blockStatisticsStayAlignedWithATextFieldBetweenTheNumericOnes() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("label", Type.STRING, ColumnDefinition.ColumnRole.FIELD),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_textfield", columns, 1);
    final long[] timestamps = new long[32];
    final Object[] labels = new Object[32];
    final Object[] values = new Object[32];
    for (int i = 0; i < timestamps.length; i++) {
      timestamps[i] = 1000L + i;
      labels[i] = "l" + i;
      values[i] = 10.0 + i;
    }
    engine.appendSamples(timestamps, labels, values);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(2, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(2, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      final long bucketTs = multi.getBucketTimestamps().getFirst();
      assertThat(multi.getValue(bucketTs, 0)).isEqualTo(10.0);
      assertThat(multi.getValue(bucketTs, 1)).isEqualTo(41.0);

      // The same misalignment would make the DEEP check reconcile a column against another column's statistics.
      assertThat(engine.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      database.commit();
    } finally {
      engine.drop();
    }
  }
}
