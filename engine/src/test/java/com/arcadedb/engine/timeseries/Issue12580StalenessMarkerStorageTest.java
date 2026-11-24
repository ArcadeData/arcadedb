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
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator;
import com.arcadedb.engine.timeseries.promql.PromQLParser;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12580: the Prometheus staleness marker (the NaN with bits {@code 0x7ff0000000000002}, issue #11991) has
 * to survive every way the store keeps a value, or an ended series is answered again. Downsampling skipped it as an ordinary NaN, so
 * a bucket in which a series ended became the mean of its real samples (answered for one more lookback) or, holding nothing but the
 * marker, an ordinary NaN; a FLOAT column narrowed it to an ordinary NaN; and a DOUBLE field sealed with the DICTIONARY codec kept it
 * as the text {@code NaN}. The expected answers are the ones of the raw samples, which is what Prometheus, having no downsampling,
 * answers.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12580StalenessMarkerStorageTest extends TestHelper {

  /** On a minute boundary, so the one-minute buckets start where the samples do. */
  private static final long   T      = 1_700_000_040_000L;
  private static final long   MINUTE = 60_000L;
  private static final double STALE  = Double.longBitsToDouble(0x7ff0000000000002L);

  @Test
  void aSeriesThatEndedInADownsampledBucketIsNotAnsweredAfterIt() throws Exception {
    final TimeSeriesEngine engine = createType("up", "DOUBLE");
    append(engine, new long[] { T, T + 15_000, T + 30_000, T, T + 15_000, T + 30_000, T + 45_000 },
        new Object[] { "a", "a", "a", "b", "b", "b", "b" }, new Object[] { 1.0, 1.0, STALE, 1.0, 1.0, 1.0, 1.0 });
    engine.compactAll();
    assertThat(byJob("up", T + 61_000)).containsOnlyKeys("b");

    downsample(engine);

    // the reproducer of the issue: after the bucket the series is gone, as it was on the raw samples
    assertThat(byJob("up", T + 61_000)).containsOnlyKeys("b");
    // the end of the series is where it was, and the mean of its real samples is kept for the evaluations before it
    assertThat(byJob("up", T + 31_000)).containsOnlyKeys("b");
    assertThat(byJob("up", T + 20_000)).containsEntry("a", 1.0).containsEntry("b", 1.0);
    // the marker is no sample of a range
    assertThat(byJob("count_over_time(up[2m])", T + 61_000)).containsEntry("a", 1.0).containsEntry("b", 1.0);

    final List<Object[]> rowsOfA = rowsOf(engine, "a");
    assertThat(rowsOfA).hasSize(2);
    assertThat((long) rowsOfA.get(0)[0]).isEqualTo(T);
    assertThat((double) rowsOfA.get(0)[2]).isEqualTo(1.0);
    assertThat((long) rowsOfA.get(1)[0]).isEqualTo(T + 30_000);
    assertThat(Double.doubleToRawLongBits((double) rowsOfA.get(1)[2])).isEqualTo(0x7ff0000000000002L);

    // the mean row and the marker row are what a pass leaves, so the next pass does not rewrite them, neither in this process...
    final long rewrites = engine.getShard(0).getSealedStore().getDownsampleRewriteCount();
    downsample(engine);
    assertThat(engine.getShard(0).getSealedStore().getDownsampleRewriteCount()).isEqualTo(rewrites);

    // ... nor after a restart, which forgets that the block was downsampled and asks its samples again
    reopenDatabase();
    final TimeSeriesEngine reopened = engineOf("up");
    downsample(reopened);
    assertThat(reopened.getShard(0).getSealedStore().getDownsampleRewriteCount()).isZero();
    assertThat(rowsOf(reopened, "a")).hasSize(2);
    assertThat(byJob("up", T + 61_000)).containsOnlyKeys("b");
  }

  @Test
  void aBucketHoldingOnlyTheMarkerIsNotReadAsANaNValue() throws Exception {
    final TimeSeriesEngine engine = createType("up", "DOUBLE");
    // job a's last real sample is in the bucket before, its marker alone in the bucket at T; job b makes the block reducible
    append(engine, new long[] { T - 10_000, T + 5_000, T - 50_000, T - 10_000, T + 5_000, T + 20_000 },
        new Object[] { "a", "a", "b", "b", "b", "b" }, new Object[] { 1.0, STALE, 2.0, 2.0, 2.0, 2.0 });
    engine.compactAll();
    downsample(engine);

    assertThat(byJob("up", T - 1_000)).containsEntry("a", 1.0).containsEntry("b", 2.0);
    assertThat(byJob("up", T + 6_000)).containsOnlyKeys("b");
    assertThat(byJob("up", T + 61_000)).containsOnlyKeys("b");
  }

  @Test
  void aSeriesThatRestartsAfterTheMarkerInTheSameBucketKeepsItsMean() throws Exception {
    final TimeSeriesEngine engine = createType("up", "DOUBLE");
    append(engine, new long[] { T, T + 10_000, T + 20_000 }, new Object[] { "a", "a", "a" }, new Object[] { 1.0, STALE, 3.0 });
    engine.compactAll();
    downsample(engine);

    assertThat(rowsOf(engine, "a")).hasSize(1);
    assertThat(byJob("up", T + 61_000)).containsOnlyKeys("a").containsEntry("a", 2.0);
  }

  /** A marker ends the series of its own field: the other fields of the marker row hold no value, and their mean is kept whole. */
  @Test
  void theMarkerRowEndsOnlyTheFieldThatEnded() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE multi TIMESTAMP ts TAGS (job STRING) FIELDS (value DOUBLE, other DOUBLE, n LONG) SHARDS 1");
    final TimeSeriesEngine engine = engineOf("multi");
    append(engine, new long[] { T, T + 15_000, T + 30_000, T + 45_000 }, new Object[] { "a", "a", "a", "a" },
        new Object[] { 1.0, 3.0, STALE, Double.NaN }, new Object[] { 10.0, 20.0, 30.0, 40.0 }, new Object[] { 1L, 2L, 3L, 4L });
    engine.compactAll();
    downsample(engine);

    final List<Object[]> rows = rowsOf(engine, "a");
    assertThat(rows).hasSize(2);
    assertThat((long) rows.get(0)[0]).isEqualTo(T);
    assertThat((double) rows.get(0)[2]).isEqualTo(2.0);
    assertThat((double) rows.get(0)[3]).isEqualTo(25.0);
    assertThat((long) rows.get(0)[4]).isEqualTo(2L);
    assertThat((long) rows.get(1)[0]).isEqualTo(T + 30_000);
    assertThat(Double.doubleToRawLongBits((double) rows.get(1)[2])).isEqualTo(0x7ff0000000000002L);
    assertThat((double) rows.get(1)[3]).isNaN();
    assertThat(rows.get(1)[4]).isNull();
  }

  @Test
  void anOrdinaryNaNIsStillAValueAfterDownsampling() throws Exception {
    final TimeSeriesEngine engine = createType("up", "DOUBLE");
    append(engine, new long[] { T, T + 15_000, T, T + 15_000 }, new Object[] { "a", "a", "c", "c" },
        new Object[] { Double.NaN, Double.NaN, 1.0, Double.NaN });
    engine.compactAll();
    downsample(engine);

    final Map<String, Double> at61 = byJob("up", T + 61_000);
    assertThat(at61).containsOnlyKeys("a", "c");
    assertThat(at61.get("a")).isNaN();
    assertThat(at61.get("c")).isEqualTo(1.0);
  }

  @Test
  void aFloatColumnKeepsTheMarker() throws Exception {
    final TimeSeriesEngine engine = createType("up", "FLOAT");
    append(engine, new long[] { T, T + 15_000, T + 30_000, T, T + 15_000, T + 30_000, T + 45_000, T + 30_000 },
        new Object[] { "a", "a", "a", "b", "b", "b", "b", "c" }, new Object[] { 1.0, 1.0, STALE, 1.0, 1.0, 1.0, 1.0, Double.NaN });

    assertEndedSeriesIsGone();
    engine.compactAll();
    assertEndedSeriesIsGone();
    downsample(engine);
    assertThat(byJob("up{job=~\"a|b\"}", T + 61_000)).containsOnlyKeys("b");
    assertThat(byJob("up", T + 20_000)).containsEntry("a", 1.0);
  }

  /** The primitive batch the ingestion surfaces fill narrows the double for a FLOAT column as well, through both of its setters. */
  @Test
  void aFloatColumnFilledThroughABatchKeepsTheMarker() throws Exception {
    for (final boolean primitiveSetter : new boolean[] { true, false }) {
      final String metric = primitiveSetter ? "up" : "up_boxed";
      final TimeSeriesEngine engine = createType(metric, "FLOAT");
      final TimeSeriesBatch batch = engine.newBatch(8);
      final long[] timestamps = { T, T + 15_000, T + 30_000, T, T + 15_000, T + 30_000, T + 45_000, T + 30_000 };
      final String[] jobs = { "a", "a", "a", "b", "b", "b", "b", "c" };
      final double[] values = { 1.0, 1.0, STALE, 1.0, 1.0, 1.0, 1.0, Double.NaN };
      for (int i = 0; i < timestamps.length; i++) {
        final int row = batch.addRow(timestamps[i]);
        batch.setString(row, 0, jobs[i]);
        if (primitiveSetter)
          batch.setDouble(row, 1, values[i]);
        else
          batch.setValue(row, 1, values[i]);
      }
      database.begin();
      engine.appendSamples(batch);
      database.commit();

      assertEndedSeriesIsGone(metric);
      engine.compactAll();
      assertEndedSeriesIsGone(metric);
    }
  }

  @Test
  void aDoubleFieldSealedAsADictionaryKeepsTheMarker() throws Exception {
    final TimeSeriesEngine engine = createType("up", "DOUBLE CODEC DICTIONARY");
    append(engine, new long[] { T, T + 15_000, T + 30_000, T, T + 15_000, T + 30_000, T + 45_000, T + 30_000 },
        new Object[] { "a", "a", "a", "b", "b", "b", "b", "c" }, new Object[] { 1.0, 1.0, STALE, 1.0, 1.0, 1.0, 1.0, Double.NaN });

    assertEndedSeriesIsGone();
    engine.compactAll();
    assertEndedSeriesIsGone();
    // not downsampled: a field sealed with the DICTIONARY codec cannot be downsampled yet (issue #12666)
  }

  /** Before the marker job a is there; from it on it is gone, and the ordinary NaN of job c is still answered as one. */
  private void assertEndedSeriesIsGone() {
    assertEndedSeriesIsGone("up");
  }

  private void assertEndedSeriesIsGone(final String metric) {
    assertThat(byJob(metric, T + 20_000)).containsOnlyKeys("a", "b");
    final Map<String, Double> at31 = byJob(metric, T + 31_000);
    assertThat(at31).containsOnlyKeys("b", "c");
    assertThat(at31.get("c")).isNaN();
  }

  private TimeSeriesEngine createType(final String name, final String field) {
    database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (job STRING) FIELDS (value " + field + ") SHARDS 1");
    return engineOf(name);
  }

  private TimeSeriesEngine engineOf(final String name) {
    return ((LocalTimeSeriesType) database.getSchema().getType(name)).getEngine();
  }

  private void append(final TimeSeriesEngine engine, final long[] timestamps, final Object[]... columnValues) throws IOException {
    database.begin();
    engine.appendSamples(timestamps, columnValues);
    database.commit();
  }

  private static void downsample(final TimeSeriesEngine engine) throws IOException {
    engine.applyDownsampling(List.of(new DownsamplingTier(1L, MINUTE)), T + 10 * MINUTE);
  }

  private List<Object[]> rowsOf(final TimeSeriesEngine engine, final String job) throws IOException {
    database.begin();
    try {
      final List<Object[]> rows = new ArrayList<>();
      for (final Object[] row : engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
        if (job.equals(row[1]))
          rows.add(row);
      rows.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
      return rows;
    } finally {
      database.commit();
    }
  }

  private Map<String, Double> byJob(final String query, final long atMs) {
    return evaluate(query, atMs).samples().stream().collect(Collectors.toMap(s -> s.labels().get("job"), VectorSample::value));
  }

  private InstantVector evaluate(final String query, final long atMs) {
    return (InstantVector) new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(new PromQLParser(query).parse(), atMs);
  }
}
