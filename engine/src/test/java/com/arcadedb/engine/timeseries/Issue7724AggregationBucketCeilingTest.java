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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7724: a refused time-series aggregation still paid for every bucket it would never return.
 * <p>
 * The three aggregation surfaces - {@code /ts/query}, the Grafana query route and the gRPC bucket stream - all
 * refuse a response carrying more buckets than a configured maximum, and all three enforced that on the RESULT:
 * {@code aggregateMulti} visited the whole range, filled the whole bucket set, and the refusal then threw it
 * away. The refusal bounded the response; it did not bound the cost of producing it, so the cheapest shape of an
 * abusive request - a one-millisecond bucket interval over a wide range, re-issued by a dashboard every few
 * seconds - made the server do the entire scan every time.
 * <p>
 * The bound is now carried INTO the scan. These tests pin the two halves that make that safe: the scan really
 * does stop (asserted on the blocks it touched, from {@link AggregationMetrics}, not on elapsed time), and what
 * it stops with is ALWAYS over the ceiling, which is what guarantees the caller's own refusal still fires and no
 * partial result can reach anybody.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7724AggregationBucketCeilingTest extends TestHelper {

  private static final String TYPE = "Ceiling";
  // One sample per bucket, so the bucket count is the sample count and the arithmetic below is exact.
  private static final long   BUCKET = 1_000L;
  private static final int    SAMPLES = 600;

  /**
   * Without a ceiling the whole range is visited and every bucket is returned: the baseline the bounded runs
   * below are compared against, and the proof that nothing truncates a request that did not ask for a bound.
   */
  @Test
  void anUnboundedAggregationStillVisitsEverything() throws Exception {
    final TimeSeriesEngine engine = sealedType();

    final AggregationMetrics metrics = new AggregationMetrics();
    final MultiColumnAggregationResult result = aggregate(engine, metrics, 0);

    assertThat(result.getBucketTimestamps()).hasSize(SAMPLES);
    assertThat(result.isOverBucketCeiling()).as("no ceiling was declared").isFalse();
    assertThat(blocksTouched(metrics)).as("every block was read").isGreaterThan(1);
  }

  /**
   * The bounded run stops. It reads strictly fewer blocks than the unbounded one over the same data, and the
   * numbers are block counts rather than a stopwatch: a wall-clock bound would be a coin flip on a loaded CI
   * machine and would say nothing about WHY the run was shorter.
   */
  @Test
  void aBoundedAggregationStopsInsteadOfFinishingTheRange() throws Exception {
    final TimeSeriesEngine engine = sealedType();

    final AggregationMetrics unbounded = new AggregationMetrics();
    aggregate(engine, unbounded, 0);

    final AggregationMetrics bounded = new AggregationMetrics();
    final MultiColumnAggregationResult result = aggregate(engine, bounded, 10);

    assertThat(blocksTouched(bounded)).as("the scan stopped rather than visiting the whole range")
        .isLessThan(blocksTouched(unbounded));
    assertThat(result.getBucketTimestamps().size())
        .as("stopping happens only once the ceiling is already passed, so the caller's own check still refuses")
        .isGreaterThan(10);
  }

  /**
   * The contract the three surfaces rely on: a result that stopped is over the ceiling, so the check they
   * already perform on {@code getBucketTimestamps().size()} necessarily fires. A partial answer can therefore
   * never be serialized to a caller.
   */
  @Test
  void aStoppedResultIsAlwaysOverTheCeiling() throws Exception {
    final TimeSeriesEngine engine = sealedType();

    for (final int ceiling : new int[] { 1, 5, 50, 199 }) {
      final MultiColumnAggregationResult result = aggregate(engine, null, ceiling);
      assertThat(result.getBucketTimestamps().size())
          .as("a result stopped at a ceiling of %d must be above it", ceiling)
          .isGreaterThan(ceiling);
    }
  }

  /**
   * A ceiling the data cannot reach changes nothing: the full answer comes back and nothing is marked over.
   * Without this the bound would be a truncation rather than a refusal mechanism.
   */
  @Test
  void aCeilingTheAnswerStaysUnderIsInvisible() throws Exception {
    final TimeSeriesEngine engine = sealedType();

    final MultiColumnAggregationResult result = aggregate(engine, null, SAMPLES * 10);

    assertThat(result.getBucketTimestamps()).hasSize(SAMPLES);
    assertThat(result.isOverBucketCeiling()).isFalse();
  }

  /**
   * The multi-shard path, where each shard stops against the ceiling independently and the results are then
   * merged. The invariant the three surfaces rely on has to survive that merge: whatever the shards did, what
   * comes back is over the ceiling, so the caller's own check still refuses it.
   * <p>
   * Block counts are deliberately NOT asserted here - samples are routed to shards round-robin, so how many
   * blocks each shard ends up touching depends on the machine's core count, which is what
   * {@code Issue7089NaNTransparentSumAvgTest} was made to pin {@code SHARDS 1} for. The bound this test is
   * about is the merged bucket count, and that is machine-independent.
   */
  @Test
  void theMergedResultOfSeveralShardsIsStillOverTheCeiling() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Sharded"
        + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 4 COMPACTION_INTERVAL 1 SECONDS");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sharded")).getEngine();

    database.transaction(() -> {
      for (int i = 0; i < SAMPLES; i++)
        database.command("sql", "INSERT INTO Sharded SET ts = :ts, value = :v",
            Map.of("ts", i * BUCKET, "v", (double) i));
    });
    engine.compactAll();

    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"));

    for (final int ceiling : new int[] { 1, 10, 100 }) {
      database.begin();
      final MultiColumnAggregationResult result;
      try {
        result = engine.aggregateMulti(0L, SAMPLES * BUCKET, requests, BUCKET, null, null, ceiling);
      } finally {
        database.commit();
      }
      assertThat(result.getBucketTimestamps().size())
          .as("merged across 4 shards, a result stopped at a ceiling of %d must still be above it", ceiling)
          .isGreaterThan(ceiling);
    }

    // And a ceiling the answer stays under still returns every bucket, merged across all four shards.
    database.begin();
    try {
      assertThat(engine.aggregateMulti(0L, SAMPLES * BUCKET, requests, BUCKET, null, null, SAMPLES * 10)
          .getBucketTimestamps()).hasSize(SAMPLES);
    } finally {
      database.commit();
    }
  }

  /** The used-bucket count is the number of rows the result will hand back, in both storage modes. */
  @Test
  void theUsedBucketCountIsTheNumberOfRowsReturned() {
    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"));

    final MultiColumnAggregationResult map = new MultiColumnAggregationResult(requests);
    map.accumulate(0L, 0, 1.0);
    map.accumulate(1_000L, 0, 1.0);
    map.accumulate(1_000L, 0, 2.0);
    assertThat(map.getUsedBucketCount()).isEqualTo(2);
    assertThat(map.getUsedBucketCount()).isEqualTo(map.getBucketTimestamps().size());

    final MultiColumnAggregationResult flat = new MultiColumnAggregationResult(requests, 0L, 1_000L, 4);
    assertThat(flat.isFlatMode()).isTrue();
    flat.accumulate(0L, 0, 1.0);
    flat.accumulate(1_000L, 0, 1.0);
    // Outside the pre-allocated window, so it lands in the overflow map and must still be counted.
    flat.accumulate(90_000L, 0, 1.0);
    assertThat(flat.getUsedBucketCount()).isEqualTo(3);
    assertThat(flat.getUsedBucketCount()).isEqualTo(flat.getBucketTimestamps().size());

    flat.setBucketCeiling(3);
    assertThat(flat.isOverBucketCeiling()).as("equal to the ceiling is not over it").isFalse();
    flat.accumulate(91_000L, 0, 1.0);
    assertThat(flat.isOverBucketCeiling()).isTrue();
  }

  /**
   * Issue #7717, on the layer that had no counters: a read answered entirely from the MUTABLE bucket must
   * report the pages it examined and the rows it materialised.
   * <p>
   * Not an edge case - it is every read of a type whose compaction interval has not elapsed - and reporting
   * zero work for it would make the counters worse than absent, because an operator would read "this query
   * touched nothing" from a query that touched everything. The sealed half was counted from the start; the
   * ascending mutable scan was not, while its descending twin always had been (CodeRabbit on PR #7728).
   */
  @Test
  void aReadAnsweredFromTheMutableBucketAloneStillReportsItsWork() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Unsealed TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Unsealed")).getEngine();

    // Deliberately NOT compacted: every sample is still in the mutable bucket.
    database.transaction(() -> {
      for (int i = 0; i < SAMPLES; i++)
        database.command("sql", "INSERT INTO Unsealed SET ts = :ts, value = :v",
            Map.of("ts", i * BUCKET, "v", (double) i));
    });

    final AggregationMetrics metrics = new AggregationMetrics();
    database.begin();
    final int rows;
    try {
      final Iterator<Object[]> it = engine.iterateQuery(0L, SAMPLES * BUCKET, null, null, metrics);
      int counted = 0;
      while (it.hasNext()) {
        it.next();
        counted++;
      }
      rows = counted;
    } finally {
      database.commit();
    }

    assertThat(rows).as("the rows really were read").isEqualTo(SAMPLES);
    assertThat(metrics.getScannedPages()).as("the mutable pages it examined").isPositive();
    assertThat(metrics.getMaterializedRows()).as("the rows it turned into objects").isEqualTo(SAMPLES);
    assertThat(metrics.getSlowPathBlocks() + metrics.getFastPathBlocks())
        .as("nothing was sealed, so no block was read").isZero();
  }

  // ---- helpers ----

  private static int blocksTouched(final AggregationMetrics metrics) {
    return metrics.getFastPathBlocks() + metrics.getSlowPathBlocks() + metrics.getSkippedBlocks();
  }

  private MultiColumnAggregationResult aggregate(final TimeSeriesEngine engine, final AggregationMetrics metrics,
      final int ceiling) throws Exception {
    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"));

    database.begin();
    try {
      return engine.aggregateMulti(0L, SAMPLES * BUCKET, requests, BUCKET, null, metrics, ceiling);
    } finally {
      database.commit();
    }
  }

  /**
   * A type whose samples are all SEALED, so the scan the ceiling stops is the block loop, and one whose blocks
   * are numerous enough that stopping is visible as a block count. One shard, so the number of blocks does not
   * depend on the machine's core count.
   */
  private TimeSeriesEngine sealedType() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1 COMPACTION_INTERVAL 1 SECONDS");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();

    database.transaction(() -> {
      for (int i = 0; i < SAMPLES; i++)
        database.command("sql", "INSERT INTO " + TYPE + " SET ts = :ts, value = :v",
            Map.of("ts", i * BUCKET, "v", (double) i));
    });

    engine.compactAll();
    return engine;
  }
}
