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
package com.arcadedb.index.sparsevector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Issue #9482: a top-K whose posting mass sits just under the old 200,000 partitioning threshold ran
 * serially and set the p99, while a query a third of its size split across the whole pool would have
 * been faster. The default is now 50,000, and on the adaptive path the number of ranges follows the
 * posting mass (one range per half the threshold), so a query just past the threshold is split in two
 * instead of claiming every worker.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9482PartitionThresholdTest extends TestHelper {

  private static final int K    = 10;
  /** Two query dims of 30,000 postings each: 60,000 in total, above the new default and below the old one. */
  private static final int DOCS = 30_000;

  @AfterEach
  void restoreConfiguration() {
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
  }

  @Test
  void defaultThresholdIsFiftyThousandPostings() {
    assertThat(GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getDefValue()).isEqualTo(50_000L);
  }

  @Test
  void rangeCountFollowsThePostingMass() {
    // Exactly at the threshold: two ranges, whatever the pool could offer.
    assertThat(PaginatedSparseVectorEngine.rangesByMass(50_000L, 50_000L)).isEqualTo(2);
    assertThat(PaginatedSparseVectorEngine.rangesByMass(74_999L, 50_000L)).isEqualTo(2);
    assertThat(PaginatedSparseVectorEngine.rangesByMass(75_000L, 50_000L)).isEqualTo(3);
    assertThat(PaginatedSparseVectorEngine.rangesByMass(200_000L, 50_000L)).isEqualTo(8);
    // A threshold of 1 (the tests' way of forcing a split) must not cap anything.
    assertThat(PaginatedSparseVectorEngine.rangesByMass(1_000L, 1L)).isEqualTo(1_000);
    assertThat(PaginatedSparseVectorEngine.rangesByMass(Long.MAX_VALUE, 1L)).isEqualTo(Integer.MAX_VALUE);
    assertThat(PaginatedSparseVectorEngine.rangesByMass(0L, 0L)).isZero();
  }

  @Test
  void thePlannerCapsTheRangesByThePostingMass() throws Exception {
    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    // With fewer workers the pool, not the mass, is the limit and the cap cannot be told apart from it.
    assumeThat(pool.getMaxParallelism()).isGreaterThanOrEqualTo(4);

    try (final PaginatedSparseVectorEngine engine = openEngine("idx9482plan")) {
      final int[] queryDims = { 0, 1 };
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      // 60,000 postings against a threshold of 60,000: exactly two ranges, not the pool's worth.
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(2L * DOCS);

      final PaginatedSparseVectorEngine.PartitionPlan plan = engine.planPartitionBoundaries(queryDims, engine.segmentsForTest());
      assertThat(plan).isNotNull();
      try {
        assertThat(plan.boundaries()).hasSize(1);
      } finally {
        pool.releaseWorkers(plan.reservedWorkers());
      }

      // A threshold of 0 must not divide by zero or refuse to plan.
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(0L);
      final PaginatedSparseVectorEngine.PartitionPlan zero = engine.planPartitionBoundaries(queryDims, engine.segmentsForTest());
      if (zero != null)
        pool.releaseWorkers(zero.reservedWorkers());
    }
  }

  @Test
  void aQueryBetweenTheNewAndTheOldThresholdIsSplitAndStaysExact() throws Exception {
    assumeThat(SparseVectorScoringPool.getInstance().getMaxParallelism()).isGreaterThanOrEqualTo(2);
    try (final PaginatedSparseVectorEngine engine = openEngine("idx9482split")) {
      final int[] queryDims = { 0, 1 };
      final float[] queryWeights = { 1.0f, 1.0f };

      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
      final List<RidScore> serial = engine.topK(queryDims, queryWeights, K);
      assertThat(serial).hasSize(K);

      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
      final long before = SparseVectorScoringPool.getInstance().getSplitQueryCount();
      final List<RidScore> adaptive = engine.topK(queryDims, queryWeights, K);

      assertThat(SparseVectorScoringPool.getInstance().getSplitQueryCount()).as("a 60,000-posting query must be split by default")
          .isGreaterThan(before);
      assertThat(adaptive).hasSameSizeAs(serial);
      for (int i = 0; i < serial.size(); i++) {
        assertThat(adaptive.get(i).rid()).isEqualTo(serial.get(i).rid());
        assertThat(adaptive.get(i).score()).isCloseTo(serial.get(i).score(), Offset.offset(1e-4f));
      }
    }
  }

  @Test
  void aQueryBelowTheThresholdStaysOnTheCallerThread() throws Exception {
    try (final PaginatedSparseVectorEngine engine = openEngine("idx9482serial")) {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
      final long before = SparseVectorScoringPool.getInstance().getSplitQueryCount();

      final List<RidScore> result = engine.topK(new int[] { 0 }, new float[] { 1.0f }, K);

      assertThat(result).hasSize(K);
      assertThat(SparseVectorScoringPool.getInstance().getSplitQueryCount()).as("a 30,000-posting query is under the threshold")
          .isEqualTo(before);
    }
  }

  private PaginatedSparseVectorEngine openEngine(final String name) {
    final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database, name,
        SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build());
    final Random rnd = new Random(9482L);
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++) {
        final RID rid = new RID(0, 1L + i);
        engine.put(0, rid, 0.5f + rnd.nextFloat());
        engine.put(1, rid, 0.5f + rnd.nextFloat());
      }
      engine.flush();
    });
    return engine;
  }
}
