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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.PositionBitmap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MatchSetTest extends TestHelper {
  private static final long STEP = 1L << 20;

  private int primaryBucketId() {
    createPrimaryBucketType();
    return database.getSchema().getType("Plain").getBuckets(false).get(0).getFileId();
  }

  private void createPrimaryBucketType() {
    if (!database.getSchema().existsType("Plain"))
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
  }

  private RID rid(final int bucketId, final long position) {
    return new RID(bucketId, position);
  }

  private static List<Long> positions(final PositionBitmap bitmap) {
    final List<Long> result = new ArrayList<>();
    if (bitmap != null)
      bitmap.forEach(result::add);
    return result;
  }

  private MatchSet build(final int bucketId, final int filterCount, final long[][] rows) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, filterCount, 0);
    for (final long[] row : rows)
      assertThat(builder.add(rid(bucketId, row[0]), (int) row[1])).isTrue();
    return builder.build();
  }

  @Test
  @Timeout(120)
  void disjunctiveMasks() {
    final int bucketId = primaryBucketId();
    final MatchSet m = build(bucketId, 2, new long[][] { { 1, 0b00 }, { 2, 0b01 }, { 3, 0b10 }, { 4, 0b11 } });
    try {
      m.setFacetFilterIndex(new int[] { 0, 1, -1 });
      assertThat(positions(m.forFacet(0, bucketId))).containsExactly(1L, 2L);
      assertThat(positions(m.forFacet(1, bucketId))).containsExactly(1L, 3L);
      assertThat(positions(m.forFacet(2, bucketId))).containsExactly(1L);
      assertThat(positions(m.forBucket(bucketId))).containsExactly(1L, 2L, 3L, 4L);
      assertThat(m.size()).isEqualTo(4);
      assertThat(m.bucketIds()).containsExactly(bucketId);
      assertThat(m.isOverBudget()).isFalse();
    } finally {
      m.close();
    }
  }

  @Test
  @Timeout(120)
  void forFacetDoesNotLeakWritesIntoTheMatchSet() {
    final int bucketId = primaryBucketId();
    final MatchSet m = build(bucketId, 1, new long[][] { { 1, 0 }, { 2, 1 } });
    try {
      m.setFacetFilterIndex(new int[] { 0 });
      final PositionBitmap first = m.forFacet(0, bucketId);
      first.set(99);
      assertThat(positions(m.forFacet(0, bucketId))).containsExactly(1L, 2L);
      assertThat(positions(m.forBucket(bucketId))).containsExactly(1L, 2L);
    } finally {
      m.close();
    }
  }

  @Test
  @Timeout(120)
  void unknownBucketHasNoBitmap() {
    final int bucketId = primaryBucketId();
    final MatchSet m = build(bucketId, 0, new long[][] { { 1, 0 } });
    try {
      m.setFacetFilterIndex(new int[] { -1 });
      assertThat(m.forBucket(bucketId + 1000)).isNull();
      assertThat(m.forFacet(0, bucketId + 1000)).isNull();
    } finally {
      m.close();
    }
  }

  @Test
  @Timeout(120)
  void stripeFollowsBucketMaxRecordsInPage() {
    final ColumnarTestData data = new ColumnarTestData(1);
    data.externalBody = true;
    data.columnarIndexes = false;
    data.createSchema(database);
    int small = -1;
    int large = -1;
    for (final Bucket bucket : database.getSchema().getBuckets()) {
      final int slots = ((LocalBucket) bucket).getMaxRecordsInPage();
      if (slots == 256)
        small = bucket.getFileId();
      else if (slots == 2048)
        large = bucket.getFileId();
    }
    assertThat(small).as("a bucket with 256 slots per page").isGreaterThanOrEqualTo(0);
    assertThat(large).as("a bucket with 2048 slots per page").isGreaterThanOrEqualTo(0);

    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    for (final long position : new long[] { 255, 256, 300, 3000 }) {
      assertThat(builder.add(rid(small, position), 0)).isTrue();
      assertThat(builder.add(rid(large, position), 0)).isTrue();
    }
    final MatchSet m = builder.build();
    try {
      assertThat(m.forBucket(small).maxRecordsInPage()).isEqualTo(256);
      assertThat(m.forBucket(large).maxRecordsInPage()).isEqualTo(2048);
      assertThat(positions(m.forBucket(small))).containsExactly(255L, 256L, 300L, 3000L);
      assertThat(positions(m.forBucket(large))).containsExactly(255L, 256L, 300L, 3000L);
      assertThat(m.forBucket(small).get(300)).isTrue();
      assertThat(m.forBucket(small).get(301)).isFalse();
      assertThat(m.forBucket(large).get(3000)).isTrue();
      assertThat(m.forBucket(large).get(2999)).isFalse();
      assertThat(m.bucketIds()).isSorted();
    } finally {
      m.close();
    }
  }

  @Test
  @Timeout(120)
  void overBudgetStopsRecording() {
    final int bucketId = primaryBucketId();
    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    try {
      // room for exactly one 1 MiB step: the second step is refused (no wait), after about 2048 pages of 2048 slots
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(STEP + 1024);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final long before = budget.available();
      final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
      int accepted = 0;
      boolean refused = false;
      for (int page = 0; page < 5_000 && !refused; page++) {
        if (builder.add(rid(bucketId, (long) page * 2048), 0))
          accepted++;
        else
          refused = true;
      }
      assertThat(refused).isTrue();
      assertThat(accepted).isGreaterThan(1_000).isLessThan(5_000);
      assertThat(builder.add(rid(bucketId, 7), 0)).as("once over budget, nothing more is recorded").isFalse();
      assertThat(budget.available()).isLessThan(before);

      final MatchSet m = builder.build();
      assertThat(m.isOverBudget()).isTrue();
      m.close();
      assertThat(budget.available()).isEqualTo(before);
      m.close();
      assertThat(budget.available()).isEqualTo(before);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      FacetMemoryBudget.instance();
    }
  }

  @Test
  @Timeout(120)
  void tinyBudgetRefusesTheFirstRecord() {
    final int bucketId = primaryBucketId();
    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1024);
      final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
      assertThat(builder.add(rid(bucketId, 1), 0)).isFalse();
      final MatchSet m = builder.build();
      assertThat(m.isOverBudget()).isTrue();
      assertThat(m.size()).isZero();
      m.close();
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      FacetMemoryBudget.instance();
    }
  }

  @Test
  @Timeout(120)
  void fromProjectionIsLivePlusPending() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(7);
    data.createSchema(database);
    data.insertAssets(database, 100);
    FacetOracle.awaitCaughtUp(database);
    final int[] bucketIds = bucketIdsOf(ColumnarTestData.TYPE);
    final ColumnarIndexManager manager = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();

    MatchSet caughtUp = MatchSet.fromProjection((DatabaseInternal) database, bucketIds, 0);
    try {
      assertThat(cardinality(caughtUp, bucketIds)).isEqualTo(100);
    } finally {
      caughtUp.close();
    }

    final List<RID> inserted;
    try (final AutoCloseable hold = manager.suspendApply()) {
      inserted = data.insertAssets(database, 10);
      final MatchSet lagging = MatchSet.fromProjection((DatabaseInternal) database, bucketIds, 0);
      try {
        assertThat(lagging.isOverBudget()).isFalse();
        // pending is tracked per dirty page, but only its slots holding a committed record are matches: the new positions, not the
        // empty rest of their page
        for (final RID rid : inserted)
          assertThat(lagging.forBucket(rid.getBucketId()).get(rid.getPosition())).as("pending " + rid).isTrue();
        assertThat(cardinality(lagging, bucketIds)).isEqualTo(110);
        assertThat(lagging.size()).isEqualTo(cardinality(lagging, bucketIds));
        assertThat(lagging.snapshot(bucketIds[0])).isNotNull();
      } finally {
        lagging.close();
      }
    }
    FacetOracle.awaitCaughtUp(database);
    final int referencesBefore = ColumnarTestAccess.stateReferences(manager, bucketIds[0]);
    final MatchSet applied = MatchSet.fromProjection((DatabaseInternal) database, bucketIds, 0);
    try {
      assertThat(cardinality(applied, bucketIds)).isEqualTo(110);
      assertThat(ColumnarTestAccess.stateReferences(manager, bucketIds[0])).as("the match set holds its snapshot")
          .isEqualTo(referencesBefore + 1);
    } finally {
      applied.close();
    }
    assertThat(ColumnarTestAccess.stateReferences(manager, bucketIds[0])).as("close releases the snapshot").isEqualTo(referencesBefore);
  }

  @Test
  @Timeout(120)
  void fromProjectionSkipsABucketWithoutProjection() {
    final int bucketId = primaryBucketId();
    final MatchSet m = MatchSet.fromProjection((DatabaseInternal) database, new int[] { bucketId }, 0);
    try {
      assertThat(m.forBucket(bucketId)).isNull();
      assertThat(m.snapshot(bucketId)).isNull();
      assertThat(m.bucketIds()).isEmpty();
      assertThat(m.size()).isZero();
    } finally {
      m.close();
    }
  }

  @Test
  @Timeout(120)
  void fromProjectionOverBudgetKeepsNothingOpen() {
    final ColumnarTestData data = new ColumnarTestData(11);
    data.createSchema(database);
    data.insertAssets(database, 50);
    FacetOracle.awaitCaughtUp(database);
    final int[] bucketIds = bucketIdsOf(ColumnarTestData.TYPE);
    final ColumnarIndexManager manager = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
    final int referencesBefore = ColumnarTestAccess.stateReferences(manager, bucketIds[0]);
    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1024);
      final MatchSet m = MatchSet.fromProjection((DatabaseInternal) database, bucketIds, 0);
      assertThat(m.isOverBudget()).isTrue();
      assertThat(m.forBucket(bucketIds[0])).isNull();
      assertThat(m.snapshot(bucketIds[0])).isNull();
      assertThat(ColumnarTestAccess.stateReferences(manager, bucketIds[0])).as("snapshots closed before returning")
          .isEqualTo(referencesBefore);
      m.close();
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      FacetMemoryBudget.instance();
    }
  }

  @Test
  @Timeout(120)
  void alternatingPagesAreChargedPerDistinctPage() {
    final int bucketId = primaryBucketId();
    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    try {
      // one step only: 100k adds alternating over 2 pages really allocate 2 stripes, but would charge 100k stripes if charged per switch
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(STEP + 1024);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final long before = budget.available();
      try (final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 1, 0)) {
        for (int i = 0; i < 100_000; i++)
          assertThat(builder.add(rid(bucketId, (i % 2) * 2048L + (i % 100)), i % 3 == 0 ? 0 : 1)).as("add " + i).isTrue();
        final MatchSet m = builder.build();
        assertThat(m.isOverBudget()).isFalse();
        assertThat(m.forBucket(bucketId).pages()).containsExactly(0, 1);
        assertThat(before - budget.available()).as("exactly one 1 MiB step reserved").isEqualTo(STEP);
        m.close();
      }
      assertThat(budget.available()).isEqualTo(before);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      FacetMemoryBudget.instance();
    }
  }

  @Test
  @Timeout(120)
  void closingABuilderThatWasNotBuiltReleasesItsReservation() {
    final int bucketId = primaryBucketId();
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final long before = budget.available();
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    assertThat(builder.add(rid(bucketId, 5), 0)).isTrue();
    assertThat(budget.available()).isLessThan(before);
    builder.close();
    assertThat(budget.available()).isEqualTo(before);
    builder.close();
    assertThat(budget.available()).isEqualTo(before);
  }

  @Test
  @Timeout(120)
  void closingABuilderAfterBuildLeavesTheReservationToTheMatchSet() {
    final int bucketId = primaryBucketId();
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final long before = budget.available();
    final MatchSet m;
    try (final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0)) {
      builder.add(rid(bucketId, 5), 0);
      m = builder.build();
    }
    assertThat(budget.available()).isLessThan(before);
    m.close();
    assertThat(budget.available()).isEqualTo(before);
  }

  @Test
  @Timeout(120)
  void unfilteredFacetSharesTheBaseSetWithoutDoubleCharge() {
    final int bucketId = primaryBucketId();
    final MatchSet m = build(bucketId, 0, new long[][] { { 1, 0 }, { 2, 0 }, { 4000, 0 } });
    try {
      m.setFacetFilterIndex(new int[] { -1 });
      assertThat(positions(m.forFacet(0, bucketId))).containsExactly(1L, 2L, 4000L);
      assertThat(positions(m.forFacet(0, bucketId))).isEqualTo(positions(m.forBucket(bucketId)));
      assertThat(m.forFacet(0, bucketId)).isSameAs(m.forBucket(bucketId));
    } finally {
      m.close();
    }
  }

  private int[] bucketIdsOf(final String type) {
    final List<Bucket> buckets = database.getSchema().getType(type).getBuckets(true);
    final int[] ids = new int[buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i).getFileId();
    return ids;
  }

  private static long cardinality(final MatchSet m, final int[] bucketIds) {
    long total = 0;
    for (final int id : bucketIds) {
      final PositionBitmap bitmap = m.forBucket(id);
      if (bitmap != null)
        total += bitmap.cardinality();
    }
    return total;
  }
}
