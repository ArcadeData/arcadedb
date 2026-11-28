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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle test of the decision that makes a vector index compact itself after a commit ({@code LSMVectorIndex.isCompactionDue}):
 * due exactly when automatic compaction is enabled ({@code indexCompactionMinPagesSchedule > 0} and
 * {@code vectorIndex.compactionBloatFactor > 0}), the file has at least the minimum number of pages, and its pages are at least
 * {@code compactionBloatFactor} times the pages its live set needs. The decision is read off a real bloated index at every
 * boundary of the three settings, with no compaction scheduled in between (the factor is parked out of reach while the garbage is
 * written).
 */
class VectorCompactionDueOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int VERTICES   = 200;
  private static final int PAGE_SIZE  = 16 * 1024;

  @Test
  void dueExactlyWhenTheFileHoldsTheConfiguredMultipleOfItsLivePages() throws Exception {
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();
    final int originalFactor = GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.getValueAsInteger();
    try {
      createSchema();
      // Garbage with the scheduler out of reach: nothing may compact while the file grows
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(1);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(Integer.MAX_VALUE);
      insertVertices();
      for (int cycle = 1; cycle <= 40; cycle++)
        updateAllVertices(cycle);

      final LSMVectorIndex index = vectorIndex();
      final int total = (int) call(index, "totalPagesForBloatRatio");
      final int live = index.estimatePagesForLiveSetForTest();
      assertThat(live).as("the live set needs a page").isGreaterThanOrEqualTo(1);
      assertThat(total).as("the workload must grow the file well past the live set, or nothing is tested").isGreaterThan(3 * live);
      assertThat((boolean) call(index, "isCompactionDue")).as("a factor out of reach").isFalse();

      // The ratio: due at floor(total / live) (the file holds at least that multiple), not at the next one up
      final int atMost = total / live;
      for (final int factor : new int[] { 1, 2, atMost - 1, atMost, atMost + 1, atMost + 2 }) {
        if (factor < 1)
          continue;
        GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(factor);
        final boolean expected = (long) total >= (long) live * factor;
        assertThat((boolean) call(index, "isCompactionDue")).as("total=%d live=%d factor=%d", total, live, factor)
            .isEqualTo(expected);
      }
      // ... and exactly at the multiple, when the live set is a whole divisor of the file
      if (total % live == 0) {
        GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(total / live);
        assertThat((boolean) call(index, "isCompactionDue")).as("the file holds exactly the factor times its live pages").isTrue();
        GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(total / live + 1);
        assertThat((boolean) call(index, "isCompactionDue")).isFalse();
      }

      // The page minimum: a file at least that long is considered, a shorter one never
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(1);
      for (final int minPages : new int[] { 1, total - 1, total, total + 1, Integer.MAX_VALUE }) {
        GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(minPages);
        assertThat((boolean) call(index, "isCompactionDue")).as("total=%d minPages=%d", total, minPages).isEqualTo(total >= minPages);
      }

      // Switched off: the minimum at zero or below (every index type), the factor at zero or below (explicit COMPACT only)
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(1);
      for (final int minPages : new int[] { 0, -1 }) {
        GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(minPages);
        assertThat((boolean) call(index, "isCompactionDue")).as("minPages=%d", minPages).isFalse();
      }
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(1);
      assertThat((boolean) call(index, "isCompactionDue")).as("factor 1 on a bloated file").isTrue();
      for (final int factor : new int[] { 0, -1 }) {
        GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(factor);
        assertThat((boolean) call(index, "isCompactionDue")).as("factor=%d", factor).isFalse();
      }
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(originalFactor);
    }
  }

  @Test
  void anIndexWithoutVectorsIsNeverDue() throws Exception {
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();
    final int originalFactor = GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.getValueAsInteger();
    try {
      createSchema();
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(1);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(1);
      assertThat(vectorIndex().estimatePagesForLiveSetForTest()).isZero();
      assertThat((boolean) call(vectorIndex(), "isCompactionDue")).isFalse();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(originalFactor);
    }
  }

  @Test
  void anIndexWhoseEveryVectorWasDeletedIsNotDueWhateverItsFileHolds() throws Exception {
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();
    final int originalFactor = GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.getValueAsInteger();
    try {
      createSchema();
      // Garbage with the scheduler out of reach, then every vector deleted
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(1);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(Integer.MAX_VALUE);
      insertVertices();
      for (int cycle = 1; cycle <= 10; cycle++)
        updateAllVertices(cycle);
      database.transaction(() -> database.command("sql", "DELETE FROM Doc"));

      final LSMVectorIndex index = vectorIndex();
      assertThat(index.estimatePagesForLiveSetForTest()).as("every vector is deleted: the live set needs no page").isZero();
      assertThat((int) call(index, "totalPagesForBloatRatio")).as("the file still holds the garbage").isGreaterThan(1);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(1);
      assertThat((boolean) call(index, "isCompactionDue")).as("no live page to take a multiple of: left to COMPACT INDEX").isFalse();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
      GlobalConfiguration.VECTOR_INDEX_COMPACTION_BLOAT_FACTOR.setValue(originalFactor);
    }
  }

  private static Object call(final LSMVectorIndex index, final String name) throws Exception {
    final Method method = LSMVectorIndex.class.getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(index);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");
      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType().withPageSize(PAGE_SIZE);
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.INT8).create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
    });
  }

  private void updateAllVertices(final int cycle) {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(i, cycle), "doc" + i);
    });
  }

  private static float[] embedding(final int vertex, final int cycle) {
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
