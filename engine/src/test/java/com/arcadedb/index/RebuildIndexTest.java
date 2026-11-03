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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@code REBUILD INDEX}: failures, stale handles and boundary values. Each nested class keeps the
 * issue number of the regression it covers in its display name.
 */
class RebuildIndexTest {
  /**
   * Regression for issue #9254: {@code REBUILD INDEX} drops the index and then builds the new one, so a build that failed
   * (a stored row with a null key under an index whose null strategy was switched to ERROR) left the type without the
   * index, and a UNIQUE index lost its constraint. A failed rebuild must leave the index it found.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9254 a failed rebuild keeps the existing indexes")
  class FailedRebuildTest extends TestHelper {

    @Test
    void failedRebuildKeepsLsmUniqueIndex() {
      verifyFailedRebuildKeepsIndexes("L", "UNIQUE", "NOTUNIQUE");
    }

    @Test
    void failedRebuildKeepsHashUniqueIndex() {
      verifyFailedRebuildKeepsIndexes("H", "UNIQUE_HASH", "NOTUNIQUE_HASH");
    }

    @Test
    void failedRebuildOfAllIndexesKeepsTheOnesThatFailed() {
      prepare("A", "UNIQUE", "NOTUNIQUE");

      try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<List<String>>getProperty("failedIndexes")).hasSize(2);
      }

      assertIndexesKept("A", true);
      assertDuplicateRefused("A");
    }

    private void verifyFailedRebuildKeepsIndexes(final String type, final String unique, final String notUnique) {
      prepare(type, unique, notUnique);

      for (final String property : new String[] { "u", "n" })
        assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `" + type + "[" + property + "]`").close()).isInstanceOf(
            IndexException.class);

      assertIndexesKept(type, true);
      assertDuplicateRefused(type);

      // the same after a reopen (Index.setNullStrategy() is not persisted, so the strategy is not compared)
      reopenDatabase();
      assertIndexesKept(type, false);
      assertDuplicateRefused(type);
    }

    private void prepare(final String type, final String unique, final String notUnique) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + type).close();
        database.command("sql", "CREATE PROPERTY " + type + ".u STRING").close();
        database.command("sql", "CREATE PROPERTY " + type + ".n STRING").close();
        database.command("sql", "CREATE INDEX ON " + type + " (u) " + unique + " NULL_STRATEGY SKIP").close();
        database.command("sql", "CREATE INDEX ON " + type + " (n) " + notUnique + " NULL_STRATEGY SKIP").close();
      });
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, u = 'u1', n = 'u1'").close();
        database.command("sql", "INSERT INTO " + type + " SET id = 2, u = 'u2', n = 'u2'").close();
        database.command("sql", "INSERT INTO " + type + " SET id = 3").close();
      });
      // the stored null row can no longer be indexed, so a rebuild must fail
      for (final String property : new String[] { "u", "n" })
        database.getSchema().getIndexByName(type + "[" + property + "]").setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
    }

    private void assertIndexesKept(final String type, final boolean checkNullStrategy) {
      assertThat(database.getSchema().getType(type).getAllIndexes(false)).hasSize(2);
      for (final String property : new String[] { "u", "n" }) {
        final Index index = database.getSchema().getIndexByName(type + "[" + property + "]");
        if (checkNullStrategy)
          assertThat(index.getNullStrategy()).isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
        assertThat(index.countEntries()).isEqualTo(2);
      }
      assertThat(database.getSchema().getIndexByName(type + "[u]").isUnique()).isTrue();

      try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM " + type + " WHERE u = 'u1'")) {
        assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("FETCH FROM INDEX");
      }
    }

    private void assertDuplicateRefused(final String type) {
      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "INSERT INTO " + type + " SET id = 9, u = 'u1'").close())).isInstanceOf(
          DuplicatedKeyException.class);
    }
  }

  /**
   * Issue #10897: {@code REBUILD INDEX} retires the index object and creates a new one under the same name. A caller holding the
   * bucket-level {@link LSMVectorIndex} from before the rebuild was answered as if the index were empty - 0 neighbours, 0 entries,
   * zeroed stats - with no exception, while the {@link TypeIndex} over it and an LSM_TREE bucket refuse with "not valid. Probably has
   * been drop or rebuilt", which is what tells the caller to fetch the index again. Every kind of bucket index now refuses the same
   * way on its read paths, and the new object fetched by name answers the data.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#10897 bucket index handle held across a rebuild refuses reads")
  class StaleIndexAfterRebuildTest extends TestHelper {
    private static final int    COUNT      = 500;
    private static final int    DIMENSIONS = 16;
    private static final String NOT_VALID  = "is not valid. Probably has been drop";

    @Test
    void aDenseVectorBucketIndexHeldAcrossARebuildRefusesInsteadOfAnsweringNothing() {
      database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      final Random random = new Random(7);
      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          database.newVertex("Doc").set("embedding", randomVector(random)).save();
      });
      database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\"}");

      final TypeIndex oldType = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex old = (LSMVectorIndex) oldType.getIndexesOnBuckets()[0];
      final float[] query = randomVector(new Random(11));
      assertThat(old.findNeighborsFromVector(query, 5)).hasSize(5);
      assertThat(old.countEntries()).isEqualTo(COUNT);

      database.command("sql", "REBUILD INDEX `Doc[embedding]`");

      assertThat(old.isValid()).isFalse();
      assertRefused(() -> old.findNeighborsFromVector(query, 5));
      assertRefused(() -> old.findNeighborsFromVector(query, 5, -1, null, WorkGuard.forCommandDeadline(null)));
      assertRefused(() -> old.findNeighborsFromVectorApproximate(query, 5));
      assertRefused(() -> old.findNeighborsFromVectorGrouped(query, 5, 1, -1, null, rid -> rid));
      assertRefused(() -> old.findNeighborsFromVectorForGroups(query, Set.of("g"), 1, -1, null, rid -> "g", Float.POSITIVE_INFINITY));
      assertRefused(() -> old.get(new Object[] { query }, 5));
      assertRefused(old::countEntries);
      // the stats describe the instance itself, and its close-time diagnostics are read after it was released
      assertThat(old.getStats()).isNotNull();

      // the data is intact: the index fetched again by name answers it
      final LSMVectorIndex fresh = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]"))
          .getIndexesOnBuckets()[0];
      assertThat(fresh).isNotSameAs(old);
      assertThat(fresh.findNeighborsFromVector(query, 5)).hasSize(5);
      assertThat(fresh.countEntries()).isEqualTo(COUNT);
      assertThat(fresh.getStats().get("activeVectors")).isEqualTo(COUNT);
    }

    @Test
    void aSparseVectorBucketIndexHeldAcrossARebuildRefusesToo() {
      database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          database.newDocument("Sparse").set("tokens", new int[] { i % 16 }, "weights", new float[] { 1.0f }).save();
      });

      final String name = "Sparse[tokens,weights]";
      final LSMSparseVectorIndex old = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(name))
          .getIndexesOnBuckets()[0];
      final int[] tokens = { 3 };
      final float[] weights = { 1.0f };
      assertThat(old.topK(tokens, weights, 5, null)).hasSize(5);

      database.command("sql", "REBUILD INDEX `" + name + "`");

      assertThat(old.isValid()).isFalse();
      assertRefused(() -> old.topK(tokens, weights, 5, null));
      assertRefused(() -> old.topKGrouped(tokens, weights, 5, 1, null, rid -> rid));
      assertRefused(() -> old.topKForGroups(tokens, weights, Set.of("g"), 1, 0f, null, rid -> "g", null));
      assertRefused(old::countEntries);
      assertThat(old.getStats()).isNotNull();

      final LSMSparseVectorIndex fresh = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(name))
          .getIndexesOnBuckets()[0];
      assertThat(fresh.topK(tokens, weights, 5, null)).hasSize(5);
    }

    /**
     * The contrast the report draws, which the vector indexes now match: a stale HASH bucket refuses its reads. An LSM_TREE index is
     * rebuilt IN PLACE: its bucket index is the same object before and after, and keeps answering.
     */
    @Test
    void lsmTreeAndHashBucketIndexesHeldAcrossARebuildRefuseTheirReads() {
      database.command("sql", "CREATE DOCUMENT TYPE Tag BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Tag.name STRING");
      database.command("sql", "CREATE PROPERTY Tag.code STRING");
      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          database.newDocument("Tag").set("name", "t" + i, "code", "c" + i).save();
      });
      database.command("sql", "CREATE INDEX ON Tag (name) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Tag (code) UNIQUE_HASH");

      for (final String name : new String[] { "Tag[name]", "Tag[code]" }) {
        final IndexInternal old = ((TypeIndex) database.getSchema().getIndexByName(name)).getIndexesOnBuckets()[0];
        assertThat(old.countEntries()).isEqualTo(COUNT);

        database.command("sql", "REBUILD INDEX `" + name + "`");

        final IndexInternal fresh = ((TypeIndex) database.getSchema().getIndexByName(name)).getIndexesOnBuckets()[0];
        if (name.equals("Tag[name]")) {
          assertThat(fresh).isSameAs(old);
          assertThat(old.isValid()).isTrue();
          assertThat(old.countEntries()).isEqualTo(COUNT);
          assertThat(old.get(new Object[] { "t5" }).hasNext()).isTrue();
          continue;
        }

        assertThat(old.isValid()).isFalse();
        assertRefused(old::countEntries);
        assertRefused(() -> old.get(new Object[] { "c5" }));
        assertThat(fresh.countEntries()).isEqualTo(COUNT);
        assertThat(fresh.getStats()).isNotNull();
      }
    }

    private static void assertRefused(final ThrowingCallable call) {
      assertThatThrownBy(call).isInstanceOf(IndexException.class).hasMessageContaining(NOT_VALID);
    }

    private static float[] randomVector(final Random random) {
      final float[] vector = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        vector[j] = (float) random.nextGaussian();
      return vector;
    }
  }

  /**
   * Code review follow-up on PR #5967 (fix for #5905/#5906): {@code Type.convert()} now range-checks before narrowing
   * to BYTE/SHORT/INTEGER instead of silently wrapping, and {@code LSMTreeIndexAbstract.convertKeysToDeclaredTypes()}/
   * {@code LSMTreeIndex.convertKeys()}/{@code HashIndex.convertKeys()} call {@code Type.convert()} directly with no
   * catch guard - unlike {@code QueryOperatorEquals}/{@code FetchFromIndexStep}, which already defend against the new
   * exception. The reviewer asked to confirm that an index rebuild, which re-reads already-stored property values,
   * can never hand one of these paths an out-of-range raw value.
   * <p>
   * It can't: an INTEGER/SHORT/BYTE property's on-disk representation is a fixed-width primitive, so a value that
   * reached storage is by construction already in range for its declared type - there is no way to persist an
   * out-of-range value in the first place (that is exactly the bug #5905 fixed). This test locks that invariant in
   * for both index engines by round-tripping the boundary values through {@code REBUILD INDEX}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5967 rebuild survives boundary integer values")
  class BoundaryIntegerValueRebuildTest extends TestHelper {

    @Test
    void lsmTreeIndexRebuildSurvivesBoundaryIntegerValues() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE V");
        database.command("sql", "CREATE PROPERTY V.n INTEGER");
        database.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
        database.command("sql", "INSERT INTO V SET n = " + Integer.MAX_VALUE);
        database.command("sql", "INSERT INTO V SET n = " + Integer.MIN_VALUE);
        database.command("sql", "INSERT INTO V SET n = 0");
      });

      database.transaction(() -> database.command("sql", "REBUILD INDEX `V[n]`"));

      database.transaction(() -> {
        try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MAX_VALUE)) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MAX_VALUE);
        }
        try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MIN_VALUE)) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MIN_VALUE);
        }
      });
    }

    @Test
    void hashIndexRebuildSurvivesBoundaryIntegerValues() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE V");
        database.command("sql", "CREATE PROPERTY V.n INTEGER");
        database.command("sql", "CREATE INDEX ON V (n) UNIQUE_HASH");
        database.command("sql", "INSERT INTO V SET n = " + Integer.MAX_VALUE);
        database.command("sql", "INSERT INTO V SET n = " + Integer.MIN_VALUE);
      });

      database.transaction(() -> database.command("sql", "REBUILD INDEX `V[n]`"));

      database.transaction(() -> {
        try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MAX_VALUE)) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MAX_VALUE);
        }
        try (final var rs = database.query("sql", "SELECT FROM V WHERE n = " + Integer.MIN_VALUE)) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(Integer.MIN_VALUE);
        }
      });
    }
  }
}
