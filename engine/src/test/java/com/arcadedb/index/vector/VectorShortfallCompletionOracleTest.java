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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The brute-force scan that completes a search whose graph walk came up short (issue #3722) starts from the rows the walk found: it has to
 * add the rows the walk did not reach, once each, within the allow-list, and without the committed rows the calling transaction removed or
 * re-embedded (issue #7378). The oracle is the exact order of every live vector by distance to the query. The scan is called with an answer
 * already holding rows, as the shortfall completion does, because a graph walk of a test index is never short in a way a query can force.
 */
class VectorShortfallCompletionOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int TOTAL      = 60;

  private Map<RID, float[]>        vectors;
  private LSMVectorIndex           index;
  private int[]                    ordinalMap;
  private RandomAccessVectorValues values;

  @BeforeEach
  void configure() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);
  }

  /** An index of {@link #TOTAL} random vectors of a new type, its graph built, and the state a scan of it is given. */
  private void buildIndex(final String type, final String quantization) throws Exception {
    vectors = new HashMap<>();
    database.transaction(() -> {
      database.getSchema().createVertexType(type).createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex(type, new String[] { "vector" }).withLSMVectorType().withDimensions(DIMENSIONS)
          .withSimilarity("EUCLIDEAN").withQuantization(VectorQuantizationType.valueOf(quantization)).withMaxConnections(16)
          .withBeamWidth(100).create();
    });
    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < TOTAL; i++) {
        final float[] vector = randomVector(random);
        final MutableVertex vertex = database.newVertex(type).set("vector", vector);
        vertex.save();
        vectors.put(vertex.getIdentity(), vector);
      }
    });
    index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[vector]")).getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    ordinalMap = ((int[]) readField(index, "ordinalToVectorId")).clone();
    values = ArcadePageVectorValues.forSearch((DatabaseInternal) database, DIMENSIONS, "vector",
        readField(index, "residentLocations"), ordinalMap, index);
    assertThat(ordinalMap).hasSize(TOTAL);
  }

  @Test
  void aPartialAnswerIsCompletedWithTheRowsItDoesNotHoldYet() throws Exception {
    buildIndex("Sf", "NONE");
    final float[] query = randomVector(new Random(101));
    final List<Pair<RID, Float>> partial = new ArrayList<>();
    scan(query, 3, null, partial, null);
    assertThat(rids(partial)).isEqualTo(oracle(query, vectors.keySet(), 3));

    scan(query, 10, null, partial, null);

    assertThat(rids(partial)).as("the exact top 10, in order, no row twice").isEqualTo(oracle(query, vectors.keySet(), 10));
    assertThat(new HashSet<>(rids(partial))).hasSize(10);
  }

  @Test
  void anAllowListOfTheIndexIsKeptWhetherItIsWalkedOrWiderThanTheIndex() throws Exception {
    buildIndex("Sf", "NONE");
    final float[] query = randomVector(new Random(202));
    final List<RID> all = new ArrayList<>(vectors.keySet());

    // narrower than the index: its members are resolved to ordinals one by one
    final Set<RID> narrow = new HashSet<>(all.subList(0, 15));
    final List<Pair<RID, Float>> walked = new ArrayList<>();
    scan(query, 2, narrow, walked, null);
    scan(query, 8, narrow, walked, null);
    assertThat(rids(walked)).isEqualTo(oracle(query, narrow, 8));
    assertThat(new HashSet<>(rids(walked))).hasSize(8);

    // wider than the index (members of it and RIDs it never held): every ordinal is scored and the filter is what keeps the answer inside the list
    final Set<RID> wide = new HashSet<>(all.subList(0, 30));
    for (int i = 0; i < TOTAL; i++)
      wide.add(new RID(all.getFirst().getBucketId(), 1_000_000L + i));
    assertThat(wide.size()).isGreaterThan(ordinalMap.length);
    final List<Pair<RID, Float>> scanned = new ArrayList<>();
    scan(query, 2, wide, scanned, null);
    scan(query, 8, wide, scanned, null);
    assertThat(rids(scanned)).isEqualTo(oracle(query, all.subList(0, 30), 8));
    assertThat(new HashSet<>(rids(scanned))).hasSize(8);
  }

  /**
   * An INT8 index scores from the codes of its own pages, which a transaction's change to a record does not touch: the committed vector of a
   * removed or rewritten row is still there to be scored, and only the overlay keeps it out of the answer.
   */
  @Test
  void theRowsATransactionRemovedOrRewroteAreLeftToItsOwnVersion() throws Exception {
    buildIndex("SfQ", "INT8");
    final List<RID> rows = new ArrayList<>(vectors.keySet());
    final RID removed = rows.get(0);
    final RID rewritten = rows.get(1);
    final float[] query = vectors.get(removed);

    database.begin();
    try {
      database.deleteRecord(database.lookupByRID(removed, true));
      database.lookupByRID(rewritten, true).asVertex().modify().set("vector", farVector()).save();
      final TransactionVectorOverlay overlay = TransactionVectorOverlay.open((DatabaseInternal) database, index,
          VectorizationProvider.getInstance().getVectorTypeSupport(), 1_000);
      assertThat(overlay).as("the transaction wrote to this index").isNotNull();

      final List<Pair<RID, Float>> without = new ArrayList<>();
      scan(query, TOTAL, null, without, null);
      assertThat(rids(without)).as("without the overlay the committed vectors are still scored").contains(removed, rewritten);

      final List<Pair<RID, Float>> results = new ArrayList<>();
      scan(query, TOTAL, null, results, overlay);

      final Set<RID> expected = new HashSet<>(rows);
      expected.remove(removed);
      expected.remove(rewritten);
      assertThat(rids(results)).as("every other row once, and not the committed vectors of the removed and the rewritten row")
          .hasSize(expected.size()).containsExactlyInAnyOrderElementsOf(expected);
    } finally {
      database.rollback();
    }
  }

  private void scan(final float[] query, final int k, final Set<RID> allowed, final List<Pair<RID, Float>> results,
      final TransactionVectorOverlay overlay) throws Exception {
    final VectorFloat<?> queryVector = VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(query);
    final Method scan = LSMVectorIndex.class.getDeclaredMethod("bruteForceScan", VectorFloat.class, int.class, Set.class, List.class,
        RandomAccessVectorValues.class, int[].class, TransactionVectorOverlay.class, WorkGuard.class);
    scan.setAccessible(true);
    scan.invoke(index, queryVector, k, allowed, results, values, ordinalMap, overlay, WorkGuard.forCommandDeadline(null));
  }

  /** The {@code k} nearest of {@code candidates} by squared Euclidean distance, nearest first. */
  private List<RID> oracle(final float[] query, final Iterable<RID> candidates, final int k) {
    final List<RID> sorted = new ArrayList<>();
    candidates.forEach(sorted::add);
    sorted.sort((a, b) -> Double.compare(squaredDistance(query, vectors.get(a)), squaredDistance(query, vectors.get(b))));
    return new ArrayList<>(sorted.subList(0, Math.min(k, sorted.size())));
  }

  private static double squaredDistance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++)
      sum += (double) (a[i] - b[i]) * (a[i] - b[i]);
    return sum;
  }

  private static List<RID> rids(final List<Pair<RID, Float>> results) {
    final List<RID> rids = new ArrayList<>();
    for (final Pair<RID, Float> result : results)
      rids.add(result.getFirst().getIdentity());
    return rids;
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat();
    return vector;
  }

  private static float[] farVector() {
    final float[] vector = new float[DIMENSIONS];
    Arrays.fill(vector, 100f);
    return vector;
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(final Object target, final String name) throws Exception {
    final Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(target);
  }
}
