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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5748: the brute-force fallback used to walk every ordinal in the index and test each one for allow-list
 * membership. An allow-list narrower than {@code k} can never satisfy the caller's expected-result threshold, so the
 * fallback fires on every single filtered query and each one pays a full index scan. The scan now resolves the
 * allow-list to its ordinals and reads only those vectors.
 * <p>
 * The optimization is only allowed to be faster, never different, so the tests here compare the two paths on the same
 * inputs rather than checking the filtered answer in isolation:
 * <ul>
 *   <li>{@link #theAllowListWalkReturnsExactlyWhatTheFullScanWouldHaveReturned} is the parity assertion, RID for RID,
 *       distance for distance, in the same order and with the same truncation at k.</li>
 *   <li>{@link #theAllowListWalkTouchesOnlyTheAllowedEntries} is the win itself, counted rather than timed: the same
 *       query resolves one location per allowed RID instead of one per ordinal in the index.</li>
 *   <li>{@link #anAllowedRidWhoseVectorCannotBeReadIsNotScored} pins the guard the rebase nearly lost. A location can
 *       say "live" while the read still fails, and {@code getVector} answers a placeholder rather than null in that
 *       case, so a {@code vec == null} check alone lets the placeholder be scored as if it were a real vector
 *       (issue #5558).</li>
 *   <li>{@link #anAllowListWiderThanTheIndexStillFilters} covers the crossover: past a certain width the plain scan is
 *       the cheaper way to answer, and taking it must not drop the filter.</li>
 *   <li>{@link #anAllowedRidWithNoLiveOrdinalIsSkippedOnBothPaths} covers the entries the walk cannot resolve at all,
 *       which is where an equivalence between "every ordinal, filtered" and "the allow-list, resolved" can quietly
 *       stop holding.</li>
 * </ul>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue5748AllowListBruteForceScanTest extends TestHelper {

  private static final VectorTypeSupport VTS        = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int               DIMENSIONS = 8;
  private static final int               VERTICES   = 40;
  private static final int               K          = 10;

  @Test
  void theAllowListWalkReturnsExactlyWhatTheFullScanWouldHaveReturned() throws Exception {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(7);

    // Three RIDs scattered across the ordinal map so the answer is not the first few ordinals by accident.
    final Set<RID> allowed = ridsOf(3, 19, 31);

    final List<Pair<RID, Float>> filtered = scan(index, query, K, allowed, ordinalMap);

    // The reference: scan everything with no allow-list at all, then apply the filter and the truncation by hand.
    // Anything the walk gets wrong - a missing RID, a RID resolved to the wrong ordinal, a different tie break -
    // shows up as a difference against this list.
    final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);
    final List<Pair<RID, Float>> reference = new ArrayList<>();
    for (final Pair<RID, Float> candidate : everything)
      if (allowed.contains(candidate.getFirst()) && reference.size() < K)
        reference.add(candidate);

    assertThat(reference).as("the fixture has to leave something for the comparison to be about").hasSize(3);
    assertThat(ridsIn(filtered)).as("the allow-list walk must return the same RIDs, in the same order")
        .isEqualTo(ridsIn(reference));
    assertThat(distancesIn(filtered)).as("and the same distances, computed from the same vectors")
        .isEqualTo(distancesIn(reference));
  }

  @Test
  void theAllowListWalkTruncatesAtKLikeTheFullScan() throws Exception {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(0);

    // More allowed RIDs than k, so truncation is what decides the answer rather than availability.
    final Set<RID> allowed = ridsOf(1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 2, 6);
    final int k = 4;

    final List<Pair<RID, Float>> filtered = scan(index, query, k, allowed, ordinalMap);

    final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);
    final List<Pair<RID, Float>> reference = new ArrayList<>();
    for (final Pair<RID, Float> candidate : everything)
      if (allowed.contains(candidate.getFirst()) && reference.size() < k)
        reference.add(candidate);

    assertThat(filtered).as("k caps the filtered answer exactly as it caps the full scan").hasSize(k);
    assertThat(ridsIn(filtered)).isEqualTo(ridsIn(reference));
    assertThat(distancesIn(filtered)).isEqualTo(distancesIn(reference));
  }

  /**
   * Two allowed vectors at the same distance, and only room for one. Which one survives the truncation is decided by
   * the order the two were scored in, so the walk has to score in ordinal order like the full scan does - the
   * allow-list's own iteration order is not the answer. The allow-list here is deliberately ordered against the
   * ordinals to make the difference visible rather than incidental.
   */
  @Test
  void tiedDistancesTruncateToTheSameRidAsTheFullScan() throws Exception {
    createSchemaAndData();
    // Two more documents sharing one embedding, inserted in a known order so the earlier one holds the lower ordinal.
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "twinA", embedding(500));
      database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "twinB", embedding(500));
      // A trailing document, because the most recent insert stays in the delta buffer and out of the ordinal map,
      // and both twins have to be in the map for the scan to see the tie at all.
      database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "tail", embedding(501));
    });
    vectorIndex().buildVectorGraphNow();

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(500);

    final Set<RID> allowed = new LinkedHashSet<>();
    allowed.add(ridOf("twinB"));
    allowed.add(ridOf("twinA"));

    final List<Pair<RID, Float>> filtered = scan(index, query, 1, allowed, ordinalMap);
    final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);

    assertThat(everything.get(0).getSecond()).as("the fixture must really produce a tie")
        .isEqualTo(everything.get(1).getSecond());

    final List<Pair<RID, Float>> reference = new ArrayList<>();
    for (final Pair<RID, Float> candidate : everything)
      if (allowed.contains(candidate.getFirst()) && reference.isEmpty())
        reference.add(candidate);

    assertThat(ridsIn(filtered)).as("a tie must break the same way on both paths").isEqualTo(ridsIn(reference));
  }

  /**
   * The win itself, counted rather than timed so it cannot flake on a loaded runner.
   * <p>
   * What the full scan spends on a filtered query is one location lookup per ordinal - a boxed key into a map the
   * size of the index, for every vector it is about to reject - and the vector reads were already limited to the
   * allowed RIDs, because the membership test comes before the read on both paths. So the lookup count is the
   * measure of this change, and it goes from "the whole index" to "the allow-list".
   */
  @Test
  void theAllowListWalkTouchesOnlyTheAllowedEntries() throws Exception {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(7);
    final Set<RID> allowed = ridsOf(3, 19, 31);

    final VectorLocationIndex original = index.getVectorIndex();
    final CountingLocationIndex locations = installLocationIndex(index, new CountingLocationIndex(original));
    final List<Pair<RID, Float>> filtered;
    final int filteredLookups;
    final int fullLookups;
    try {
      locations.lookups = 0;
      filtered = scan(index, query, K, allowed, ordinalMap);
      filteredLookups = locations.lookups;

      locations.lookups = 0;
      scan(index, query, ordinalMap.length, null, ordinalMap);
      fullLookups = locations.lookups;
    } finally {
      // Put the real map back before teardown: the counting one only forwards reads, it holds no entries of its own.
      installLocationIndex(index, original);
    }

    assertThat(filtered).as("and it still answers the query").hasSize(allowed.size());
    assertThat(filteredLookups)
        .as("the filtered scan resolves the allow-list and nothing else, so its cost follows the allow-list")
        .isLessThanOrEqualTo(allowed.size() * 2);
    assertThat(fullLookups).as("while the unfiltered scan costs one lookup per ordinal, which is the baseline")
        .isGreaterThanOrEqualTo(ordinalMap.length);
    assertThat(filteredLookups).as("and the gap is what issue #5748 is about").isLessThan(fullLookups / 4);
  }

  /**
   * A location that says "live" is not a promise that the vector can be read: a document whose vector property was
   * removed, has the wrong type or reads back as all zeros comes out of {@code getVector} as the deleted placeholder,
   * which is a well-formed vector of the right shape and scores like one. The scan has to recognise it.
   * <p>
   * The fixture makes every read fail at once by pointing the vector values at a property the documents do not have,
   * which is the same code path as a single unreadable document and needs no mutation of the index behind its own
   * back. With the guard removed from the allow-list walk this returns three results carrying a plausible distance.
   */
  @Test
  void anAllowedRidWhoseVectorCannotBeReadIsNotScored() throws Exception {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(7);
    final Set<RID> allowed = ridsOf(3, 19, 31);

    final RandomAccessVectorValues unreadable = new ArcadePageVectorValues((DatabaseInternal) database, DIMENSIONS,
        "thereIsNoSuchProperty", index.getVectorIndex(), ordinalMap, index);

    assertThat(unreadable.getVector(0)).as("the fixture must really produce the placeholder, not a vector")
        .matches(v -> ((ArcadePageVectorValues) unreadable).isDeletedSentinel(v));

    final List<Pair<RID, Float>> filtered = new ArrayList<>();
    invokeBruteForceScan(index, query, K, allowed, filtered, unreadable, ordinalMap);
    assertThat(filtered).as("an allowed RID whose vector cannot be read must not be scored").isEmpty();

    final List<Pair<RID, Float>> full = new ArrayList<>();
    invokeBruteForceScan(index, query, K, null, full, unreadable, ordinalMap);
    assertThat(full).as("and the full scan has to agree, which is what makes the two paths one behaviour").isEmpty();
  }

  /**
   * Past the crossover the allow-list is wider than the index and resolving every entry costs more than walking the
   * ordinals, so the scan takes the plain loop. Taking it must not lose the filter: the answer is still only the
   * allowed RIDs.
   */
  @Test
  void anAllowListWiderThanTheIndexStillFilters() throws Exception {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(7);

    final Set<RID> real = ridsOf(3, 19, 31);
    final Set<RID> wide = new LinkedHashSet<>(real);
    // RIDs of a bucket this index knows nothing about, purely to push the allow-list past the ordinal count.
    for (int i = 0; wide.size() <= ordinalMap.length; i++)
      wide.add(new RID(9999, i));
    assertThat(wide.size()).as("the allow-list has to be wide enough to take the other branch")
        .isGreaterThan(ordinalMap.length);

    final List<Pair<RID, Float>> filtered = scan(index, query, K, wide, ordinalMap);

    assertThat(ridsIn(filtered)).as("a wide allow-list filters exactly like a narrow one")
        .isEqualTo(ridsIn(scan(index, query, K, real, ordinalMap)));
  }

  /**
   * The allow-list to ordinal mapping has to be total: an entry it cannot resolve must contribute nothing, not a
   * guessed ordinal. Three kinds of unresolvable entry are in the allow-list here - a RID whose vector was tombstoned,
   * which keeps its ordinal in the map but loses its location; a RID ingested after the last graph build, which is
   * only in the delta buffer and has no ordinal at all; and a RID this index never saw. The answer still has to be
   * what the full scan would have filtered down to.
   */
  @Test
  void anAllowedRidWithNoLiveOrdinalIsSkippedOnBothPaths() throws Exception {
    createSchemaAndData();

    final RID tombstoned = ridOf("doc5");
    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc5"));
    database.transaction(
        () -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "afterTheBuild", embedding(600)));

    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
    final float[] query = embedding(7);

    final Set<RID> allowed = new LinkedHashSet<>(ridsOf(3, 19, 31));
    allowed.add(tombstoned);
    final RID onlyInTheDelta = ridOf("afterTheBuild");
    allowed.add(onlyInTheDelta);
    allowed.add(new RID(9999, 0));
    assertThat(allowed.size()).as("the fixture has to stay on the allow-list walk").isLessThan(ordinalMap.length);

    final List<Pair<RID, Float>> filtered = scan(index, query, K, allowed, ordinalMap);

    final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);
    final List<Pair<RID, Float>> reference = new ArrayList<>();
    for (final Pair<RID, Float> candidate : everything)
      if (allowed.contains(candidate.getFirst()) && reference.size() < K)
        reference.add(candidate);

    assertThat(reference).as("only the three live indexed RIDs can be answered at all").hasSize(3);
    assertThat(ridsIn(filtered)).as("an unresolvable allow-list entry contributes nothing, on either path")
        .isEqualTo(ridsIn(reference));
    assertThat(ridsIn(filtered)).doesNotContain(tombstoned, onlyInTheDelta);
  }

  /**
   * The reverse lookup is a binary search, so the ordinal map has to be sorted. Every producer sorts it because the
   * ordinals have to line up with the order the graph was persisted in; this pins that invariant where the search
   * depends on it, since breaking it would not throw, it would silently return fewer neighbors.
   */
  @Test
  void theOrdinalMapIsSortedAscending() {
    createSchemaAndData();

    final int[] ordinalMap = vectorIndex().getOrdinalToVectorIdForTest();
    assertThat(ordinalMap).as("the fixture must have built a graph").isNotEmpty();
    assertThat(ordinalMap).isSorted();
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchemaAndData() {
    // No rebuild may run mid-test: the ordinal map has to stay the one the assertions captured. Both settings are
    // database-scoped, so they are set on this database rather than globally - the inactivity timer is process-wide
    // and disarming it here would follow the JVM into every later test in the module.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      // No quantization: the vectors are read back from the documents, which is what lets the unreadable-vector test
      // fail the read by naming a property that does not exist.
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("EUCLIDEAN")
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });

    vectorIndex().buildVectorGraphNow();
  }

  private List<Pair<RID, Float>> scan(final LSMVectorIndex index, final float[] query, final int k,
      final Set<RID> allowedRIDs, final int[] ordinalMap) throws Exception {
    final List<Pair<RID, Float>> results = new ArrayList<>();
    invokeBruteForceScan(index, query, k, allowedRIDs, results, vectorValues(ordinalMap), ordinalMap);
    return results;
  }

  private void invokeBruteForceScan(final LSMVectorIndex index, final float[] query, final int k,
      final Set<RID> allowedRIDs, final List<Pair<RID, Float>> results, final RandomAccessVectorValues vectors,
      final int[] ordinalMap) throws Exception {
    final Method bruteForceScan = LSMVectorIndex.class.getDeclaredMethod("bruteForceScan", VectorFloat.class, int.class,
        Set.class, List.class, RandomAccessVectorValues.class, int[].class);
    bruteForceScan.setAccessible(true);
    bruteForceScan.invoke(index, VTS.createFloatVector(query), k, allowedRIDs, results, vectors, ordinalMap);
  }

  /**
   * Replaces the index's location map with one that counts its lookups. The instance is only swapped out by a
   * compaction, which this fixture never triggers, so the counter installed here is the one the scan will use.
   */
  private <T extends VectorLocationIndex> T installLocationIndex(final LSMVectorIndex index, final T locations)
      throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("vectorIndex");
    field.setAccessible(true);
    field.set(index, locations);
    return locations;
  }

  /**
   * The page-backed reader the index itself would use. It has to be that class and not a decorator around it: the
   * scan recognises {@link ArcadePageVectorValues} to apply its deleted-placeholder guard, and a wrapper would
   * silently turn the guard off and move the test onto a different code path.
   */
  private ArcadePageVectorValues vectorValues(final int[] ordinalMap) {
    return new ArcadePageVectorValues((DatabaseInternal) database, DIMENSIONS, "embedding",
        vectorIndex().getVectorIndex(), ordinalMap, vectorIndex());
  }

  private Set<RID> ridsOf(final int... ids) {
    final Set<RID> rids = new HashSet<>();
    for (final int id : ids)
      rids.add(ridOf("doc" + id));
    return rids;
  }

  private RID ridOf(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private static List<RID> ridsIn(final List<Pair<RID, Float>> results) {
    final List<RID> rids = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      rids.add(r.getFirst());
    return rids;
  }

  private static List<Float> distancesIn(final List<Pair<RID, Float>> results) {
    final List<Float> distances = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      distances.add(r.getSecond());
    return distances;
  }

  private LSMVectorIndex vectorIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  /** Deterministic, distinct, non-zero vectors, well spread so no two of them tie on distance. */
  private static float[] embedding(final int i) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(i * 0.37 + j * 0.91) + 2.0f + i * 0.013f;
    return vector;
  }

  /** Counts the location lookups the scan performs, delegating the answers to the real map. */
  private static final class CountingLocationIndex extends VectorLocationIndex {
    private final VectorLocationIndex delegate;
    private       int                 lookups;

    CountingLocationIndex(final VectorLocationIndex delegate) {
      this.delegate = delegate;
    }

    @Override
    public VectorLocation getLocation(final int vectorId) {
      lookups++;
      return delegate.getLocation(vectorId);
    }

    @Override
    public int[] getVectorIdsForRid(final RID rid) {
      return delegate.getVectorIdsForRid(rid);
    }
  }
}
