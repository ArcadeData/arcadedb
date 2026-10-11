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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #9633: under BINARY quantization the grouped search ({@code groupBy} on {@code vector.neighbors}) skipped the
 * rerank on the stored vectors that the plain search applies (#8960). It returned the graph's distances, computed on a
 * one-bit reconstruction of each vector, so recall halved, a record queried by its own vector reported a non-zero
 * distance, and a {@code maxDistance} bound that kept the exact match without {@code groupBy} dropped it with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9633BinaryGroupedRerankTest extends TestHelper {
  private static final int DIM     = 32;
  private static final int RECORDS = 1000;

  private final Map<RID, float[]> vectors = new HashMap<>();
  private final Map<RID, Integer> pids    = new HashMap<>();

  @Test
  void groupedSearchReturnsTheExactSelfMatchThroughSql() {
    createCorpus(500, 1);
    final float[] query = vectorOfPid(42);

    final List<Result> plain = query("SELECT expand(`vector.neighbors`('P[embedding]', ?, 3))", query);
    final List<Result> grouped = query(
        "SELECT expand(`vector.neighbors`('P[embedding]', ?, 3, { groupBy: 'grp', groupSize: 1 }))", query);

    assertThat(plain.getFirst().<Integer>getProperty("pid")).isEqualTo(42);
    assertThat(grouped.getFirst().<Integer>getProperty("pid")).isEqualTo(42);
    assertThat(grouped.getFirst().<Float>getProperty("distance")).isCloseTo(0f, within(1e-5f));
    for (int i = 0; i < plain.size(); i++) {
      assertThat(grouped.get(i).<Integer>getProperty("pid")).isEqualTo(plain.get(i).<Integer>getProperty("pid"));
      assertThat(grouped.get(i).<Float>getProperty("distance")).isCloseTo(plain.get(i).<Float>getProperty("distance"),
          within(1e-5f));
    }

    final List<Result> bounded = query(
        "SELECT expand(`vector.neighbors`('P[embedding]', ?, 3, { groupBy: 'grp', groupSize: 1, maxDistance: 0.01 }))",
        query);
    assertThat(bounded).hasSize(1);
    assertThat(bounded.getFirst().<Integer>getProperty("pid")).isEqualTo(42);
  }

  /**
   * One group per record and {@code groupSize = 1}, so the grouped answer must be the plain top-k: grouping by itself
   * removes nothing, and the two paths must reach the same recall and report the same, exact, distances.
   */
  @Test
  void groupedRecallMatchesThePlainSearch() {
    createCorpus(RECORDS, 1);
    final LSMVectorIndex index = bucketIndex();
    final Function<RID, Object> groupOf = rid -> rid.asDocument().get("grp");

    final Random random = new Random(9633);
    final int k = 10;
    double plainRecall = 0;
    double groupedRecall = 0;
    final int queries = 50;
    for (int q = 0; q < queries; q++) {
      final float[] query = unit(random);
      final Set<RID> truth = bruteForce(query, k);

      final List<Pair<RID, Float>> plain = index.findNeighborsFromVector(query, k, 128, null);
      final List<Pair<RID, Float>> grouped = index.findNeighborsFromVectorGrouped(query, k, 1, 128, null, groupOf);

      plainRecall += overlap(plain, truth);
      groupedRecall += overlap(grouped, truth);

      assertThat(grouped).hasSize(k);
      float previous = Float.NEGATIVE_INFINITY;
      for (final Pair<RID, Float> row : grouped) {
        assertThat(row.getSecond()).as("the distance must be the exact one").isCloseTo(exact(query, row.getFirst()), within(1e-3f));
        assertThat(row.getSecond()).isGreaterThanOrEqualTo(previous);
        previous = row.getSecond();
      }
    }
    plainRecall /= queries * k;
    groupedRecall /= queries * k;

    assertThat(groupedRecall).as("grouped recall %s against plain recall %s", groupedRecall, plainRecall)
        .isGreaterThanOrEqualTo(plainRecall - 0.02);
  }

  /**
   * Groups with several members: every row carries its exact distance, no group exceeds its cap, and the members of each
   * group are its nearest ones among the rows reranked.
   */
  @Test
  void groupedSearchWithSharedGroupsAppliesTheCapOnExactDistances() {
    createCorpus(RECORDS, 50);
    final LSMVectorIndex index = bucketIndex();
    final Function<RID, Object> groupOf = rid -> rid.asDocument().get("grp");

    final Random random = new Random(42);
    for (int q = 0; q < 20; q++) {
      final float[] query = unit(random);
      final List<Pair<RID, Float>> grouped = index.findNeighborsFromVectorGrouped(query, 5, 3, 128, null, groupOf);

      assertThat(grouped).hasSize(15);
      final Map<Object, Integer> perGroup = new HashMap<>();
      float previous = Float.NEGATIVE_INFINITY;
      for (final Pair<RID, Float> row : grouped) {
        assertThat(row.getSecond()).isCloseTo(exact(query, row.getFirst()), within(1e-3f));
        assertThat(row.getSecond()).isGreaterThanOrEqualTo(previous);
        previous = row.getSecond();
        perGroup.merge(groupOf.apply(row.getFirst()), 1, Integer::sum);
      }
      assertThat(perGroup).hasSize(5);
      assertThat(perGroup.values()).allMatch(count -> count == 3);
    }

    // The restricted search of a multi-bucket merge (issue #8002) is reranked too: its bound is an exact distance
    final float[] self = vectorOfPid(7);
    final Object group = groupOf.apply(ridOfPid(7));
    final List<Pair<RID, Float>> restricted = index.findNeighborsFromVectorForGroups(self, Set.of(group), 2, 128, null, groupOf,
        0.01f);
    assertThat(restricted).hasSize(1);
    assertThat(restricted.getFirst().getFirst()).isEqualTo(ridOfPid(7));
    assertThat(restricted.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
  }

  /**
   * The members that hold a group's slots are its nearest on the stored vectors, not on the graph's approximation. Every
   * member of the near group has the sign pattern of the query, so the one-bit codes cannot tell them apart and the graph
   * hands them over in an order that has nothing to do with their real distance. They are inserted farthest first, so a
   * cap applied in that order keeps the wrong two.
   */
  @Test
  void theNearestStoredVectorsWinTheGroupCap() {
    database.command("sql", "CREATE DOCUMENT TYPE Q BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Q.grp STRING");
    database.command("sql", "CREATE PROPERTY Q.rank INTEGER");
    database.command("sql", "CREATE PROPERTY Q.embedding ARRAY_OF_FLOATS");

    final Random random = new Random(9633);
    final float[] query = new float[DIM];
    for (int i = 0; i < DIM; i++)
      query[i] = 1f + random.nextFloat();

    final int members = 8;
    database.transaction(() -> {
      // The near group: the query plus noise that grows with the rank and never flips a sign. Farthest first
      for (int rank = members; rank >= 1; rank--) {
        final float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++)
          v[i] = query[i] + (random.nextFloat() - 0.5f) * 0.1f * rank;
        database.newDocument("Q").set("grp", "near", "rank", rank, "embedding", v).save();
      }
      // Far groups: mixed signs, so they never compete with the near group
      for (int g = 0; g < 30; g++)
        for (int m = 0; m < 3; m++)
          database.newDocument("Q").set("grp", "far" + g, "rank", 0, "embedding", unit(random)).save();
    });
    database.command("sql", "CREATE INDEX ON Q (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIM
        + ", \"similarity\": \"EUCLIDEAN\", \"beamWidth\": 100, \"quantization\": \"BINARY\" }");

    final List<Integer> ranks = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT rank FROM (SELECT expand(`vector.neighbors`('Q[embedding]', ?, 1, { groupBy: 'grp', groupSize: 2 })))", query)) {
      while (rs.hasNext())
        ranks.add(rs.next().getProperty("rank"));
    }
    assertThat(ranks).containsExactly(1, 2);
  }

  private void createCorpus(final int records, final int groupModulo) {
    database.command("sql", "CREATE DOCUMENT TYPE P BUCKETS 1");
    database.command("sql", "CREATE PROPERTY P.pid INTEGER");
    database.command("sql", "CREATE PROPERTY P.grp INTEGER");
    database.command("sql", "CREATE PROPERTY P.embedding ARRAY_OF_FLOATS");

    final Random random = new Random(records);
    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final float[] v = unit(random);
        final Document doc = database.newDocument("P").set("pid", i, "grp", groupModulo == 1 ? i : i % groupModulo, "embedding", v)
            .save();
        vectors.put(doc.getIdentity(), v);
        pids.put(doc.getIdentity(), i);
      }
    });
    database.command("sql", "CREATE INDEX ON P (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIM
        + ", \"similarity\": \"EUCLIDEAN\", \"beamWidth\": 100, \"quantization\": \"BINARY\" }");
  }

  private LSMVectorIndex bucketIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("P[embedding]")).getIndexesOnBuckets()[0];
  }

  private List<Result> query(final String sql, final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private RID ridOfPid(final int pid) {
    for (final Map.Entry<RID, Integer> entry : pids.entrySet())
      if (entry.getValue() == pid)
        return entry.getKey();
    throw new IllegalArgumentException("No record with pid " + pid);
  }

  private float[] vectorOfPid(final int pid) {
    return vectors.get(ridOfPid(pid));
  }

  private Set<RID> bruteForce(final float[] query, final int k) {
    final List<Map.Entry<RID, float[]>> all = new ArrayList<>(vectors.entrySet());
    all.sort((a, b) -> Double.compare(l2(query, a.getValue()), l2(query, b.getValue())));
    final Set<RID> top = new HashSet<>();
    for (int i = 0; i < k; i++)
      top.add(all.get(i).getKey());
    return top;
  }

  private static int overlap(final List<Pair<RID, Float>> answer, final Set<RID> truth) {
    int hits = 0;
    for (final Pair<RID, Float> row : answer)
      if (truth.contains(row.getFirst()))
        ++hits;
    return hits;
  }

  /** The distance the index reports under EUCLIDEAN, computed on the stored vector: the squared L2 distance. */
  private float exact(final float[] query, final RID rid) {
    return (float) l2(query, vectors.get(rid));
  }

  private static double l2(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      final double d = a[i] - b[i];
      sum += d * d;
    }
    return sum;
  }

  private static float[] unit(final Random r) {
    final float[] v = new float[DIM];
    double s = 0;
    for (int i = 0; i < DIM; i++) {
      v[i] = (float) r.nextGaussian();
      s += v[i] * v[i];
    }
    s = Math.sqrt(s);
    for (int i = 0; i < DIM; i++)
      v[i] /= (float) s;
    return v;
  }
}
