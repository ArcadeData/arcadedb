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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a search inside an open transaction answers, against an exact cosine oracle over the rows the transaction sees: the
 * committed ones minus what it deleted or re-embedded, plus what it wrote. The graph is frozen over 1100 vectors (no rebuild runs),
 * so the committed side is answered by the graph walk, the allow-list plan or the delta buffer, as the query decides.
 * <p>
 * Each test pins a part of the overlay the unit tests of {@link VectorTransactionSearchTest} leave unobserved: a row deleted
 * by the transaction reaching the allow-list plan, a re-embedded row answering with its old vector, the rows of a transaction on an
 * index that holds nothing else, and a merge of more pending rows than the committed buffer holds.
 */
class VectorTransactionSearchOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int COMMITTED  = 1100;

  private final Random                rnd   = new Random(7378);
  /** The committed rows by RID. */
  private final Map<RID, float[]>     model = new LinkedHashMap<>();

  @BeforeEach
  void freezeTheGraph() {
    // no rebuild between the writes of a test: the graph keeps the 1100 vectors it was built over
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void thawTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  @Test
  void aRowTheTransactionDeletedIsNotAnswered() {
    createTypeWithGraph();
    final RID nearest = nearestTo(model, centerOf(7));

    database.begin();
    database.lookupByRID(nearest, true).delete();
    final Map<RID, float[]> seen = new LinkedHashMap<>(model);
    seen.remove(nearest);

    // the graph walk: the deleted row is the nearest of the committed index and must not come back (the nearest of the rest does)
    assertThat(neighbours(centerOf(7), 5, null)).as("walk").hasSize(5).doesNotContain(nearest).first()
        .isEqualTo(exact(seen, centerOf(7), 1, null).getFirst());
    // a short allow-list holding it: the plan scores the allow-list directly, and has to drop the row too
    final List<RID> allowList = allowListWith(nearest, 6);
    assertThat(neighbours(centerOf(7), 10, allowList)).as("allow-list plan").isEqualTo(exact(seen, centerOf(7), 10, allowList))
        .doesNotContain(nearest);
    // the index itself, below the record lookup that hides a deleted row from the SQL answer
    assertThat(indexNeighbours(centerOf(7), 5, null)).as("index walk").hasSize(5).doesNotContain(nearest);
    assertThat(indexNeighbours(centerOf(7), 10, new HashSet<>(allowList))).as("index allow-list plan").hasSize(5).doesNotContain(nearest);
    database.rollback();

    // nothing of it stays after the rollback
    assertThat(neighbours(centerOf(7), 1, null)).containsExactly(nearest);
  }

  @Test
  void aRowTheTransactionReEmbeddedAnswersWithItsNewVectorOnly() {
    createTypeWithGraph();
    final float[] oldVector = centerOf(3);
    final RID row = nearestTo(model, oldVector);
    final float[] newVector = farFrom(oldVector);

    database.begin();
    database.lookupByRID(row, true).asVertex().modify().set("embedding", newVector).save();
    final Map<RID, float[]> seen = new LinkedHashMap<>(model);
    seen.put(row, newVector);

    assertThat(neighbours(oldVector, 5, null)).as("old vector").hasSize(5).doesNotContain(row).first()
        .isEqualTo(exact(seen, oldVector, 1, null).getFirst());
    assertThat(neighbours(newVector, 5, null)).as("new vector").hasSize(5).first().isEqualTo(row);
    final List<RID> allowList = allowListWith(row, 6);
    assertThat(neighbours(oldVector, 10, allowList)).as("old vector, allow-list").isEqualTo(exact(seen, oldVector, 10, allowList));
    assertThat(neighbours(newVector, 10, allowList)).as("new vector, allow-list").isEqualTo(exact(seen, newVector, 10, allowList))
        .startsWith(row);
    database.rollback();

    assertThat(neighbours(oldVector, 1, null)).containsExactly(row);
  }

  @Test
  void aRowWhoseEmbeddingTheTransactionRemovedIsNotAnswered() {
    createTypeWithGraph();
    final RID row = nearestTo(model, centerOf(5));

    database.begin();
    final MutableDocument withoutEmbedding = database.lookupByRID(row, true).asVertex().modify();
    withoutEmbedding.remove("embedding");
    withoutEmbedding.save();
    final Map<RID, float[]> seen = new LinkedHashMap<>(model);
    seen.remove(row);

    assertThat(neighbours(centerOf(5), 5, null)).hasSize(5).doesNotContain(row).first()
        .isEqualTo(exact(seen, centerOf(5), 1, null).getFirst());
    final List<RID> allowList = allowListWith(row, 4);
    assertThat(neighbours(centerOf(5), 10, allowList)).isEqualTo(exact(seen, centerOf(5), 10, allowList)).doesNotContain(row);
    database.rollback();
  }

  @Test
  void anIndexThatHoldsNothingElseAnswersTheRowsItsTransactionWrote() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\"}");
    });

    database.begin();
    final Map<RID, float[]> seen = new LinkedHashMap<>();
    for (int i = 0; i < 3; i++) {
      final float[] vector = randomVector();
      seen.put(database.newVertex("Doc").set("embedding", vector).save().getIdentity(), vector);
    }
    final float[] query = randomVector();
    // k above the number of rows: the answer is every row the transaction wrote, nearest first
    assertThat(neighbours(query, 10, null)).hasSize(3).isEqualTo(exact(seen, query, 10, null));
    database.rollback();
    assertThat(neighbours(query, 10, null)).isEmpty();
  }

  @Test
  void moreRowsThanTheCommittedBufferHoldsAreMergedWithIt() {
    createTypeWithGraph();
    // one committed row after the graph: the delta buffer holds it
    final float[] late = randomVector();
    database.transaction(() -> model.put(database.newVertex("Doc").set("id", "late").set("embedding", late).save().getIdentity(), late));

    database.begin();
    final Map<RID, float[]> seen = new LinkedHashMap<>(model);
    // the transaction supersedes the buffered row and writes more rows than the buffer holds
    final RID lateRid = nearestTo(model, late);
    final float[] reEmbedded = randomVector();
    database.lookupByRID(lateRid, true).asVertex().modify().set("embedding", reEmbedded).save();
    seen.put(lateRid, reEmbedded);
    for (int i = 0; i < 5; i++) {
      final float[] vector = randomVector();
      seen.put(database.newVertex("Doc").set("id", "pending" + i).set("embedding", vector).save().getIdentity(), vector);
    }
    // an allow-list of all the rows the transaction sees as new or rewritten, and a few committed ones: scored directly
    final List<RID> allowList = new ArrayList<>(allowListWith(lateRid, 4));
    for (final RID rid : seen.keySet())
      if (!model.containsKey(rid))
        allowList.add(rid);
    for (int q = 0; q < 5; q++) {
      final float[] query = randomVector();
      assertThat(neighbours(query, 10, allowList)).isEqualTo(exact(seen, query, 10, allowList));
    }
    database.rollback();
  }

  // ------------------------------------------------------------------------------------------------------------------

  private void createTypeWithGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < COMMITTED; i++) {
        final float[] vector = i < 12 ? centerOf(i) : randomVector();
        model.put(database.newVertex("Doc").set("id", "d" + i).set("embedding", vector).save().getIdentity(), vector);
      }
    });
    // the first search builds the graph over all of them
    assertThat(neighbours(centerOf(0), 3, null)).hasSize(3);
  }

  /** Twelve fixed vectors, each also a committed row: the nearest row to one of them is that row itself. */
  private static float[] centerOf(final int i) {
    final float[] v = new float[DIMENSIONS];
    v[i % DIMENSIONS] = 10f;
    v[(i + 1 + i / DIMENSIONS) % DIMENSIONS] += 3f;
    return v;
  }

  private float[] randomVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  private static float[] farFrom(final float[] v) {
    final float[] far = new float[v.length];
    for (int i = 0; i < v.length; i++)
      far[i] = -v[(i + 3) % v.length] + 0.01f * i;
    return far;
  }

  /**
   * An allow-list of {@code size} rows (well under a fifth of the index, so the plan scores it directly) that holds {@code row}. The
   * rows beside it are random ones: the twelve fixed centers are orthogonal to most queries, which ties their distances.
   */
  private List<RID> allowListWith(final RID row, final int size) {
    final List<RID> allowList = new ArrayList<>();
    allowList.add(row);
    int index = 0;
    for (final RID rid : model.keySet()) {
      if (allowList.size() >= size)
        break;
      if (index++ >= 12 && !rid.equals(row))
        allowList.add(rid);
    }
    return allowList;
  }

  private static double cosineDistance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }

  private static RID nearestTo(final Map<RID, float[]> rows, final float[] query) {
    return exact(rows, query, 1, null).getFirst();
  }

  /** The exact answer: the {@code k} nearest rows of {@code rows} (restricted to {@code allowList} when given), nearest first. */
  private static List<RID> exact(final Map<RID, float[]> rows, final float[] query, final int k, final List<RID> allowList) {
    final Map<RID, Double> distances = new HashMap<>();
    for (final Map.Entry<RID, float[]> e : rows.entrySet())
      if (allowList == null || allowList.contains(e.getKey()))
        distances.put(e.getKey(), cosineDistance(query, e.getValue()));
    final List<RID> ranked = new ArrayList<>(distances.keySet());
    ranked.sort(Comparator.comparing(distances::get));
    return ranked.subList(0, Math.min(k, ranked.size()));
  }

  private List<RID> neighbours(final float[] query, final int k, final List<RID> allowList) {
    final Map<String, Object> params = new HashMap<>();
    params.put("q", query);
    params.put("k", k);
    final String options;
    if (allowList == null)
      options = "{efSearch: 400}";
    else {
      params.put("f", allowList);
      options = "{efSearch: 400, filter: :f}";
    }
    final List<RID> rids = new ArrayList<>();
    final List<Double> distances = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Doc[embedding]', :q, :k, " + options + ")))", params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rids.add(((Identifiable) row.getProperty("rid")).getIdentity());
        distances.add(row.<Number>getProperty("distance").doubleValue());
      }
    }
    for (int i = 1; i < distances.size(); i++)
      assertThat(distances.get(i)).as("ascending").isGreaterThanOrEqualTo(distances.get(i - 1) - 1e-6);
    if (!rids.isEmpty())
      assertThat(distances.getFirst()).isBetween(-1e-3, 2.0 + 1e-3).isNotNaN();
    return rids;
  }

  /** The answer of the index itself, without the record lookup of the SQL function. */
  private List<RID> indexNeighbours(final float[] query, final int k, final Set<RID> allowList) {
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    final List<RID> rids = new ArrayList<>();
    for (final Pair<RID, Float> neighbour : index.findNeighborsFromVector(query, k, 400, allowList))
      rids.add(neighbour.getFirst());
    return rids;
  }
}
