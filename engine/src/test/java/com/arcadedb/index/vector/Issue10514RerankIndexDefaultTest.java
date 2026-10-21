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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.select.SelectVectorResult;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10514: a PRODUCT index walks on its PQ codes and re-ranks the best candidates on the stored vectors, and the number
 * of candidates is one setting of the index, {@code rerankOversample}, that every way of searching it shares: the SQL function
 * {@code vector.neighbors}, the openCypher procedure {@code db.index.vector.queryNodes} and the fluent select. A query
 * overrides it with {@code rerank} in SQL; {@code 0} is the PQ scores alone, which read no stored vector.
 * <p>
 * The test counts the stored vectors one cold search reads ({@code vectorFetchFromDocuments}): the exact walk reads one per
 * node it visits, the PQ walk reads the candidates it re-ranks and nothing else.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue10514RerankIndexDefaultTest extends TestHelper {
  private static final int DIMENSIONS = 64;
  private static final int VECTORS    = 5000;
  private static final int CLUSTERS   = 50;
  private static final int K          = 10;

  private static final String SQL    = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?)))";
  private static final String SQL_OPTIONS = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?, ?)))";
  private static final String CYPHER = "CALL db.index.vector.queryNodes('Doc[embedding]', " + K
      + ", $v) YIELD node, score RETURN node.vid AS vid, score";

  @Test
  void theCypherProcedureWalksOnThePqCodesOfAProductIndex() {
    final float[][] base = clustered(new Random(10514L), VECTORS);
    load(base, "PRODUCT", "");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).as("the scenario needs a trained codebook").isTrue();

    final float[] query = clustered(new Random(105140L), 1)[0];
    final long start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> rows = cypher(query);
    final long reads = index.metrics.getVectorFetchFromDocuments() - start;

    assertThat(rows).hasSize(K);
    // The default re-ranks 10 candidates per neighbour: about 100 stored vectors, where the exact walk reads several hundred
    assertThat(reads).as("stored vectors read by one cold db.index.vector.queryNodes on a PRODUCT index").isLessThanOrEqualTo(200L);
  }

  @Test
  void theIndexDefaultAppliesToSqlCypherAndSelectAlike() {
    final float[][] base = clustered(new Random(1L), VECTORS);
    load(base, "PRODUCT", "\"rerankOversample\": 0, ");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.getMetadata().rerankOversample).isZero();

    final float[] query = base[17];
    final List<Pair<RID, Float>> pqScores = index.findNeighborsFromVectorApproximate(query, K);

    long start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> sql = sql(query);
    assertThat(index.metrics.getVectorFetchFromDocuments() - start).as("SQL reads no stored vector with rerankOversample 0").isZero();
    assertSameRowsAs(pqScores, sql, "distance");

    start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> cypher = cypher(query);
    assertThat(index.metrics.getVectorFetchFromDocuments() - start).as("openCypher reads no stored vector with rerankOversample 0")
        .isZero();
    assertThat(cypher).extracting(r -> (int) r.getProperty("vid")).containsExactlyElementsOf(
        pqScores.stream().map(p -> vid(p.getFirst())).toList());

    start = index.metrics.getVectorFetchFromDocuments();
    final List<SelectVectorResult<Document>> select = database.select().fromType("Doc")
        .nearestTo("embedding", query, K).documents();
    assertThat(index.metrics.getVectorFetchFromDocuments() - start).as("the fluent select reads no stored vector either").isZero();
    assertThat(select).extracting(r -> r.getDocument().getInteger("vid")).containsExactlyElementsOf(
        pqScores.stream().map(p -> vid(p.getFirst())).toList());
  }

  @Test
  void aQueryOverridesTheIndexDefault() {
    final float[][] base = clustered(new Random(2L), VECTORS);
    load(base, "PRODUCT", "\"rerankOversample\": 0, ");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = base[23];
    final long start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> rows = sql(query, Map.of("rerank", 10));
    final long reads = index.metrics.getVectorFetchFromDocuments() - start;

    assertThat(rows).hasSize(K);
    assertThat(reads).as("rerank: 10 beats the index default of 0").isBetween((long) K, 200L);
    float previous = -1;
    for (final Result row : rows) {
      final float distance = row.getProperty("distance");
      final float expected = squaredDistance(query, base[row.<Integer>getProperty("vid")]);
      assertThat(distance).isCloseTo(expected, Offset.offset(Math.max(1e-3f, expected * 1e-4f)));
      assertThat(distance).isGreaterThanOrEqualTo(previous);
      previous = distance;
    }
  }

  @Test
  void aWiderIndexDefaultReReadsMoreCandidates() {
    final float[][] base = clustered(new Random(3L), VECTORS);
    load(base, "PRODUCT", "\"rerankOversample\": 30, ");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final long start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> rows = sql(base[5]);
    final long reads = index.metrics.getVectorFetchFromDocuments() - start;

    assertThat(rows).hasSize(K);
    // 30 candidates per neighbour for 10 neighbours is 300 stored vectors, where the default of 10 is 100
    assertThat(reads).as("stored vectors read with rerankOversample 30").isBetween(250L, 600L);
  }

  @Test
  void theIndexDefaultSurvivesAReopenAndAnInvalidOneIsRefused() {
    final float[][] base = clustered(new Random(4L), 500);
    load(base, "PRODUCT", "\"rerankOversample\": 7, ");
    assertThat(indexOf().getMetadata().rerankOversample).isEqualTo(7);

    database.close();
    database = factory.open();
    assertThat(indexOf().getMetadata().rerankOversample).isEqualTo(7);

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Doc (vid) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"rerankOversample\": -2 }"))
        .hasMessageContaining("rerankOversample");
  }

  @Test
  void otherQuantizationsKeepTheirExactWalk() {
    final float[][] base = clustered(new Random(5L), 3000);
    load(base, "NONE", "\"rerankOversample\": 0, ");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = base[9];
    final List<Pair<RID, Float>> exact = index.findNeighborsFromVector(query, K, -1, null);
    final List<Result> rows = sql(query);
    assertSameRowsAs(exact, rows, "distance");
    assertThat(cypher(query)).extracting(r -> (int) r.getProperty("vid")).containsExactlyElementsOf(
        exact.stream().map(p -> vid(p.getFirst())).toList());
  }

  @Test
  void aSelectRerankAppliesWithoutApproximateToo() {
    final float[][] base = clustered(new Random(7L), VECTORS);
    load(base, "PRODUCT", "\"rerankOversample\": 0, ");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = base[31];
    final long start = index.metrics.getVectorFetchFromDocuments();
    final List<SelectVectorResult<Document>> rows = database.select().fromType("Doc").nearestTo("embedding", query, K).rerank(10)
        .documents();
    final long reads = index.metrics.getVectorFetchFromDocuments() - start;

    assertThat(rows).hasSize(K);
    assertThat(reads).as("rerank(10) beats the index default of 0, as the rerank option of vector.neighbors does")
        .isBetween((long) K, 200L);
  }

  @Test
  void aRerankSeesAVectorInsertedAfterTheBuildAtItsExactDistanceWithAndWithoutAnAllowList() {
    // The graph is frozen, so the new vector stays in the delta buffer and only the delta scan can find it
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
    try {
      final float[][] base = clustered(new Random(8L), 3000);
      load(base, "PRODUCT", "");
      final LSMVectorIndex index = indexOf();
      index.buildVectorGraphNow();

      final float[] query = clustered(new Random(80L), 1)[0];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("vid", base.length);
        doc.set("embedding", query);
        doc.save();
      });
      assertThat(PendingDeltaVectors.of(index)).as("the vector has to stay in the delta buffer").isEqualTo(1L);

      final Set<RID> narrow = new HashSet<>();
      final Set<RID> wide = new HashSet<>();
      database.iterateType("Doc", false).forEachRemaining(r -> {
        final int id = r.asDocument().getInteger("vid");
        if (id == base.length || id < 20)
          narrow.add(r.getIdentity());
        if (id == base.length || id % 2 == 0)
          wide.add(r.getIdentity());
      });

      // no allow-list (the walk), a narrow one (the exact pre-filter plan) and a wide one (the walk, filtered)
      for (final Set<RID> allowed : Arrays.asList(null, narrow, wide)) {
        final List<Pair<RID, Float>> answer = index.findNeighborsFromVectorApproximate(query, K, -1,
            LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE, allowed, WorkGuard.forCommandDeadline(null));
        final String which = allowed == null ? "no allow-list" : allowed == narrow ? "a narrow allow-list" : "a wide allow-list";
        assertThat(answer).as(which).hasSize(K);
        assertThat(vid(answer.getFirst().getFirst())).as("the delta vector is its own nearest neighbour, " + which)
            .isEqualTo(base.length);
        for (final Pair<RID, Float> neighbour : answer) {
          if (allowed != null)
            assertThat(allowed).as(which).contains(neighbour.getFirst());
          final int id = vid(neighbour.getFirst());
          final float expected = id == base.length ? 0f : squaredDistance(query, base[id]);
          assertThat(neighbour.getSecond()).as("exact distance, " + which)
              .isCloseTo(expected, Offset.offset(Math.max(1e-3f, expected * 1e-4f)));
        }
      }
    } finally {
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
    }
  }

  @Test
  void theAllowListThresholdDependsOnWhetherTheSearchReRanks() {
    final float[][] base = clustered(new Random(9L), VECTORS);
    load(base, "PRODUCT", "");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    // 10% of the index: above the 5% threshold of the PQ-scores plan, below the 20% one of the exact plan
    final Set<RID> allowed = new HashSet<>();
    database.iterateType("Doc", false).forEachRemaining(r -> {
      if (r.asDocument().getInteger("vid") < VECTORS / 10)
        allowed.add(r.getIdentity());
    });
    final float[] query = base[7];

    final long start = index.getStats().get("preFilterSearches");
    index.findNeighborsFromVectorApproximate(query, K, -1, 0, allowed, WorkGuard.forCommandDeadline(null));
    assertThat((long) index.getStats().get("preFilterSearches")).as("PQ scores alone walk the graph at 10% selectivity")
        .isEqualTo(start);

    index.findNeighborsFromVectorApproximate(query, K, -1, LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE, allowed,
        WorkGuard.forCommandDeadline(null));
    assertThat((long) index.getStats().get("preFilterSearches")).as("a re-ranked search scores the allow-list exactly at 10%")
        .isEqualTo(start + 1);
  }

  @Test
  void aNegativePersistedRerankOversampleFailsWhenTheSchemaLoads() {
    final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("Doc", new String[] { "embedding" }, 0);
    final JSONObject persisted = new JSONObject().put("typeName", "Doc").put("properties", new JSONArray().put("embedding"))
        .put("rerankOversample", -1);
    assertThatThrownBy(() -> metadata.fromJSON(persisted)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rerankOversample");
  }

  @Test
  void aProductIndexWithoutATrainedCodebookFallsBackToTheExactWalk() {
    // fewer vectors than the 256 clusters of the default codebook: no PQ is trained, so there are no codes to walk on
    final float[][] base = clustered(new Random(6L), 100);
    load(base, "PRODUCT", "");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).as("the scenario needs an index without a codebook").isFalse();

    final float[] query = base[3];
    final List<Pair<RID, Float>> exact = index.findNeighborsFromVector(query, K, -1, null);
    assertThat(exact).hasSize(K);
    assertThat(index.findNeighbors(query, K, -1, -1, null, WorkGuard.forCommandDeadline(null))).isEqualTo(exact);
    // the fallback keeps the beam the caller asked for
    assertThat(index.findNeighborsFromVectorApproximate(query, K, 50, 10, null, WorkGuard.forCommandDeadline(null)))
        .isEqualTo(index.findNeighborsFromVector(query, K, 50, null));
    assertSameRowsAs(exact, sql(query), "distance");
    assertThat(cypher(query)).extracting(r -> (int) r.getProperty("vid")).containsExactlyElementsOf(
        exact.stream().map(p -> vid(p.getFirst())).toList());
  }

  private void assertSameRowsAs(final List<Pair<RID, Float>> expected, final List<Result> rows, final String distanceProperty) {
    assertThat(rows).hasSameSizeAs(expected);
    for (int i = 0; i < rows.size(); i++) {
      assertThat((int) rows.get(i).getProperty("vid")).isEqualTo(vid(expected.get(i).getFirst()));
      assertThat((float) rows.get(i).getProperty(distanceProperty)).isEqualTo(expected.get(i).getSecond());
    }
  }

  private List<Result> sql(final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", SQL, query, K)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<Result> sql(final float[] query, final Map<String, Object> options) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", SQL_OPTIONS, query, K, options)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<Result> cypher(final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", CYPHER, Map.of("v", query))) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private int vid(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().getInteger("vid");
  }

  private void load(final float[][] base, final String quantization, final String extraMetadata) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"" + quantization + "\", " + extraMetadata
          + "\"storeVectorsInGraph\": false, \"addHierarchy\": false }");
    });
    database.transaction(() -> {
      for (int i = 0; i < base.length; i++) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("vid", i);
        doc.set("embedding", base[i]);
        doc.save();
      }
    });
  }

  /** {@code count} vectors around {@link #CLUSTERS} centres; the centres come from their own fixed seed, so base and queries share them. */
  private static float[][] clustered(final Random random, final int count) {
    final Random centreRandom = new Random(42L);
    final float[][] centres = new float[CLUSTERS][DIMENSIONS];
    for (final float[] centre : centres)
      for (int j = 0; j < DIMENSIONS; j++)
        centre[j] = (float) (centreRandom.nextGaussian() * 3.0);
    final float[][] vectors = new float[count][DIMENSIONS];
    for (int i = 0; i < count; i++) {
      final float[] centre = centres[random.nextInt(CLUSTERS)];
      for (int j = 0; j < DIMENSIONS; j++)
        vectors[i][j] = centre[j] + (float) random.nextGaussian();
    }
    return vectors;
  }

  private static float squaredDistance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      final double d = a[i] - b[i];
      sum += d * d;
    }
    return (float) sum;
  }
}
