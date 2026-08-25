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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6559 item 2: {@link LSMVectorIndex#findNeighborsFromVectorApproximate} - the zero-disk-I/O PQ path - scored
 * every graph candidate approximately from the in-memory PQ codes, but merged in delta-buffer rows scored
 * <em>exactly</em>, then sorted the two against each other as one list.
 * <p>
 * The fixture puts the defect in its purest form: a row is inserted into the delta buffer carrying a vector that is
 * <em>bit-identical</em> to one already in the graph. The two are the same point in space, at the same true distance
 * from any query, so whatever number the search hands back for one it has to hand back for the other. Before the fix
 * the graph row came back with its PQ distance (carrying quantization error) and the delta row with an exact 0, so
 * the delta twin outranked its own graph copy every time - not by being nearer, but by being the only one of the two
 * measured without error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6559ApproximateDeltaScoringScaleTest extends TestHelper {

  private static final int DIMENSIONS = 32;
  private static final int VECTORS    = 400;
  private static final int PQ_CLUSTERS = 16;
  // The graph vector the delta row is made a twin of. Any indexed vector does; this one is simply not at an edge.
  private static final int TWINNED    = 17;
  // Wide enough that the twinned vector and its delta copy are both comfortably inside the answer.
  private static final int TOP_K      = 25;

  @BeforeEach
  void freezeTheGraph() {
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

  /**
   * Two rows, one vector, one query: the graph copy and its delta twin must come back with the same distance. The
   * guard below is what keeps this from passing vacuously - it pins that PQ really is lossy on this fixture, so a
   * build that scored the delta side exactly would produce two visibly different numbers and fail here.
   */
  @Test
  void aDeltaRowIsScoredOnTheSamePqScaleAsItsGraphTwin() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    assertThat(index.isPQSearchAvailable()).as("the fixture is only meaningful while the PQ path is really taken")
        .isTrue();

    final float[] query = vector(TWINNED);

    // The graph row's own PQ distance to a query that IS its vector. Non-zero exactly to the extent PQ is lossy.
    final List<Pair<RID, Float>> beforeInsert = index.findNeighborsFromVectorApproximate(query, TOP_K, null);
    final float graphDistance = distanceOf(beforeInsert, "doc" + TWINNED);
    assertThat(graphDistance)
        .as("guard against a vacuous test: PQ has to be lossy here, or scoring the delta side exactly would agree "
            + "with the graph side by accident and this test could never fail")
        .isGreaterThan(1e-6f);

    // The twin: same vector, new record, and it stays in the delta buffer because the graph is frozen.
    database.transaction(() -> database.newDocument("Doc").set("id", "twin").set("embedding", vector(TWINNED)).save());
    assertThat(index.getStats().get("deltaVectorsCount"))
        .as("the fixture is only a regression test while the twin is still in the delta buffer").isEqualTo(1L);

    final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, TOP_K, null);
    final float twinDistance = distanceOf(results, "twin");

    assertThat(twinDistance)
        .as("a delta row and its bit-identical graph twin are the same point, so the PQ path has to score them on "
            + "the same scale - before the fix the twin came back exact (0) against the graph row's PQ distance")
        .isCloseTo(graphDistance, org.assertj.core.data.Offset.offset(1e-5f));
  }

  /**
   * The other surface with the same merge, and the same defect: the issue #6514 pre-filter plan scores its
   * allow-listed ordinals from the PQ codes too, so the delta rows merged alongside them need the same treatment.
   */
  @Test
  void thePreFilterPlanAlsoScoresDeltaRowsOnThePqScale() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = vector(TWINNED);

    final float graphDistance = distanceOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null), "doc" + TWINNED);
    assertThat(graphDistance).as("guard against a vacuous test: PQ has to be lossy here").isGreaterThan(1e-6f);

    database.transaction(() -> database.newDocument("Doc").set("id", "twin").set("embedding", vector(TWINNED)).save());

    // Narrow enough to take the pre-filter plan rather than the graph walk.
    final java.util.Set<RID> allowed = new java.util.HashSet<>();
    allowed.add(ridOf("doc" + TWINNED));
    allowed.add(ridOf("twin"));

    final long preFilterBefore = index.getStats().get("preFilterSearches");
    final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, TOP_K, allowed);
    assertThat(index.getStats().get("preFilterSearches")).as("the fixture has to actually reach the pre-filter plan")
        .isGreaterThan(preFilterBefore);

    assertThat(distanceOf(results, "twin"))
        .as("the pre-filter plan ranks its ordinals on the PQ scale, so its merged delta rows must be there too")
        .isCloseTo(distanceOf(results, "doc" + TWINNED), org.assertj.core.data.Offset.offset(1e-5f));
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private float distanceOf(final List<Pair<RID, Float>> results, final String id) {
    for (final Pair<RID, Float> r : results)
      if (id.equals(((Document) database.lookupByRID(r.getFirst(), true)).getString("id")))
        return r.getSecond();
    throw new AssertionError("row '" + id + "' is missing from the answer: " + results);
  }

  private RID ridOf(final String id) {
    try (final com.arcadedb.query.sql.executor.ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?",
        id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private void createSchemaAndData() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      for (int i = 0; i < VECTORS; i++)
        database.newDocument("Doc").set("id", "doc" + i).set("embedding", vector(i)).save();
    });

    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": %d,
          "similarity": "COSINE",
          "quantization": "PRODUCT",
          "pqClusters": %d
        }""".formatted(DIMENSIONS, PQ_CLUSTERS));
  }

  /**
   * Deterministic, and spread over the whole space rather than clustered around one direction: with a constant
   * offset added to every component the vectors are all nearly parallel, cosine similarity is ~1 for every pair, and
   * every distance in the answer ties - which hides exactly the difference this test is here to measure.
   */
  private static float[] vector(final int seed) {
    final Random rnd = new Random(seed * 1_000_003L);
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = rnd.nextFloat() * 2 - 1;
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
