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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4334.
 *
 * JVector's EUCLIDEAN compare() returns similarity = 1/(1+L2²), not a distance.
 * K-NN must return closest matches first (ascending L2²).
 * Before the fix the sort used raw similarity, placing farthest candidates first.
 */
class LSMVectorIndexEuclideanOrderingTest extends TestHelper {

  @Test
  void euclideanKnnReturnClosestFirst() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EuclidVec IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY EuclidVec.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY EuclidVec.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON EuclidVec (embedding) LSM_VECTOR
          METADATA {
            "dimensions": 2,
            "similarity": "EUCLIDEAN",
            "maxConnections": 16,
            "beamWidth": 100
          }""");
    });

    // near: L2²=0.02, far: L2²=200 from query [0,0]
    final RID[] nearRid = new RID[1];
    database.transaction(() -> {
      nearRid[0] = database.newVertex("EuclidVec")
          .set("name", "near")
          .set("embedding", new float[] { 0.1f, 0.1f })
          .save()
          .getIdentity();
      database.newVertex("EuclidVec")
          .set("name", "far")
          .set("embedding", new float[] { 10.0f, 10.0f })
          .save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("EuclidVec[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(new float[] { 0.0f, 0.0f }, 1);

    assertThat(results).hasSize(1);
    assertThat(results.getFirst().getFirst()).as("k=1 must return the closest vector").isEqualTo(nearRid[0]);
  }

  @Test
  void euclideanKnnOrdering() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EuclidOrder IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY EuclidOrder.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY EuclidOrder.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON EuclidOrder (embedding) LSM_VECTOR
          METADATA {
            "dimensions": 2,
            "similarity": "EUCLIDEAN",
            "maxConnections": 16,
            "beamWidth": 100
          }""");
    });

    // Three vectors at known L2² from query [0,0]:
    //   near: [0.1, 0.1] → L2²=0.02
    //   mid:  [3.0, 4.0] → L2²=25.0
    //   far:  [10.0,10.0] → L2²=200.0
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      rids[0] = database.newVertex("EuclidOrder")
          .set("name", "near")
          .set("embedding", new float[] { 0.1f, 0.1f })
          .save().getIdentity();
      rids[1] = database.newVertex("EuclidOrder")
          .set("name", "mid")
          .set("embedding", new float[] { 3.0f, 4.0f })
          .save().getIdentity();
      rids[2] = database.newVertex("EuclidOrder")
          .set("name", "far")
          .set("embedding", new float[] { 10.0f, 10.0f })
          .save().getIdentity();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("EuclidOrder[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(new float[] { 0.0f, 0.0f }, 3);

    assertThat(results).hasSize(3);
    assertThat(results.getFirst().getFirst()).as("first result must be the nearest vector").isEqualTo(rids[0]);
    assertThat(results.get(2).getFirst()).as("last result must be the farthest vector").isEqualTo(rids[2]);
    assertThat(results.getFirst().getSecond())
        .as("distances must be non-decreasing")
        .isLessThanOrEqualTo(results.get(1).getSecond());
    assertThat(results.get(1).getSecond())
        .as("distances must be non-decreasing")
        .isLessThanOrEqualTo(results.get(2).getSecond());
  }

  @Test
  void scoreToDistanceHelperRoundsTripEuclidean() {
    // Verify the helper formula directly: similarity = 1/(1+L2²) → distance = (1/similarity)-1 = L2²
    final float l2Squared = 4.0f;
    final float similarity = 1.0f / (1.0f + l2Squared);
    final float distance = LSMVectorIndex.scoreToDistance(VectorSimilarityFunction.EUCLIDEAN, similarity);
    assertThat(distance).isEqualTo(l2Squared);

    // Defensive branch: zero similarity (vectors at infinity) must produce a max-distance sentinel
    assertThat(LSMVectorIndex.scoreToDistance(VectorSimilarityFunction.EUCLIDEAN, 0.0f))
        .isEqualTo(Float.MAX_VALUE);
  }

  @Test
  void queryNodesEuclideanScoreIsPositiveSimilarity() {
    // Neo4j-compatible db.index.vector.queryNodes must yield a (0, 1] similarity score for EUCLIDEAN,
    // not 1 - L2² (which would be hugely negative once the LSMVectorIndex distance is squared L2).
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE EuclidQN IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY EuclidQN.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY EuclidQN.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON EuclidQN (embedding) LSM_VECTOR
          METADATA {
            "dimensions": 2,
            "similarity": "EUCLIDEAN",
            "idPropertyName": "name"
          }""");
    });

    database.transaction(() -> {
      database.newVertex("EuclidQN").set("name", "near")
          .set("embedding", new float[] { 0.1f, 0.1f }).save();
      database.newVertex("EuclidQN").set("name", "far")
          .set("embedding", new float[] { 10.0f, 10.0f }).save();
    });

    final Map<String, Object> params = new HashMap<>(Map.of(
        "vec", new float[]{0.0f, 0.0f},
        "k", 2));

    try (final ResultSet results = database.query("opencypher",
        "CALL db.index.vector.queryNodes('EuclidQN[embedding]', $k, $vec) YIELD node, score RETURN node.name AS name, score",
        params)) {

      assertThat(results.hasNext()).isTrue();
      final Result first = results.next();
      assertThat((String) first.getProperty("name")).isEqualTo("near");
      final double nearScore = ((Number) first.getProperty("score")).doubleValue();
      assertThat(nearScore).as("near vector score must be a positive similarity in (0, 1]").isBetween(0.0, 1.0);

      assertThat(results.hasNext()).isTrue();
      final Result second = results.next();
      assertThat((String) second.getProperty("name")).isEqualTo("far");
      final double farScore = ((Number) second.getProperty("score")).doubleValue();
      assertThat(farScore).as("far vector score must be a positive similarity in (0, 1]").isBetween(0.0, 1.0);
      assertThat(nearScore).as("nearer vector must have a higher similarity score").isGreaterThan(farScore);
    }
  }
}
