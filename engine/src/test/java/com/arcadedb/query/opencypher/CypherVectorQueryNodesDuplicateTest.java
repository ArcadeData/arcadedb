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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7057: {@code db.index.vector.queryNodes} returned every matching node twice, so asking for {@code k} rows
 * yielded {@code k} rows carrying only {@code k / 2} distinct nodes - silently halving the candidates any consumer
 * taking the top {@code k} received, and pinning measured recall at exactly 0.50.
 * <p>
 * The procedure concatenates one search per vector sub-index of the type and truncates the merged list at {@code k}.
 * Nothing in that pipeline promised one row per node: not the merge, and not the graph walk feeding it, where a RID
 * owning more than one live vector id owns one graph ordinal per id.
 */
class CypherVectorQueryNodesDuplicateTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int NODES      = 100;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Entity IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Entity.uuid IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Entity.name_embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Entity (uuid) UNIQUE");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Entity (name_embedding) LSM_VECTOR
          METADATA {
            dimensions: 16,
            similarity: 'COSINE',
            idPropertyName: 'uuid'
          }""");
    });

    database.transaction(() -> {
      for (int i = 0; i < NODES; i++)
        database.newVertex("Entity").set("uuid", "u" + i).set("name_embedding", vector(i)).save();
    });
  }

  /**
   * The reported shape. A type whose {@link TypeIndex} carries the same bucket sub-index twice - which
   * {@link TypeIndex#addIndexOnBucket} accepts without complaint - made the procedure search the same records twice
   * and concatenate the two answers. Before the fix this returned the issue's numbers exactly: 6 rows, 3 distinct.
   * <p>
   * The same sub-index, not two different ones on one bucket: the schema rejects a second index over an
   * already-indexed property set outright ("A type holds one index per property set"), so listing one index twice is
   * the only multiplicity this fan-out can actually meet.
   */
  @Test
  void queryNodesReturnsDistinctNodesWhenTheSameSubIndexIsAttachedTwice() {
    final TypeIndex typeIndex = database.getSchema().getType("Entity").getPolymorphicIndexByProperties("name_embedding");
    final IndexInternal[] before = typeIndex.getIndexesOnBuckets();
    for (final IndexInternal bucketIndex : before)
      typeIndex.addIndexOnBucket(bucketIndex);

    assertThat(typeIndex.getIndexesOnBuckets())
        .as("the fixture has to actually plant the multiplicity, or the assertions below cannot fail")
        .hasSize(before.length * 2);

    assertDistinctTopK(6);
    assertDistinctTopK(10);
  }

  /**
   * The dedup has to keep the NEAREST sighting of a node, not an arbitrary one, and it has to keep the order: the
   * doubled schema must produce exactly the answer the healthy one does, row for row and score for score.
   */
  @Test
  void queryNodesAnswerIsUnchangedByTheDuplicatedSubIndex() {
    final List<String> healthy = queryTopK(vector(3), 8);
    final List<Double> healthyScores = scoresTopK(vector(3), 8);

    final TypeIndex typeIndex = database.getSchema().getType("Entity").getPolymorphicIndexByProperties("name_embedding");
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      typeIndex.addIndexOnBucket(bucketIndex);

    assertThat(queryTopK(vector(3), 8)).isEqualTo(healthy);
    assertThat(scoresTopK(vector(3), 8)).isEqualTo(healthyScores);
  }

  /**
   * {@code vector.neighbors} is the ArcadeDB-native entry point onto the same fan-out over the type's vector
   * sub-indexes, and it duplicated for the same reason. Fixing only the Neo4j-compatible procedure would leave the
   * SQL function - and {@code CALL vector.neighbors(...)} from Cypher - returning each record twice.
   */
  @Test
  void vectorNeighborsReturnsDistinctRecordsWhenTheSameSubIndexIsAttachedTwice() {
    final TypeIndex typeIndex = database.getSchema().getType("Entity").getPolymorphicIndexByProperties("name_embedding");
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      typeIndex.addIndexOnBucket(bucketIndex);

    final Map<String, Object> params = new HashMap<>();
    params.put("v", vector(3));

    final List<String> uuids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(vector.neighbors('Entity[name_embedding]', :v, 6))", params)) {
      while (rs.hasNext())
        uuids.add(rs.next().getProperty("uuid"));
    }

    assertThat(uuids).hasSize(6);
    assertThat(new LinkedHashSet<>(uuids)).as("rows %s must all be distinct records", uuids).hasSize(6);
  }

  /** The ordinary case: nothing anomalous, and the answer must stay exactly what it was. */
  @Test
  void queryNodesStillReturnsTheNearestNodesInOrder() {
    final List<String> hits = queryTopK(vector(3), 5);

    assertThat(hits).hasSize(5);
    assertThat(hits.getFirst()).isEqualTo("u3");
    assertThat(new LinkedHashSet<>(hits)).hasSize(5);
  }

  private void assertDistinctTopK(final int k) {
    final List<String> rows = queryTopK(vector(3), k);
    final Set<String> distinct = new LinkedHashSet<>(rows);

    assertThat(rows).as("k rows were requested and %d nodes exist", NODES).hasSize(k);
    assertThat(distinct).as("rows %s must all be distinct nodes", rows).hasSize(k);
  }

  private List<Double> scoresTopK(final float[] queryVector, final int k) {
    final Map<String, Object> params = new HashMap<>();
    params.put("v", queryVector);
    params.put("k", k);

    final List<Double> scores = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', $k, $v) YIELD node AS n, score RETURN score AS s",
        params)) {
      while (rs.hasNext())
        scores.add(((Number) rs.next().getProperty("s")).doubleValue());
    }
    return scores;
  }

  private List<String> queryTopK(final float[] queryVector, final int k) {
    final Map<String, Object> params = new HashMap<>();
    params.put("v", queryVector);
    params.put("k", k);

    final List<String> uuids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', $k, $v) YIELD node AS n, score RETURN n.uuid AS u",
        params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        uuids.add(row.getProperty("u"));
      }
    }
    return uuids;
  }

  private static float[] vector(final int i) {
    final float[] v = new float[DIMENSIONS];
    v[i % DIMENSIONS] = 1.0f;
    v[(i + 1) % DIMENSIONS] = 0.1f + (i % 7) * 0.01f;
    v[(i + 2) % DIMENSIONS] = 0.05f + (i % 5) * 0.02f;
    return v;
  }
}
