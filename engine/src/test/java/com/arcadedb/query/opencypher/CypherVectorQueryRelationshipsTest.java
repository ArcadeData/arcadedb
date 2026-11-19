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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code db.index.vector.queryRelationships}: the Neo4j procedure that searches a vector index over a relationship type.
 * ArcadeDB could create such an index and answer it through {@code db.index.vector.queryNodes}, but had no procedure that
 * yields the relationships as relationships, so a vector index over the facts of a knowledge graph could not be read the
 * way {@code db.index.fulltext.queryRelationships} reads a full-text one.
 */
class CypherVectorQueryRelationshipsTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int EDGES      = 100;
  private static final int GROUPS     = 20;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Entity IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Entity.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Entity.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE EDGE TYPE RELATES_TO IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY RELATES_TO.uuid IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY RELATES_TO.grp IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY RELATES_TO.fact_embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON RELATES_TO (fact_embedding) LSM_VECTOR
          METADATA {
            dimensions: 16,
            similarity: 'COSINE',
            idPropertyName: 'uuid'
          }""");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Entity (embedding) LSM_VECTOR
          METADATA {
            dimensions: 16,
            similarity: 'COSINE',
            idPropertyName: 'id'
          }""");
    });

    database.transaction(() -> {
      final MutableVertex source = database.newVertex("Entity").set("id", "a").set("embedding", vector(0)).save();
      final MutableVertex target = database.newVertex("Entity").set("id", "b").set("embedding", vector(1)).save();
      for (int i = 0; i < EDGES; i++)
        source.newEdge("RELATES_TO", target).set("uuid", "e" + i).set("grp", "g" + (i % GROUPS))
            .set("fact_embedding", vector(i)).save();
    });
  }

  /** The nearest relationship comes first, with the full score of an identical vector, and each is a distinct edge. */
  @Test
  void yieldsTheNearestRelationshipsNearestFirst() {
    final List<Result> rows = query(
        "CALL db.index.vector.queryRelationships('RELATES_TO[fact_embedding]', 5, $v) YIELD relationship AS r, score "
            + "RETURN r.uuid AS uuid, score AS score", Map.of("v", vector(3)));

    assertThat(rows).hasSize(5);
    assertThat(rows.getFirst().<String>getProperty("uuid")).isEqualTo("e3");
    assertThat(rows.getFirst().<Number>getProperty("score").doubleValue()).isEqualTo(1.0, org.assertj.core.data.Offset.offset(1e-5));
    assertThat(new LinkedHashSet<>(uuids(rows))).as("rows %s must be distinct relationships", uuids(rows)).hasSize(5);
    for (int i = 1; i < rows.size(); i++)
      assertThat(rows.get(i).<Number>getProperty("score").doubleValue())
          .isLessThanOrEqualTo(rows.get(i - 1).<Number>getProperty("score").doubleValue());
  }

  /** What is yielded is a relationship: its endpoints can be read, which a bare record could not offer. */
  @Test
  void theYieldedRelationshipKnowsItsEndpoints() {
    final List<Result> rows = query(
        "CALL db.index.vector.queryRelationships('RELATES_TO[fact_embedding]', 1, $v) YIELD relationship AS r "
            + "RETURN startNode(r).id AS source, endNode(r).id AS target", Map.of("v", vector(3)));

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("source")).isEqualTo("a");
    assertThat(rows.getFirst().<String>getProperty("target")).isEqualTo("b");
  }

  /**
   * The {@code where} option is applied by the index while it walks. Asking for as many rows as the group holds, with a query
   * vector nearest to another group, only comes back full if the filter is a pre-filter: a top k cut down afterwards would hold
   * rows of the other groups and leave the group short.
   */
  @Test
  void theWhereOptionFiltersDuringTheSearchNotAfterIt() {
    final List<Result> rows = query(
        "CALL db.index.vector.queryRelationships('RELATES_TO[fact_embedding]', 5, $v, {where: {grp: 'g3'}}) "
            + "YIELD relationship AS r RETURN r.uuid AS uuid, r.grp AS grp", Map.of("v", vector(0)));

    assertThat(rows).hasSize(EDGES / GROUPS);
    assertThat(rows).allSatisfy(row -> assertThat(row.<String>getProperty("grp")).isEqualTo("g3"));
  }

  /** An index over a vertex type is refused, and the message says which procedure answers it. */
  @Test
  void anIndexOverAVertexTypeIsRefused() {
    assertThatThrownBy(() -> query(
        "CALL db.index.vector.queryRelationships('Entity[embedding]', 5, $v) YIELD relationship AS r RETURN r",
        Map.of("v", vector(0))))
        .hasMessageContaining("node type 'Entity'")
        .hasMessageContaining("db.index.vector.queryNodes()");
  }

  private List<Result> query(final String cypher, final Map<String, Object> params) {
    final Map<String, Object> copy = new HashMap<>(params);
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, copy)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private static List<String> uuids(final List<Result> rows) {
    return rows.stream().map(row -> row.<String>getProperty("uuid")).toList();
  }

  private static float[] vector(final int i) {
    final float[] v = new float[DIMENSIONS];
    v[i % DIMENSIONS] = 1.0f;
    v[(i + 1) % DIMENSIONS] = 0.1f + (i % 7) * 0.01f;
    v[(i + 2) % DIMENSIONS] = 0.05f + (i % 5) * 0.02f;
    return v;
  }
}
