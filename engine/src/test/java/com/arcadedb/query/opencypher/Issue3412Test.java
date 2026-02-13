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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3412.
 * Tests that MATCH with a parent type label returns vertices of child types (polymorphism).
 * <p>
 * When CHUNK_EMBEDDING inherits from EMBEDDING, a query like:
 * {@code MATCH (n:CHUNK)--(b:EMBEDDING) WHERE ID(n) = "#1:0" RETURN n, b}
 * should also return vertices of type CHUNK_EMBEDDING.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3412">Issue 3412</a>
 */
class Issue3412Test {
  private Database database;
  private String chunkRid;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue3412-test").create();

    // Create type hierarchy: CHUNK_EMBEDDING extends EMBEDDING
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("EMBEDDING");
    final VertexType chunkEmbedding = database.getSchema().createVertexType("CHUNK_EMBEDDING");
    chunkEmbedding.addSuperType("EMBEDDING");
    database.getSchema().createEdgeType("HAS_EMBEDDING");

    // Create test data
    database.transaction(() -> {
      final MutableVertex chunk = database.newVertex("CHUNK").set("name", "chunk1").save();
      chunkRid = chunk.getIdentity().toString();

      final MutableVertex emb1 = database.newVertex("CHUNK_EMBEDDING").set("name", "emb1").save();
      final MutableVertex emb2 = database.newVertex("CHUNK_EMBEDDING").set("name", "emb2").save();

      chunk.newEdge("HAS_EMBEDDING", emb1, true, (Object[]) null);
      chunk.newEdge("HAS_EMBEDDING", emb2, true, (Object[]) null);
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Matching with child type should return results (baseline).
   */
  @Test
  void matchWithChildTypeShouldReturnResults() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)--(b:CHUNK_EMBEDDING) WHERE ID(n) = \"" + chunkRid + "\" RETURN n, b");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
  }

  /**
   * Matching with parent type should also return child type vertices (polymorphism).
   * This is the core of issue #3412.
   */
  @Test
  void matchWithParentTypeShouldReturnChildVertices() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)--(b:EMBEDDING) WHERE ID(n) = \"" + chunkRid + "\" RETURN n, b");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Should return 2 results because CHUNK_EMBEDDING extends EMBEDDING
    assertThat(results).hasSize(2);
    for (final Result result : results) {
      final Vertex b = result.getProperty("b");
      assertThat(b.getType().instanceOf("EMBEDDING")).isTrue();
    }
  }

  /**
   * Matching with parent type without WHERE ID filter should also work polymorphically.
   */
  @Test
  void matchWithParentTypeNoIdFilter() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)--(b:EMBEDDING) RETURN n, b");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
  }

  /**
   * Matching with parent type using a directed relationship.
   */
  @Test
  void matchWithParentTypeDirected() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)-->(b:EMBEDDING) RETURN n, b");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
  }

  /**
   * Also test with a mix of parent and child type vertices.
   */
  @Test
  void matchWithParentTypeIncludesBothParentAndChildVertices() {
    // Add a vertex of the parent type directly
    database.transaction(() -> {
      final MutableVertex chunk = database.iterateType("CHUNK", false).next().asVertex().modify();
      final MutableVertex parentEmb = database.newVertex("EMBEDDING").set("name", "parent_emb").save();
      chunk.newEdge("HAS_EMBEDDING", parentEmb, true, (Object[]) null);
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK)--(b:EMBEDDING) RETURN n, b");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // 2 CHUNK_EMBEDDING + 1 EMBEDDING = 3 results
    assertThat(results).hasSize(3);
  }
}
