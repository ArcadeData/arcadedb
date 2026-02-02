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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for pattern predicates in WHERE clauses.
 * Regression test for GitHub issue #3277.
 */
class CypherPatternPredicateTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-pattern-predicate").create();
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("IMAGE");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("in");

    database.transaction(() -> {
      // Create chunks
      MutableVertex chunk1 = database.newVertex("CHUNK");
      chunk1.set("name", "chunk1");
      chunk1.save();

      MutableVertex chunk2 = database.newVertex("CHUNK");
      chunk2.set("name", "chunk2");
      chunk2.save();

      MutableVertex chunk3 = database.newVertex("CHUNK");
      chunk3.set("name", "chunk3");
      chunk3.save();

      // Create image and document
      MutableVertex image = database.newVertex("IMAGE");
      image.set("name", "image1");
      image.save();

      MutableVertex document = database.newVertex("DOCUMENT");
      document.set("name", "doc1");
      document.save();

      // chunk1 -> in -> IMAGE
      chunk1.newEdge("in", image, true, (Object[]) null);

      // chunk2 -> in -> DOCUMENT
      chunk2.newEdge("in", document, true, (Object[]) null);

      // chunk3 has no relationships (orphan)
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
   * Tests the exact query from GitHub issue #3277:
   * MATCH (n:CHUNK) WHERE NOT (n)-[:in]-(:IMAGE) AND NOT (n)-[:in]-(:DOCUMENT) return n
   * Should return only chunk3 (the orphan chunk with no relationships).
   */
  @Test
  void patternPredicateWithNotAndLabels() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:CHUNK) WHERE NOT (n)-[:in]-(:IMAGE) AND NOT (n)-[:in]-(:DOCUMENT) RETURN n");

      List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        Vertex vertex = (Vertex) result.toElement();
        names.add(vertex.getString("name"));
      }

      // Should only get chunk3 (no relationships to IMAGE or DOCUMENT)
      assertThat(names).hasSize(1);
      assertThat(names).containsExactly("chunk3");
    });
  }

  /**
   * Tests positive pattern predicate (without NOT).
   */
  @Test
  void patternPredicatePositive() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:CHUNK) WHERE (n)-[:in]-(:IMAGE) RETURN n");

      List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        Vertex vertex = (Vertex) result.toElement();
        names.add(vertex.getString("name"));
      }

      // Should only get chunk1 (has relationship to IMAGE)
      assertThat(names).hasSize(1);
      assertThat(names).containsExactly("chunk1");
    });
  }

  /**
   * Tests pattern predicate with OR.
   */
  @Test
  void patternPredicateWithOr() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:CHUNK) WHERE (n)-[:in]-(:IMAGE) OR (n)-[:in]-(:DOCUMENT) RETURN n");

      List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        Vertex vertex = (Vertex) result.toElement();
        names.add(vertex.getString("name"));
      }

      // Should get chunk1 and chunk2 (have relationships)
      assertThat(names).hasSize(2);
      assertThat(names).containsOnly("chunk1", "chunk2");
    });
  }
}
