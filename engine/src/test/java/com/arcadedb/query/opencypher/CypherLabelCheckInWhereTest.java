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
 * Tests for label check expressions in WHERE clauses.
 * Regression test for GitHub issue #3276: match (n) WHERE n:NER OR n:CHUNK return n
 * fails to filter by type, returning all nodes.
 */
public class CypherLabelCheckInWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-label-check-where").create();
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("OTHER");

    database.transaction(() -> {
      MutableVertex ner1 = database.newVertex("NER");
      ner1.set("name", "ner1");
      ner1.save();

      MutableVertex ner2 = database.newVertex("NER");
      ner2.set("name", "ner2");
      ner2.save();

      MutableVertex chunk1 = database.newVertex("CHUNK");
      chunk1.set("name", "chunk1");
      chunk1.save();

      MutableVertex other1 = database.newVertex("OTHER");
      other1.set("name", "other1");
      other1.save();

      MutableVertex other2 = database.newVertex("OTHER");
      other2.set("name", "other2");
      other2.save();
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
   * Tests the exact query from GitHub issue #3276:
   * match (n) WHERE n:NER OR n:CHUNK return n
   * Should only return NER and CHUNK nodes, not OTHER nodes.
   */
  @Test
  void testLabelCheckWithOrInWhere() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) WHERE n:NER OR n:CHUNK RETURN n");

      List<String> types = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        // Single element return - access via toElement()
        Vertex vertex = (Vertex) result.toElement();
        types.add(vertex.getTypeName());
      }

      // Should only get NER (2) and CHUNK (1) nodes, not OTHER (2) nodes
      assertThat(types).hasSize(3);
      assertThat(types).containsOnly("NER", "CHUNK");
      assertThat(types.stream().filter(t -> t.equals("NER")).count()).isEqualTo(2);
      assertThat(types.stream().filter(t -> t.equals("CHUNK")).count()).isEqualTo(1);
    });
  }

  /**
   * Tests simple single label check in WHERE clause.
   */
  @Test
  void testSingleLabelCheckInWhere() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) WHERE n:NER RETURN n");

      List<String> types = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        // Single element return - access via toElement()
        Vertex vertex = (Vertex) result.toElement();
        types.add(vertex.getTypeName());
      }

      assertThat(types).hasSize(2);
      assertThat(types).containsOnly("NER");
    });
  }

  /**
   * Tests label check combined with other conditions.
   */
  @Test
  void testLabelCheckWithOtherConditions() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) WHERE n:NER AND n.name = 'ner1' RETURN n");

      List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        // Single element return - access via toElement()
        Vertex vertex = (Vertex) result.toElement();
        names.add(vertex.getString("name"));
      }

      assertThat(names).hasSize(1);
      assertThat(names).containsExactly("ner1");
    });
  }

  /**
   * Tests negated label check (NOT n:Label).
   */
  @Test
  void testNegatedLabelCheckInWhere() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) WHERE NOT n:OTHER RETURN n");

      List<String> types = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        // Single element return - access via toElement()
        Vertex vertex = (Vertex) result.toElement();
        types.add(vertex.getTypeName());
      }

      // Should get NER (2) and CHUNK (1), not OTHER (2)
      assertThat(types).hasSize(3);
      assertThat(types).containsOnly("NER", "CHUNK");
    });
  }

  /**
   * Tests label check with AND operator.
   */
  @Test
  void testLabelCheckWithAndInWhere() {
    database.transaction(() -> {
      // This should return 0 results since no vertex is both NER and CHUNK
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) WHERE n:NER AND n:CHUNK RETURN n");

      List<String> types = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        // Single element return - access via toElement()
        Vertex vertex = (Vertex) result.toElement();
        types.add(vertex.getTypeName());
      }

      assertThat(types).isEmpty();
    });
  }
}
