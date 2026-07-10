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
import com.arcadedb.graph.Edge;
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
 * Regression tests for <a href="https://github.com/ArcadeData/arcadedb/issues/4353">#4353</a>:
 * nodes(), relationships() and length() on variable-length path patterns such as [*1..3].
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4353Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-issue-4353");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Worker");
    database.getSchema().createEdgeType("REPORTS_TO");

    // Chain of 4 workers: a -> b -> c -> d (3 hops)
    database.command("opencypher",
        """
        CREATE (a:Worker {name: 'A'}), \
        (b:Worker {name: 'B'}), \
        (c:Worker {name: 'C'}), \
        (d:Worker {name: 'D'}), \
        (a)-[:REPORTS_TO]->(b), \
        (b)-[:REPORTS_TO]->(c), \
        (c)-[:REPORTS_TO]->(d)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void nodesOnVariableLengthPath() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
        RETURN nodes(p) AS nodeList, b.name AS endName \
        ORDER BY endName""");

    final List<List<?>> nodes = new ArrayList<>();
    final List<String> endNames = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      nodes.add((List<?>) row.getProperty("nodeList"));
      endNames.add(row.getProperty("endName"));
    }
    assertThat(endNames).containsExactly("B", "C", "D");
    assertThat(nodes.getFirst()).hasSize(2);
    assertThat(nodes.get(1)).hasSize(3);
    assertThat(nodes.get(2)).hasSize(4);
    for (final List<?> n : nodes)
      for (final Object o : n)
        assertThat(o).isInstanceOf(Vertex.class);
  }

  @Test
  void relationshipsOnVariableLengthPath() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
        RETURN relationships(p) AS relList, b.name AS endName \
        ORDER BY endName""");

    final List<List<?>> rels = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      rels.add((List<?>) row.getProperty("relList"));
    }
    assertThat(rels.getFirst()).hasSize(1);
    assertThat(rels.get(1)).hasSize(2);
    assertThat(rels.get(2)).hasSize(3);
    for (final List<?> r : rels)
      for (final Object o : r)
        assertThat(o).isInstanceOf(Edge.class);
  }

  @Test
  void lengthOnVariableLengthPath() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
        RETURN length(p) AS pathLength, b.name AS endName \
        ORDER BY endName""");

    final List<Long> lengths = new ArrayList<>();
    while (result.hasNext())
      lengths.add(((Number) result.next().getProperty("pathLength")).longValue());

    assertThat(lengths).containsExactly(1L, 2L, 3L);
  }

  @Test
  void allPathFunctionsTogether() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker {name: 'D'}) \
        RETURN nodes(p) AS nodeList, relationships(p) AS relList, length(p) AS pathLength""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final List<?> nodeList = row.getProperty("nodeList");
    final List<?> relList = row.getProperty("relList");
    assertThat(nodeList).hasSize(4);
    assertThat(relList).hasSize(3);
    assertThat(((Number) row.getProperty("pathLength")).longValue()).isEqualTo(3L);
  }

  @Test
  void whereLengthFilter() {
    // Only return paths of length 2 or more
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
        WHERE length(p) >= 2 \
        RETURN b.name AS endName, length(p) AS len \
        ORDER BY endName""");

    final List<String> endNames = new ArrayList<>();
    final List<Long> lengths = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      endNames.add(row.getProperty("endName"));
      lengths.add(((Number) row.getProperty("len")).longValue());
    }
    assertThat(endNames).containsExactly("C", "D");
    assertThat(lengths).containsExactly(2L, 3L);
  }

  @Test
  void unwindNodesOfPath() {
    // UNWIND nodes(p) AS n - exposes each node of every matching path
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker {name: 'D'}) \
        UNWIND nodes(p) AS n \
        RETURN n.name AS name \
        ORDER BY name""");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));
    assertThat(names).containsExactly("A", "B", "C", "D");
  }

  @Test
  void variableLengthWithNoBounds() {
    // [*] (no bounds) defaults to *1..max - typically 1..Integer.MAX_VALUE in Neo4j
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*]->(b:Worker {name: 'D'}) \
        RETURN length(p) AS len, nodes(p) AS ns, relationships(p) AS rs""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("len")).longValue()).isEqualTo(3L);
    assertThat((List<?>) row.getProperty("ns")).hasSize(4);
    assertThat((List<?>) row.getProperty("rs")).hasSize(3);
  }

  @Test
  void variableLengthWithUpperBoundOnly() {
    // [*..3] - up to 3 hops (default min=1)
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*..3]->(b:Worker) \
        RETURN length(p) AS len, b.name AS endName \
        ORDER BY endName""");

    final List<Long> lengths = new ArrayList<>();
    while (result.hasNext())
      lengths.add(((Number) result.next().getProperty("len")).longValue());
    assertThat(lengths).containsExactly(1L, 2L, 3L);
  }

  @Test
  void variableLengthWithReverseDirection() {
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (d:Worker {name: 'D'})<-[:REPORTS_TO*1..3]-(b:Worker) \
        RETURN length(p) AS len, b.name AS sourceName \
        ORDER BY sourceName""");

    final List<String> names = new ArrayList<>();
    final List<Long> lengths = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      names.add(row.getProperty("sourceName"));
      lengths.add(((Number) row.getProperty("len")).longValue());
    }
    assertThat(names).containsExactly("A", "B", "C");
    assertThat(lengths).containsExactly(3L, 2L, 1L);
  }

  @Test
  void chainedVariableLengthPaths() {
    // Path variable spans two consecutive variable-length segments
    final ResultSet result = database.command("opencypher",
        """
        MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..2]->(b:Worker)-[:REPORTS_TO*1..2]->(c:Worker {name: 'D'}) \
        RETURN length(p) AS len, b.name AS midName \
        ORDER BY midName""");

    // From A to D, length must be 3 in every match (always 3 hops total)
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat(((Number) row.getProperty("len")).longValue()).isEqualTo(3L);
    }
  }
}
