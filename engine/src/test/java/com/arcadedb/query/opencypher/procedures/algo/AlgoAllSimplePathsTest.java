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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the algo.allSimplePaths Cypher procedure.
 * <p>
 * Reproduces issue #3937 - optional 5th argument (options map) with
 * {@code skipRelTypes} to exclude specific relationship types from the traversal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoAllSimplePathsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-allsimplepaths");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Gateway");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("FRIEND");
    database.getSchema().createEdgeType("WORKS_WITH");

    // Graph:
    //   A -[KNOWS]-> B -[KNOWS]-> D
    //   A -[FRIEND]-> C -[FRIEND]-> D
    //   A -[KNOWS]-> C -[KNOWS]-> D   (additional KNOWS path via C)
    //   B -[WORKS_WITH]-> C
    //   A -[KNOWS]-> G -[KNOWS]-> D   (G is a Gateway vertex acting as a barrier)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      final MutableVertex g = database.newVertex("Gateway").set("name", "G").save();

      a.newEdge("KNOWS", b).save();
      b.newEdge("KNOWS", d).save();

      a.newEdge("FRIEND", c).save();
      c.newEdge("FRIEND", d).save();

      a.newEdge("KNOWS", c).save();
      c.newEdge("KNOWS", d).save();

      b.newEdge("WORKS_WITH", c).save();

      a.newEdge("KNOWS", g).save();
      g.newEdge("KNOWS", d).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void allSimplePathsFindsAllPathsWithoutSkip() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5) YIELD path \
        RETURN path""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Expected simple paths from A to D: at least the 3 direct 2-hop paths plus multi-hop variations
    assertThat(results.size()).isGreaterThanOrEqualTo(3);
  }

  @Test
  @SuppressWarnings("unchecked")
  void allSimplePathsSkipsRelTypeViaOptions() {
    // With skipRelTypes = ['FRIEND'] the FRIEND-edge path A-[FRIEND]->C-[FRIEND]->D must disappear
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {skipRelTypes: ['FRIEND']}) YIELD path \
        RETURN path""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();

    for (final Result r : results) {
      final Map<String, Object> path = (Map<String, Object>) r.getProperty("path");
      final List<Object> rels = (List<Object>) path.get("relationships");
      for (final Object rel : rels) {
        final String typeName = rel instanceof Edge edge ? edge.getTypeName() : null;
        assertThat(typeName).isNotEqualTo("FRIEND");
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void allSimplePathsSkipsMultipleRelTypes() {
    // Skipping both FRIEND and WORKS_WITH must leave only KNOWS paths
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {skipRelTypes: ['FRIEND','WORKS_WITH']}) YIELD path \
        RETURN path""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();

    for (final Result r : results) {
      final Map<String, Object> path = (Map<String, Object>) r.getProperty("path");
      final List<Object> rels = (List<Object>) path.get("relationships");
      for (final Object rel : rels) {
        final String typeName = rel instanceof Edge edge ? edge.getTypeName() : null;
        assertThat(typeName).isEqualTo("KNOWS");
      }
    }
  }

  @Test
  void allSimplePathsSkipAcceptsSingleStringValue() {
    // skipRelTypes should accept a single string in addition to a list
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {skipRelTypes: 'FRIEND'}) YIELD path \
        RETURN path""");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
  }

  @Test
  void allSimplePathsEmptyOptionsBehavesAsFourArgCall() {
    final ResultSet rsNoOpts = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5) YIELD path \
        RETURN path""");
    int countNoOpts = 0;
    while (rsNoOpts.hasNext()) {
      rsNoOpts.next();
      countNoOpts++;
    }

    final ResultSet rsEmptyOpts = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {}) YIELD path \
        RETURN path""");
    int countEmptyOpts = 0;
    while (rsEmptyOpts.hasNext()) {
      rsEmptyOpts.next();
      countEmptyOpts++;
    }

    assertThat(countEmptyOpts).isEqualTo(countNoOpts);
  }

  @Test
  @SuppressWarnings("unchecked")
  void allSimplePathsSkipsVertexTypeViaOptions() {
    // With skipVertexTypes = ['Gateway'] the path A-[KNOWS]->G(Gateway)-[KNOWS]->D must disappear
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {skipVertexTypes: ['Gateway']}) YIELD path \
        RETURN path""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();

    for (final Result r : results) {
      final Map<String, Object> path = (Map<String, Object>) r.getProperty("path");
      final List<Object> nodes = (List<Object>) path.get("nodes");
      for (final Object node : nodes) {
        final String typeName = node instanceof Vertex v ? v.getTypeName() : null;
        assertThat(typeName).isNotEqualTo("Gateway");
      }
    }
  }

  @Test
  void allSimplePathsSkipVertexTypePrunesGatewayPath() {
    // Count of paths must drop by exactly one (the single Gateway path) when Gateway is skipped
    final ResultSet rsAll = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5) YIELD path \
        RETURN path""");
    int countAll = 0;
    while (rsAll.hasNext()) {
      rsAll.next();
      countAll++;
    }

    final ResultSet rsSkip = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {skipVertexTypes: 'Gateway'}) YIELD path \
        RETURN path""");
    int countSkip = 0;
    while (rsSkip.hasNext()) {
      rsSkip.next();
      countSkip++;
    }

    assertThat(countSkip).isEqualTo(countAll - 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void allSimplePathsSkipsBothVertexAndRelTypes() {
    // Combining skipRelTypes and skipVertexTypes must apply both filters
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
        CALL algo.allSimplePaths(a, d, ['KNOWS','FRIEND','WORKS_WITH'], 5, {skipRelTypes: ['FRIEND'], skipVertexTypes: ['Gateway']}) YIELD path \
        RETURN path""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();

    for (final Result r : results) {
      final Map<String, Object> path = (Map<String, Object>) r.getProperty("path");
      final List<Object> rels = (List<Object>) path.get("relationships");
      for (final Object rel : rels)
        assertThat(rel instanceof Edge edge ? edge.getTypeName() : null).isNotEqualTo("FRIEND");

      final List<Object> nodes = (List<Object>) path.get("nodes");
      for (final Object node : nodes)
        assertThat(node instanceof Vertex v ? v.getTypeName() : null).isNotEqualTo("Gateway");
    }
  }

  @Test
  void allSimplePathsRejectsNonMapFifthArg() {
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:Person {name:'A'}), (d:Person {name:'D'}) \
          CALL algo.allSimplePaths(a, d, ['KNOWS'], 5, 'not-a-map') YIELD path \
          RETURN path""");
      while (rs.hasNext())
        rs.next();
    }).hasStackTraceContaining("options must be a map");
  }
}
