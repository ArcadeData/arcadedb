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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10442 (follow-up of #10440): {@code g.V().hasLabel('Ev')} over a TIMESERIES type {@code Ev} answered no vertex, and
 * {@code g.V().hasLabel('Ev').count()} answered 0, while SQL counts the samples. Time-series samples are neither vertices nor
 * edges, so a {@code hasLabel()} naming the type now fails with an error naming it, as the Cypher pattern does since #10440.
 */
class GremlinTimeSeriesLabelIssue10442Test {
  private static final String MESSAGE = "'Ev', a TIMESERIES type";
  private static final String DB_PATH = "./target/testgremlin-timeseries-10442";

  private ArcadeGraph graph;

  @BeforeEach
  void setUp() {
    // a directory left by a crashed run would be opened, and the type below created twice
    FileUtils.deleteRecursively(new File(DB_PATH));
    graph = ArcadeGraph.open(DB_PATH);
    final BasicDatabase database = graph.getDatabase();
    database.command("sqlscript", """
        CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE);
        INSERT INTO Ev SET ts = 1792396800000, k = 'a', v = 1.0;
        INSERT INTO Ev SET ts = 1792396860000, k = 'a', v = 2.0;
        """);
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      final ArcadeVertex x = graph.addVertex(T.label, "P", "name", "x");
      final ArcadeVertex y = graph.addVertex(T.label, "P", "name", "y");
      x.addEdge("R", y);
    });
  }

  @AfterEach
  void tearDown() {
    if (graph != null) {
      if (graph.tx().isOpen())
        graph.tx().rollback();
      graph.drop();
      graph = null;
    }
  }

  @Test
  void sqlStillSeesTheSamples() {
    try (final ResultSet rs = graph.getDatabase().query("sql", "SELECT FROM Ev")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // the queries of the issue: the count rewrite (ArcadeCountGlobalStep) and the type scan (ArcadeFilterByTypeStep)
      "g.V().hasLabel('Ev').count()",
      "g.V().hasLabel('Ev')",
      // a property filter next to the label: the index/filter branch of the rewrite
      "g.V().hasLabel('Ev').has('k', 'a')",
      "g.V().has('Ev', 'k', 'a')",
      // more than one label is a within() predicate, never rewritten: the label is checked on each vertex of a full scan
      "g.V().hasLabel('Ev', 'P').count()",
      "g.V().hasLabel(within('Ev', 'P')).count()",
      // the label is checked on vertices reached by an expansion, never scanned
      "g.V().hasLabel('P').out().hasLabel('Ev').count()",
      // inside a child traversal
      "g.V().where(__.out().hasLabel('Ev')).count()",
      "g.V().filter(__.hasLabel('Ev')).count()",
      // the edge side
      "g.E().hasLabel('Ev').count()",
      "g.V().outE().hasLabel('Ev').count()",
      // an edge label of a hop
      "g.V().out('Ev').count()",
      "g.V().outE('Ev').count()",
      "g.V().both('P', 'Ev').count()"
  })
  void hasLabelOnTimeSeriesTypeFails(final String query) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = graph.gremlin(query).execute()) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasStackTraceContaining(MESSAGE);
  }

  @Test
  void hasLabelOnTimeSeriesTypeFailsThroughTheTraversalApi() {
    assertThatThrownBy(() -> graph.traversal().V().hasLabel("Ev").count().next()).hasStackTraceContaining(MESSAGE);
    assertThatThrownBy(() -> graph.traversal().V().hasLabel("Ev").toList()).hasStackTraceContaining(MESSAGE);
    assertThatThrownBy(() -> graph.traversal().V().hasLabel(P.within("Ev")).toList()).hasStackTraceContaining(MESSAGE);
    assertThatThrownBy(() -> graph.traversal().V().hasLabel(P.eq("P").or(P.eq("Ev"))).toList()).hasStackTraceContaining(MESSAGE);
  }

  @Test
  void negatedLabelIsAnsweredNotRefused() {
    // no vertex carries the label, so every vertex passes the negation: a correct answer, not a silent empty one
    assertThat(graph.traversal().V().hasLabel(P.neq("Ev")).count().next()).isEqualTo(2L);
    assertThat(graph.traversal().V().hasLabel(P.without("Ev")).count().next()).isEqualTo(2L);
    assertThat(graph.traversal().V().not(__.hasLabel("Ev")).count().next()).isEqualTo(2L);
    assertThat(graph.traversal().V().not(__.out("Ev")).count().next()).isEqualTo(2L);
    // nested inside the body of not()
    assertThat(graph.traversal().V().not(__.and(__.hasLabel("Ev"), __.has("name", "x"))).count().next()).isEqualTo(2L);
    assertThat(graph.traversal().V().not(__.where(__.out("Ev"))).count().next()).isEqualTo(2L);
    // two negations ask for the label again
    assertThatThrownBy(() -> graph.traversal().V().not(__.not(__.hasLabel("Ev"))).count().next()).hasStackTraceContaining(MESSAGE);
  }

  @Test
  void graphLabelsAreUnaffected() {
    assertThat(graph.traversal().V().hasLabel("P").count().next()).isEqualTo(2L);
    assertThat(graph.traversal().V().count().next()).isEqualTo(2L);
    assertThat(graph.traversal().E().hasLabel("R").count().next()).isEqualTo(1L);
    assertThat(graph.traversal().V().hasLabel("P", "DoesNotExist").count().next()).isEqualTo(2L);
    assertThat(graph.traversal().V().hasLabel("DoesNotExist").count().next()).isEqualTo(0L);
  }

  @Test
  void addVertexOfTimeSeriesTypeFails() {
    // already an error before #10442: the type is not a vertex type
    assertThatThrownBy(() -> graph.traversal().addV("Ev").next()).hasStackTraceContaining("'Ev' is not a vertex");
    assertThatThrownBy(() -> graph.traversal().V().has("name", "x").addE("Ev").to(__.V().has("name", "y")).next())
        .hasStackTraceContaining("'Ev' is not a edge");
  }

  @Test
  void patternPredicateOnTheLabelIsNotRefused() {
    // a documented limit of the guard: a TextP names no type, so it is answered (nothing carries a label starting with 'E')
    assertThat(graph.traversal().V().hasLabel(TextP.startingWith("E")).count().next()).isEqualTo(0L);
  }
}
