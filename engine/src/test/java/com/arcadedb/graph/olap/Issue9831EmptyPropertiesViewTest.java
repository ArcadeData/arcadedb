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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9831: {@code CREATE GRAPH ANALYTICAL VIEW ... PROPERTIES ()} creates a view without any vertex property, as
 * {@code GraphAnalyticalViewBuilder.withProperties()} with an empty array does. No {@code PROPERTIES} clause still
 * copies every vertex property.
 */
class Issue9831EmptyPropertiesViewTest extends TestHelper {
  private RID a;

  @BeforeEach
  void createGraph() {
    database.getSchema().createVertexType("P");
    // an edge property reaches the view only when the schema declares it
    database.getSchema().createEdgeType("K").createProperty("w", Type.DOUBLE);
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("P").set("name", "a", "age", 1).save();
      final MutableVertex vb = database.newVertex("P").set("name", "b", "age", 2).save();
      final MutableVertex vc = database.newVertex("P").set("name", "c", "age", 3).save();
      va.newEdge("K", vb, "w", 1.5);
      vb.newEdge("K", vc, "w", 2.5);
      a = va.getIdentity();
    });
  }

  @Test
  void emptyPropertiesCreatesAViewWithoutPropertyColumns() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () UPDATE MODE OFF");
    final GraphAnalyticalView view = awaitView("v");

    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.getNodeCount()).isEqualTo(3);
    assertThat(view.getEdgeCount()).isEqualTo(2);
    assertThat(view.getPropertyFilter()).isEmpty();
    assertThat(view.getColumnStore()).isNull();
    assertThat(view.getProperty(view.getNodeId(a), "name")).isNull();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = 'v'")) {
      final List<?> propertyFilter = rs.next().getProperty("propertyFilter");
      assertThat(propertyFilter).isNotNull().isEmpty();
    }
  }

  @Test
  void noClauseCopiesEveryPropertyAndAListCopiesOnlyItsOwn() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW vAll");
    final GraphAnalyticalView all = awaitView("vAll");
    assertThat(all.getPropertyFilter()).isNull();
    assertThat(all.getProperty(all.getNodeId(a), "name")).isEqualTo("a");
    assertThat(all.getProperty(all.getNodeId(a), "age")).isEqualTo(1);

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW vName PROPERTIES (name)");
    final GraphAnalyticalView name = awaitView("vName");
    assertThat(name.getPropertyFilter()).containsExactly("name");
    assertThat(name.getProperty(name.getNodeId(a), "name")).isEqualTo("a");
    assertThat(name.getProperty(name.getNodeId(a), "age")).isNull();
  }

  @Test
  void emptyPropertiesSurviveAReopen() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () UPDATE MODE OFF");
    awaitView("v");

    reopenDatabase();

    final GraphAnalyticalView restored = awaitView("v");
    assertThat(restored.isRestoredFromPersistedCsr()).isTrue();
    assertThat(restored.getNodeCount()).isEqualTo(3);
    assertThat(restored.getEdgeCount()).isEqualTo(2);
    assertThat(restored.getPropertyFilter()).isEmpty();
    assertThat(restored.getColumnStore()).isNull();
  }

  @Test
  void emptyPropertiesRoundTripsThroughTheParser() {
    final String sql = "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () EDGE PROPERTIES (w)";
    final Statement statement = new SQLAntlrParser(null).parse(sql);
    assertThat(statement).isInstanceOf(CreateGraphAnalyticalViewStatement.class);
    assertThat(((CreateGraphAnalyticalViewStatement) statement).properties).isEmpty();
    assertThat(statement.toString()).isEqualTo(sql);

    final Statement reparsed = new SQLAntlrParser(null).parse(statement.toString());
    assertThat(reparsed).isEqualTo(statement);
    assertThat(statement.copy()).isEqualTo(statement);
    assertThat(statement.copy().toString()).isEqualTo(sql);

    // an empty list and no list at all are different statements, the statement cache must not mix them up
    final Statement noClause = new SQLAntlrParser(null).parse("CREATE GRAPH ANALYTICAL VIEW v EDGE PROPERTIES (w)");
    assertThat(((CreateGraphAnalyticalViewStatement) noClause).properties).isNull();
    assertThat(noClause).isNotEqualTo(statement);
  }

  @Test
  void edgePropertiesStillWorkNextToEmptyProperties() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () EDGE PROPERTIES (w)");
    final GraphAnalyticalView view = awaitView("v");

    assertThat(view.getColumnStore()).isNull();
    assertThat(view.getEdgePropertyFilter()).containsExactly("w");
    assertThat(view.getEdgeColumnStore("K").getColumn("w")).isNotNull();
  }

  @Test
  void algorithmRunsOnAViewWithoutProperties() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () UPDATE MODE OFF");
    awaitView("v");

    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node.name AS name ORDER BY score DESC")) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    assertThat(names).containsExactlyInAnyOrder("a", "b", "c");

    try (final ResultSet rs = database.command("opencypher",
        "PROFILE CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score")) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("CSR-accelerated via Graph Analytical View");
    }
  }

  private GraphAnalyticalView awaitView(final String name) {
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    assertThat(view).isNotNull();
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    return view;
  }
}
