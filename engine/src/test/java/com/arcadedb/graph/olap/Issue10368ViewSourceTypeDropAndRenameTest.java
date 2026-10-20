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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10368: a Graph Analytical View stores its vertex and edge types by name, in the schema extension it is restored from
 * on every open, yet DROP TYPE and ALTER TYPE ... NAME of one of them went through: the live view kept answering from a CSR
 * that no longer matched the schema, and the next open's build failed on the old name and left the view unbuilt for good, with
 * REBUILD refused because the registry no longer held it. A view is now a dependant like a materialized view: a type it lists
 * cannot be dropped, and a rename of one is carried to its definition and the view rebuilt from it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10368ViewSourceTypeDropAndRenameTest extends TestHelper {
  private final RID[] vertices = new RID[2];

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE VERTEX TYPE R");
    database.command("sql", "CREATE VERTEX TYPE Unlisted");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE EDGE TYPE M");
    database.transaction(() -> {
      final MutableVertex p = database.newVertex("P").set("id", 1).save();
      final MutableVertex q = database.newVertex("Q").set("id", 2).save();
      p.newEdge("L", q);
      vertices[0] = p.getIdentity();
      vertices[1] = q.getIdentity();
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "M", "R", "P", "L" })
  void aTypeTheViewListsCannotBeDropped(final String typeName) {
    createView("SYNCHRONOUS");

    // UNSAFE, so that P and L, which hold records, reach the guard rather than the refusal of a non-empty type
    assertThatThrownBy(() -> database.command("sql", "DROP TYPE " + typeName + " UNSAFE"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("graph analytical view 'gav'")
        .hasMessageContaining("DROP GRAPH ANALYTICAL VIEW gav");
    assertThat(database.getSchema().existsType(typeName)).isTrue();
  }

  @Test
  void anAliasDoesNotWalkPastTheGuard() {
    database.command("sql", "ALTER TYPE M ALIASES Mx");
    createView("SYNCHRONOUS");

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE Mx")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("edge type of graph analytical view 'gav'");
    assertThat(database.getSchema().existsType("M")).isTrue();
  }

  @Test
  void aTypeTheViewDoesNotListCanBeDropped() {
    createView("SYNCHRONOUS");
    database.command("sql", "DROP TYPE Unlisted");
    assertThat(database.getSchema().existsType("Unlisted")).isFalse();
  }

  @Test
  void droppingTheViewReleasesItsTypes() {
    createView("SYNCHRONOUS");
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
    database.command("sql", "DROP TYPE M");
    database.command("sql", "DROP TYPE R");
    assertThat(database.getSchema().existsType("M")).isFalse();
    assertThat(database.getSchema().existsType("R")).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS", "OFF" })
  void aRenamedVertexTypeIsCarriedToTheViewAndSurvivesAReopen(final String mode) {
    createView(mode);

    database.command("sql", "ALTER TYPE Q NAME Q2");

    assertThat(definedTypes("vertexTypes")).containsExactly("P", "Q2", "R");
    GraphAnalyticalView view = awaitView();
    assertThat(view.coversVertexType("Q2")).isTrue();
    assertThat(view.coversVertexType("Q")).isFalse();
    assertThat(view.getEdgeCount()).isEqualTo(1);

    // a vertex of the renamed type and an edge to it reach the view
    database.transaction(() -> vertices[0].asVertex().modify().newEdge("L", database.newVertex("Q2").set("id", 3).save()));
    if (!"OFF".equals(mode)) {
      view = awaitView();
      assertThat(view.getEdgeCount()).isEqualTo(2);
    }

    reopenDatabase();
    view = awaitView();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.coversVertexType("Q2")).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(2);
    assertThat(view.countEdges(view.getNodeId(vertices[0]), Vertex.DIRECTION.OUT, "L")).isEqualTo(2);
  }

  @Test
  void aRenamedEdgeTypeIsCarriedToTheViewAndSurvivesAReopen() {
    createView("SYNCHRONOUS");

    database.command("sql", "ALTER TYPE L NAME L2");

    assertThat(definedTypes("edgeTypes")).containsExactly("L2", "M");
    GraphAnalyticalView view = awaitView();
    assertThat(view.coversEdgeType("L2")).isTrue();
    assertThat(view.countEdges(view.getNodeId(vertices[0]), Vertex.DIRECTION.OUT, "L2")).isEqualTo(1);

    database.transaction(() -> vertices[1].asVertex().modify().newEdge("L2", vertices[0]));
    view = awaitView();
    assertThat(view.getEdgeCount()).isEqualTo(2);

    reopenDatabase();
    view = awaitView();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.getEdgeCount()).isEqualTo(2);
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:P)-[:L2]->(b) RETURN count(*) AS n")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
    }
  }

  /** A rename of a type the view does not list leaves the view as it is: the same instance, still ready. */
  @Test
  void aRenameOfAnotherTypeLeavesTheViewAlone() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS");
    database.command("sql", "ALTER TYPE Unlisted NAME Unlisted2");
    assertThat(GraphAnalyticalViewRegistry.get(database, "gav")).isSameAs(view);
    assertThat(view.isReady()).isTrue();
  }

  /**
   * A definition left naming a type that no longer exists - by a build before this fix - fails safe: a drop cannot be told
   * apart from the drop of the type it meant, so every drop is refused, and REBUILD, which used to be refused because the view
   * was not loaded, reports the reason the build fails.
   */
  @Test
  void aDanglingDefinitionFailsSafe() {
    createView("SYNCHRONOUS");
    final JSONObject all = database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY);
    all.getJSONObject("gav").put("edgeTypes", new JSONArray(List.of("L", "Gone")));
    database.getSchema().setExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY, all);
    GraphAnalyticalViewRegistry.get(database, "gav").shutdown();

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE Unlisted")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("graph analytical view 'gav'").hasMessageContaining("'Gone'").hasMessageContaining("does not exist");

    assertThatThrownBy(() -> database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW gav"))
        .hasMessageContaining("Gone");

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
    database.command("sql", "DROP TYPE Unlisted");
    assertThatThrownBy(() -> database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW gav"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("does not exist");
  }

  /** REBUILD of a view whose build failed on open builds it again from its definition. */
  @Test
  void rebuildRestoresAViewTheRegistryDropped() {
    createView("SYNCHRONOUS");
    GraphAnalyticalViewRegistry.get(database, "gav").shutdown();
    assertThat(GraphAnalyticalViewRegistry.get(database, "gav")).isNull();

    database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW gav").close();

    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view).isNotNull();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.getEdgeCount()).isEqualTo(1);
  }

  private List<Object> definedTypes(final String key) {
    return database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY).getJSONObject("gav").getJSONArray(key)
        .toList();
  }

  private GraphAnalyticalView createView(final String mode) {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q, R) EDGE TYPES (L, M) PROPERTIES (id) UPDATE MODE " + mode).close();
    return awaitView();
  }

  private GraphAnalyticalView awaitView() {
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view).isNotNull();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }
}
