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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12177: {@code EDGE PROPERTIES (w)} on an edge property the schema does not declare created a view that listed
 * {@code w} among its edge properties and held no column of it, so every call weighted by {@code w} read the edge records as
 * if there were no view, and nothing said so. Such a view is now refused, naming the property and how to declare it.
 */
class GavEdgePropertyDeclarationTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE EDGE TYPE S");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("k", 0).save();
      final MutableVertex b = database.newVertex("N").set("k", 1).save();
      a.newEdge("R", b, "w", 3);
      b.newEdge("S", a, "w", 2.5);
    });
  }

  @Test
  void anUndeclaredEdgePropertyIsRefusedByTheStatement() {
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) EDGE PROPERTIES (w) UPDATE MODE OFF"))
        .hasMessageContaining("edge property 'w' is not declared on R").hasMessageContaining("CREATE PROPERTY R.w");
    assertNoView();
  }

  @Test
  void anUndeclaredEdgePropertyIsRefusedByTheBuilder() {
    assertThatThrownBy(() -> GraphAnalyticalView.builder(database).withName("gv").withVertexTypes("N").withEdgeTypes("R")
        .withEdgeProperties("w").build()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("edge property 'w' is not declared on R");
    assertNoView();
  }

  @Test
  void anEdgePropertyDeclaredWithATypeNoColumnHoldsIsRefused() {
    database.command("sql", "CREATE PROPERTY R.at DATETIME");
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) EDGE PROPERTIES (at)"))
        .hasMessageContaining("edge property 'at' is not declared on R");
    assertNoView();
  }

  @Test
  void anEdgePropertyDeclaredWithTwoTypesIsRefused() {
    database.command("sql", "CREATE PROPERTY R.w INTEGER");
    database.command("sql", "CREATE PROPERTY S.w DOUBLE");
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R, S) EDGE PROPERTIES (w)"))
        .hasMessageContaining("edge property 'w' is not declared on R, S");
    assertNoView();
  }

  /** A third edge type declaring the first type again does not make the conflict with the second one go away. */
  @Test
  void aTypeConflictStaysAcrossALaterEdgeType() {
    database.command("sql", "CREATE EDGE TYPE T");
    database.command("sql", "CREATE PROPERTY R.w INTEGER");
    database.command("sql", "CREATE PROPERTY S.w DOUBLE");
    database.command("sql", "CREATE PROPERTY T.w INTEGER");
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R, S, T) EDGE PROPERTIES (w)"))
        .hasMessageContaining("edge property 'w' is not declared on R, S, T");
    assertNoView();
  }

  /** Every property the filter names is reported at once, sorted, so a second round trip does not find the next one. */
  @Test
  void everyUndeclaredEdgePropertyIsNamedInOneRefusal() {
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) EDGE PROPERTIES (w, a)"))
        .hasMessageContaining("edge properties 'a', 'w' are not declared on R").hasMessageContaining("CREATE PROPERTY R.a");
    assertNoView();
  }

  /** One declared property does not carry another one the schema does not declare along with it. */
  @Test
  void onlyTheUndeclaredEdgePropertyIsNamed() {
    database.command("sql", "CREATE PROPERTY R.w INTEGER");
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) EDGE PROPERTIES (w, cost)"))
        .hasMessageContaining("edge property 'cost' is not declared on R").hasMessageNotContaining("'w'");
    assertNoView();
  }

  /** The weight of a contraction hierarchy is an edge property of the view too: without a column it reads the edge records. */
  @Test
  void aContractionHierarchyWeightThatIsNotDeclaredIsRefused() {
    assertThatThrownBy(() -> database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) UPDATE MODE SYNCHRONOUS CCH (w)"))
        .hasMessageContaining("edge property 'w' is not declared on R").hasMessageContaining("CREATE PROPERTY R.w");
    assertNoView();

    assertThatThrownBy(() -> GraphAnalyticalView.builder(database).withName("gv").withVertexTypes("N").withEdgeTypes("R")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withContractionHierarchy("w").build())
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("edge property 'w' is not declared on R");
    assertNoView();
  }

  @Test
  void aDeclaredContractionHierarchyWeightIsAccepted() throws Exception {
    database.command("sql", "CREATE PROPERTY R.w DOUBLE");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) UPDATE MODE SYNCHRONOUS CCH (w)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gv");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgePropertyFilter()).containsExactly("w");
    assertThat(view.hasEdgeProperty("R", "w")).isTrue();
  }

  /** A view that names no edge type covers every one of them, and is refused when none of them declares the property. */
  @Test
  void aViewOverEveryEdgeTypeNeedsOneEdgeTypeToDeclareTheProperty() throws Exception {
    assertThatThrownBy(() -> database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE PROPERTIES (w)"))
        .hasMessageContaining("edge property 'w' is not declared on any edge type").hasMessageContaining("CREATE PROPERTY <EdgeType>.w");
    assertNoView();

    database.command("sql", "CREATE PROPERTY S.w DOUBLE");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE PROPERTIES (w)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gv");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.hasEdgeProperty("S", "w")).isTrue();
  }

  /** The Cypher procedure goes through the same builder as the statement. */
  @Test
  void anUndeclaredEdgePropertyIsRefusedByTheCypherProcedure() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL db.analyticalView.create('gv', ['N'], ['R'], {edgeProperties: ['w']})").close())
        .hasMessageContaining("edge property 'w' is not declared on R");
    assertNoView();
  }

  /** The fix the issue measured: once declared, the view holds the column the weighted calls read. */
  @Test
  void aDeclaredEdgePropertyGetsItsColumn() throws Exception {
    database.command("sql", "CREATE PROPERTY R.w INTEGER");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R, S) EDGE PROPERTIES (w)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gv");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.hasEdgeProperties()).isTrue();
    assertThat(view.getEdgePropertyFilter()).containsExactly("w");
    assertThat(view.hasEdgeProperty("R", "w")).as("the column the weighted calls read").isTrue();
  }

  /**
   * A saved view whose edge property lost its declaration is restored as it was defined, not refused: a refusal on open would
   * drop the definition.
   */
  @Test
  void aSavedViewWhosePropertyLostItsDeclarationIsStillRestored() throws Exception {
    database.command("sql", "CREATE PROPERTY R.w INTEGER");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R) EDGE PROPERTIES (w)");
    assertThat(GraphAnalyticalViewRegistry.get(database, "gv").awaitReady(60, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "DROP PROPERTY R.w");

    reopenDatabase();

    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gv");
    assertThat(view).isNotNull();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgePropertyFilter()).containsExactly("w");
  }

  private void assertNoView() {
    assertThat(GraphAnalyticalViewRegistry.get(database, "gv")).isNull();
    try (final ResultSet rows = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = 'gv'")) {
      assertThat(rows.hasNext()).isFalse();
    }
  }
}
