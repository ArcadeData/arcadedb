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

  /** The fix the issue measured: once declared, the view holds the column the weighted calls read. */
  @Test
  void aDeclaredEdgePropertyGetsItsColumn() throws Exception {
    database.command("sql", "CREATE PROPERTY R.w INTEGER");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (N) EDGE TYPES (R, S) EDGE PROPERTIES (w)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gv");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.hasEdgeProperties()).isTrue();
    assertThat(view.getEdgePropertyFilter()).containsExactly("w");
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
