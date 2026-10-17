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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10215: a SQL MATCH pattern {@code {type: V, as: a}.outE('R2'){as: e}.inV(){type: A, as: b}} also returned edges that
 * are not {@code R2} when the planner started from the typed end node {@code b} and walked both steps backwards. Reversed,
 * {@code {e}.inV(){b}} became {@code inE()} (any edge type) and {@code {a}.outE('R2'){e}} became {@code outV('R2')}, where the
 * edge type means nothing: no step checked that the edge bound to {@code e} is an {@code R2}.
 * <p>
 * Graph: {@code C1 -R2-> A2} and {@code C1 -R-> A3}, {@code A} and {@code C} sub types of {@code V}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10215MatchReverseEdgeTypeTest extends TestHelper {

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE VERTEX TYPE A EXTENDS V");
    database.command("sql", "CREATE VERTEX TYPE C EXTENDS V");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE EDGE TYPE R2");
    database.command("sql", "CREATE EDGE TYPE R2Sub EXTENDS R2");
    database.transaction(() -> {
      final MutableVertex c1 = database.newVertex("C").set("id", 1).save();
      final MutableVertex a2 = database.newVertex("A").set("id", 2).save();
      final MutableVertex a3 = database.newVertex("A").set("id", 3).save();
      c1.newEdge("R2", a2).save();
      c1.newEdge("R", a3).save();
    });
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> rows.add(
          r.<Object>getProperty("a") + "-" + r.<Object>getProperty("t") + "->" + r.<Object>getProperty("b")));
    }
    rows.sort(null);
    return rows;
  }

  @Test
  void aTypedEndNodeDoesNotLetEdgesOfAnotherTypeThrough() {
    assertThat(rows("MATCH {type: V, as: a}.outE('R2'){as: e}.inV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2");
    assertThat(rows("MATCH {type: V, as: a}.outE('R2'){as: e}.inV(){as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2");
  }

  /** The same three shapes with the edge read from its target: {@code inE()} reversed is {@code inV()} with the types dropped. */
  @Test
  void theIncomingAndTheBothDirectionsToo() {
    assertThat(rows("MATCH {type: A, as: b}.inE('R2'){as: e}.outV(){type: C, as: a} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2");
    // bothV() answers BOTH ends of the edge, the one the walk came from included: A2 reaches itself through its own R2 edge
    assertThat(rows("MATCH {type: V, as: a}.bothE('R2'){as: e}.bothV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2", "2-R2->2");
    assertThat(rows("MATCH {type: V, as: a}.bothE('R2'){as: e}.bothV(){as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->1", "1-R2->2", "2-R2->1", "2-R2->2");
  }

  /** The edge type is matched polymorphically, as the forward walk does. */
  @Test
  void aSubTypeOfTheEdgeTypeStillMatches() {
    database.transaction(() -> database.command("sql",
        "CREATE EDGE R2Sub FROM (SELECT FROM C WHERE id = 1) TO (SELECT FROM A WHERE id = 3)"));

    assertThat(rows("MATCH {type: V, as: a}.outE('R2'){as: e}.inV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R2->2", "1-R2Sub->3");
  }

  /** Several edge types, as the method accepts them. */
  @Test
  void severalEdgeTypes() {
    assertThat(rows("MATCH {type: V, as: a}.outE('R2', 'R'){as: e}.inV(){type: A, as: b} RETURN a.id AS a, b.id AS b, e.@type AS t"))
        .containsExactly("1-R->3", "1-R2->2");
  }
}
