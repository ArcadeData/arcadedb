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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10435: SQL MATCH over a Graph Analytical View holding several vertex types returned rows that break the pattern in
 * chains of three or more hops. The fused walk of the consecutive hops bound the nodes it reached without looking at the type the
 * pattern gave them, so {@code {type: C}} on the third node or a later one let other types through (the hops before it were
 * checked by the regular step); since #10212 gave a view over a type its sub-types, a typed second node of a three-hop chain
 * returned paths that do not exist. Every chain is answered with the view and again after {@code DROP GRAPH ANALYTICAL VIEW},
 * and the rows must be the same.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10435SqlMatchViewMultiTypeTest extends TestHelper {

  private static final String[] QUERIES = {//
      "MATCH {type: V, as: a}.out('R'){type: C, as: b} RETURN a, b",//
      "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){type: C, as: c} RETURN a, b, c",//
      "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: C, as: d} RETURN a, b, c, d",//
      "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){as: d}.out('R'){type: C, as: e} RETURN a, b, c, d, e",//
      "MATCH {type: V, as: a}.out('R'){type: C, as: b}.out('R'){as: c}.out('R'){as: d} RETURN a, b, c, d",//
      "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){type: C, as: c}.out('R'){as: d} RETURN a, b, c, d",//
      "MATCH {type: V, as: a}.in('R'){as: b}.in('R'){as: c}.in('R'){type: C, as: d} RETURN a, b, c, d",//
      "MATCH {type: V, as: a}.both('R'){as: b}.both('R'){as: c}.both('R'){type: C, as: d} RETURN a, b, c, d",//
      "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: A, as: d} RETURN a, b, c, d",//
      "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){as: d, where: (k = 1)} RETURN a, b, c, d",//
      "MATCH {type: C, as: a}.out('R'){type: C, as: b}.out('R'){type: C, as: c}.out('R'){type: C, as: d} RETURN a, b, c, d",//
      "MATCH {type: A, as: a}.out('R'){type: B, as: b}.out('R'){type: V, as: c}.out('R'){type: C, as: d} RETURN a, b, c, d",//
  };

  private void schema(final boolean subTypes) {
    if (subTypes) {
      sql("CREATE VERTEX TYPE V");
      sql("CREATE VERTEX TYPE A EXTENDS V");
      sql("CREATE VERTEX TYPE B EXTENDS A");
      sql("CREATE VERTEX TYPE C EXTENDS V");
    } else {
      sql("CREATE VERTEX TYPE V");
      sql("CREATE VERTEX TYPE A");
      sql("CREATE VERTEX TYPE B");
      sql("CREATE VERTEX TYPE C");
    }
    sql("CREATE PROPERTY V.k LONG");
    sql("CREATE EDGE TYPE R");
  }

  private void sql(final String statement) {
    database.command("sql", statement).close();
  }

  private void fill() {
    final String[] types = { "V", "A", "B", "C" };
    final Random random = new Random(3);
    final List<RID> vertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        vertices.add(database.newVertex(types[i % 4]).set("k", (long) (i % 3)).save().getIdentity());
      for (int i = 0; i < 180; i++)
        vertices.get(random.nextInt(60)).asVertex().newEdge("R", vertices.get(random.nextInt(60)).asVertex());
    });
  }

  private void createView(final String vertexTypes, final String mode) {
    sql("CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES " + vertexTypes + " EDGE TYPES (R) PROPERTIES (k) UPDATE MODE " + mode);
    assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  /** The rows of a chain, each as the RIDs of its aliases, sorted. */
  private List<String> rows(final String query) {
    final String[] aliases = query.substring(query.indexOf("RETURN ") + "RETURN ".length()).split(",\\s*");
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String alias : aliases)
          sb.append(((Identifiable) row.getProperty(alias.trim())).getIdentity()).append(' ');
        rows.add(sb.toString());
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private void assertSameWithAndWithoutTheView(final String vertexTypes, final String mode) {
    createView(vertexTypes, mode);
    final List<List<String>> withView = new ArrayList<>();
    for (final String query : QUERIES)
      withView.add(rows(query));

    sql("DROP GRAPH ANALYTICAL VIEW g");

    for (int i = 0; i < QUERIES.length; i++)
      assertThat(withView.get(i)).as(QUERIES[i]).isEqualTo(rows(QUERIES[i]));
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void unrelatedTypesAllInTheView(final String mode) {
    schema(false);
    fill();
    assertSameWithAndWithoutTheView("(V, A, B, C)", mode);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void subTypesBroughtByTheViewOverTheSuperType(final String mode) {
    schema(true);
    fill();
    assertSameWithAndWithoutTheView("(V)", mode);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void subTypesListedOneByOne(final String mode) {
    schema(true);
    fill();
    assertSameWithAndWithoutTheView("(V, A, B, C)", mode);
  }

  /** The report's smallest case: the one {@code C} vertex is no reason for the three-hop chain to end on an {@code A}. */
  @Test
  void aTypeOnTheLastNodeOfAThreeHopChainIsRespected() {
    sql("CREATE VERTEX TYPE V");
    sql("CREATE VERTEX TYPE A");
    sql("CREATE VERTEX TYPE C");
    sql("CREATE EDGE TYPE R");
    database.transaction(() -> database.command("opencypher",
        "CREATE (v1:V), (a1:A), (a2:A), (a3:A), (c:C), (v1)-[:R]->(a1), (a1)-[:R]->(a2), (a2)-[:R]->(a3)").close());
    createView("(V, A, C)", "SYNCHRONOUS");

    final String chain = "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: C, as: d} RETURN a, b, c, d";
    assertThat(rows(chain)).as("d is an A, not a C").isEmpty();
    assertThat(rows("MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: A, as: d} RETURN a, b, c, d")).hasSize(1);
  }
}
