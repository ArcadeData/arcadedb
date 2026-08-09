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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5904 reported that SQL {@code expand(both('E'))} and Cypher {@code -[:E]-} disagree on how many
 * times a self-loop is visited: SQL returns the vertex twice, Cypher once. This is not a bug to reconcile -
 * it is two query languages each matching their own reference implementation's documented semantics:
 * <p>
 * - SQL's {@code both()} mirrors Apache TinkerPop/Gremlin's {@code both()} step, defined as the union of
 * {@code out()} and {@code in()}. A self-loop is a member of both sets, so the union legitimately contains
 * it twice - the same behavior any Gremlin-based graph database exhibits.
 * <p>
 * - Cypher's undirected {@code -[]-} hop was deliberately fixed to match Neo4j, which returns each
 * relationship once regardless of direction (issue #5446 / PR #5447 for the cost-based operators
 * {@code ExpandAll}/{@code GAVExpandAll}, issue #5456 for pattern comprehensions, the latter citing
 * verification against Neo4j 2026.05). Reverting that would regress two already Neo4j-verified fixes and
 * the openCypher TCK conformance they preserved.
 * <p>
 * Forcing the two engines to agree would require breaking whichever one currently matches its own reference
 * implementation. This test locks in the divergence as intentional so it is not "fixed" by accident.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5904SqlCypherSelfLoopBothDivergenceTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex d = database.newVertex("N").set("name", "d").save();
      d.newEdge("E", d, "name", "loop").save();
    });
  }

  @Test
  void sqlBothUnionsOutAndInSoASelfLoopIsVisitedTwice() {
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(both('E')) FROM N WHERE name = 'd'")) {
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().<String>getProperty("name"));
      assertThat(names).containsExactly("d", "d");
    }
  }

  @Test
  void sqlOutAndInEachVisitTheSelfLoopOnce() {
    try (final ResultSet rs = database.query("sql", "SELECT expand(out('E')) FROM N WHERE name = 'd'")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT expand(in('E')) FROM N WHERE name = 'd'")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void cypherUndirectedMatchVisitsTheSelfLoopOnceMatchingNeo4j() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:N {name:'d'})-[:E]-(x) RETURN x.name AS name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void cypherDirectedMatchesEachVisitTheSelfLoopOnce() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:N {name:'d'})-[:E]->(x) RETURN x.name AS name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (x)-[:E]->(p:N {name:'d'}) RETURN x.name AS name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
