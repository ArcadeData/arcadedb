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
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CALL subqueries (SubqueryStep) against Neo4j: what a UNION body imports with WITH *, and the columns an OPTIONAL CALL answers when its body returns no row. Every expected row and statistic is what Neo4j 5.26 returns for the same data and statement. A row is rendered
 * {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherSubqueryImportsOracleTest extends TestHelper {
  private QueryStatistics lastStatistics;

  /** Runs one statement in its own transaction and renders its rows. */
  private List<String> run(final String statement) {
    return run(statement, Map.of());
  }

  private List<String> run(final String statement, final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement, params)) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
        lastStatistics = rs.getStatistics().orElse(null);
      }
    });
    return rows;
  }

  private void assertStats(final int propertiesSet, final int nodesCreated, final int relationshipsCreated, final int labelsAdded) {
    assertThat(lastStatistics).isNotNull();
    assertThat(lastStatistics.getPropertiesSet()).as("properties set").isEqualTo(propertiesSet);
    assertThat(lastStatistics.getNodesCreated()).as("nodes created").isEqualTo(nodesCreated);
    assertThat(lastStatistics.getRelationshipsCreated()).as("relationships created").isEqualTo(relationshipsCreated);
    assertThat(lastStatistics.getLabelsAdded()).as("labels added").isEqualTo(labelsAdded);
  }

  private static Map<String, Object> map(final Object... keysAndValues) {
    final Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      m.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return m;
  }

  private static List<Object> list(final Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }

  private static String render(final Result row) {
    final Map<String, Object> sorted = new TreeMap<>();
    for (final String name : row.getPropertyNames())
      sorted.put(name, row.getProperty(name));
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, Object> e : sorted.entrySet()) {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append(e.getKey()).append('=').append(value(e.getValue()));
    }
    return sb.toString();
  }

  private static String value(final Object o) {
    if (o == null)
      return "null";
    if (o instanceof Number n)
      return n.doubleValue() == Math.rint(n.doubleValue()) && !(o instanceof Double || o instanceof Float) ? String.valueOf(n.longValue())
          : String.valueOf(n.doubleValue());
    if (o instanceof Map<?, ?> m) {
      final StringBuilder sb = new StringBuilder("{");
      for (final Map.Entry<?, ?> e : new TreeMap<>(m).entrySet())
        sb.append(sb.length() > 1 ? ", " : "").append(e.getKey()).append(": ").append(value(e.getValue()));
      return sb.append('}').toString();
    }
    if (o instanceof Collection<?> c) {
      final List<String> parts = new ArrayList<>();
      for (final Object x : c)
        parts.add(value(x));
      return parts.toString();
    }
    return String.valueOf(o);
  }

  @Test
  void aUnionBranchImportingWithStarSeesEveryOuterVariable() {
    run("UNWIND [1, 2] AS i CREATE (:N {i: i})");
    assertThat(run("MATCH (n:N) WITH n CALL { WITH * RETURN 1 AS r UNION WITH n RETURN 2 AS r } RETURN n.i AS i, r ORDER BY i, r")).containsExactly("i=1, r=1", "i=1, r=2", "i=2, r=1", "i=2, r=2");
    assertThat(run("MATCH (n:N) WITH n CALL { WITH * RETURN n.i AS r UNION WITH n RETURN n.i * 10 AS r } RETURN n.i AS i, r ORDER BY i, r")).containsExactly("i=1, r=1", "i=1, r=10", "i=2, r=2", "i=2, r=20");
  }

  @Test
  void aStarImportInTheLastBranchOfAUnionStillSeesTheVariablesTheFirstBranchDoesNotImport() {
    run("CREATE (:N {i: 1}), (:M {i: 5})");
    assertThat(run("MATCH (n:N), (m:M) WITH n, m CALL { WITH n RETURN n.i AS r UNION WITH * RETURN m.i AS r } RETURN n.i AS ni, m.i AS mi, r ORDER BY r")).containsExactly("mi=5, ni=1, r=1", "mi=5, ni=1, r=5");
  }

  @Test
  void anOptionalCallOverAUnionBodyThatAnswersNothingKeepsEveryColumnOfTheBodyAsNull() {
    run("UNWIND [1, 2] AS i CREATE (:N {i: i})");
    assertThat(run("MATCH (n:N) OPTIONAL CALL (n) { MATCH (m:M) WHERE m.k = n.i RETURN m.k AS k, m.j AS j UNION MATCH (m:M) WHERE m.k = -n.i RETURN m.k AS k, m.j AS j } RETURN n.i AS i, k, j ORDER BY i")).containsExactly("i=1, j=null, k=null", "i=2, j=null, k=null");
  }

  @Test
  void anOptionalCallOverAStarReturnKeepsTheColumnsOfTheScope() {
    run("UNWIND [1, 2] AS i CREATE (:N {i: i})");
    assertThat(run("MATCH (n:N) OPTIONAL CALL (n) { MATCH (m:M) WHERE m.k = n.i WITH m, n.i AS x RETURN * } RETURN n.i AS i, m, x ORDER BY i")).containsExactly("i=1, m=null, x=null", "i=2, m=null, x=null");
  }

  @Test
  void anOptionalCallOverAUnionOfStarReturnsKeepsTheColumnsOfTheScopeAsNull() {
    run("UNWIND [1, 2] AS i CREATE (:N {i: i})");
    assertThat(run("MATCH (n:N) OPTIONAL CALL { MATCH (m:M) RETURN * UNION MATCH (m:M) RETURN * } RETURN n.i AS i, m ORDER BY i")).containsExactly("i=1, m=null", "i=2, m=null");
    assertThat(run("MATCH (n:N) OPTIONAL CALL { MATCH (m:M) WITH m, 5 AS q RETURN * UNION MATCH (m:M) WITH m, 5 AS q RETURN * } RETURN n.i AS i, m, q ORDER BY i")).containsExactly("i=1, m=null, q=null", "i=2, m=null, q=null");
  }

  @Test
  void anOptionalCallOverABodyThatWritesAndAnswersNothingKeepsEveryColumnAsNull() {
    run("UNWIND [1, 2] AS i CREATE (:N {i: i})");
    assertThat(run("MATCH (n:N) OPTIONAL CALL (n) { MATCH (m:M) WHERE m.k = n.i SET m.x = 1 RETURN m.k AS k } RETURN n.i AS i, k ORDER BY i")).containsExactly("i=1, k=null", "i=2, k=null");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n:N) CALL (n) { MATCH (m:M) WHERE m.k = n.i SET m.x = 1 RETURN m.k AS k } RETURN n.i AS i, k ORDER BY i")).isEmpty();
    assertStats(0, 0, 0, 0);
  }
}
