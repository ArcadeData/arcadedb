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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8723: {@code WHERE e.x = $p OR e.x = $q} over an indexed property read every vertex of the label, while the
 * equivalent {@code IN} answered from the index, and an OR across two indexed properties scanned the label where SQL
 * unions two index fetches. Both ORs are now answered from the indexes; the WHERE is still evaluated above the seek, so
 * the answers do not change.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8723CypherOrIndexSeekTest extends TestHelper {
  private static final int VERTICES = 2_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE E");
    database.command("sql", "CREATE PROPERTY E.id LONG");
    database.command("sql", "CREATE PROPERTY E.x INTEGER");
    database.command("sql", "CREATE PROPERTY E.s STRING");
    database.command("sql", "CREATE PROPERTY E.u STRING");
    database.command("sql", "CREATE INDEX ON E (x) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON E (s) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.newVertex("E").set("id", (long) i).set("x", i % 500).set("s", "s" + (i % 400)).set("u", "u" + (i % 7)).save();
    });
  }

  @Test
  void orOfEqualitiesOnOnePropertyUsesTheIndex() {
    final String query = "MATCH (e:E) WHERE e.x = 3 OR e.x = 7 RETURN e.id AS id ORDER BY id";
    assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 500 == 7));
    assertThat(explain(query)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
  }

  @Test
  void orOfEqualitiesWithParametersUsesTheIndex() {
    final String query = "MATCH (e:E) WHERE e.x = $p OR e.x = $q RETURN count(*) AS n";
    assertThat(column(query, Map.of("p", 3, "q", 7))).containsExactly((long) expectedIds(i -> i % 500 == 3 || i % 500 == 7).size());
    assertThat(explain(query)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
  }

  @Test
  void orOfTwoIndexedPropertiesIsAUnionOfSeeks() {
    final String query = "MATCH (e:E) WHERE e.x = 3 OR e.s = 's10' RETURN e.id AS id ORDER BY id";
    assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 400 == 10));
    assertThat(explain(query)).contains("NodeIndexUnionSeek").doesNotContain("NodeByLabelScan");
  }

  @Test
  void aVertexThatSatisfiesBothDisjunctsIsReturnedOnce() {
    // id 1003: x = 3 and s = 's203'; s 's203' also holds id 203 (x = 203)
    final String query = "MATCH (e:E) WHERE e.x = 3 OR e.s = $s RETURN count(*) AS n";
    final long expected = expectedIds(i -> i % 500 == 3 || i % 400 == 3).size();
    assertThat(column(query, Map.of("s", "s3"))).containsExactly(expected);
    assertThat(explain(query)).contains("NodeIndexUnionSeek");
  }

  @Test
  void anInListDisjunctAndAnAndDisjunct() {
    final String query = "MATCH (e:E) WHERE e.x IN [1, 2] OR (e.s = 's9' AND e.u = 'u2') RETURN e.id AS id ORDER BY id";
    assertThat(column(query, Map.of()))
        .containsExactlyElementsOf(expectedIds(i -> i % 500 == 1 || i % 500 == 2 || (i % 400 == 9 && i % 7 == 2)));
    assertThat(explain(query)).contains("NodeIndexUnionSeek");
  }

  @Test
  void orNestedUnderAndKeepsTheOtherConjunct() {
    final String query = "MATCH (e:E) WHERE e.id > 1000 AND (e.x = 3 OR e.s = 's10') RETURN e.id AS id ORDER BY id";
    assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i > 1000 && (i % 500 == 3 || i % 400 == 10)));
  }

  @Test
  void aDisjunctWithoutIndexKeepsTheScan() {
    final String query = "MATCH (e:E) WHERE e.x = 3 OR e.u = 'u1' RETURN e.id AS id ORDER BY id";
    assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 7 == 1));
    assertThat(explain(query)).contains("NodeByLabelScan").doesNotContain("NodeIndexUnionSeek");
  }

  @Test
  void aNullDisjunctDoesNotMatchAnything() {
    final String query = "MATCH (e:E) WHERE e.x = null OR e.x = 5 RETURN e.id AS id ORDER BY id";
    assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 5));
  }

  @Test
  void anOrOnOtherVariableDoesNotSeekTheAnchor() {
    final String query = "MATCH (e:E), (f:E) WHERE e.x = 3 OR f.x = 4 RETURN count(*) AS n";
    final long expected = (long) expectedIds(i -> i % 500 == 3).size() * VERTICES + (long) VERTICES * expectedIds(i -> i % 500 == 4).size()
        - (long) expectedIds(i -> i % 500 == 3).size() * expectedIds(i -> i % 500 == 4).size();
    assertThat(column(query, Map.of())).containsExactly(expected);
  }

  @Test
  void explainOfTheInSeekPrintsTheValues() {
    final String plan = explain("MATCH (e:E) WHERE e.x IN [1, 2] RETURN e.id");
    assertThat(plan).contains("IN [1, 2]").doesNotContain("LiteralExpression@");
  }

  private static List<Object> expectedIds(final IntPredicate predicate) {
    final List<Object> ids = new ArrayList<>();
    for (int i = 0; i < VERTICES; i++)
      if (predicate.test(i))
        ids.add((long) i);
    return ids;
  }

  private List<Object> column(final String query, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(new HashMap<>(rs.next().toMap()).values().iterator().next());
    }
    return values;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, Map.of())) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
