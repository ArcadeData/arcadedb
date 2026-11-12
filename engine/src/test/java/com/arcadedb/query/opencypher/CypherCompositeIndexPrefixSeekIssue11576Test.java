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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11576: on the non-optimized path a node pattern that binds only the leading properties of a
 * composite index, {@code (n:T {a: $x})} on {@code T(a, b)}, never used the index and scanned the whole type: the step
 * required every property of the index. A MATCH after a WITH, an OPTIONAL MATCH and {@code UNWIND $rows AS row MATCH ...}
 * all run there. The step now reads the range of the bound prefix, and its row filter checks the rest as before.
 */
class CypherCompositeIndexPrefixSeekIssue11576Test extends TestHelper {

  private static final String PREFIX_SEEK = "[index: T[a, b], prefix: a]";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.a STRING");
    database.command("sql", "CREATE PROPERTY T.b INTEGER");
    database.command("sql", "CREATE INDEX ON T (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newVertex("T").set("a", "k" + (i % 50)).set("b", i).save();
    });
  }

  @Test
  void prefixLookupsSeekTheCompositeIndex() {
    final Object[][] cases = {
        { "WITH 1 AS one MATCH (n:T {a: $x}) RETURN n.b AS b ORDER BY b", Map.of("x", "k7") },
        { "OPTIONAL MATCH (n:T {a: $x}) RETURN n.b AS b ORDER BY b", Map.of("x", "k7") },
        { "UNWIND $xs AS x MATCH (n:T {a: x}) RETURN n.b AS b ORDER BY b", Map.of("xs", List.of("k7")) },
        { "UNWIND $xs AS x MATCH (n:T) WHERE n.a = x RETURN n.b AS b ORDER BY b", Map.of("xs", List.of("k7")) } };
    for (final Object[] testCase : cases) {
      @SuppressWarnings("unchecked")
      final Map<String, Object> params = (Map<String, Object>) testCase[1];
      final String query = (String) testCase[0];
      assertThat(values(query, params)).as(query).containsExactly(7, 57, 107, 157);
      assertThat(profile(query, params)).as(query).contains(PREFIX_SEEK);
    }
  }

  @Test
  void theUnboundPropertiesAreStillFiltered() {
    final String query = "WITH 1 AS one MATCH (n:T {a: $x}) WHERE n.b > 100 RETURN n.b AS b ORDER BY b";
    assertThat(values(query, Map.of("x", "k7"))).containsExactly(107, 157);
    assertThat(values("WITH 1 AS one MATCH (n:T {a: $x}) RETURN n.b AS b", Map.of("x", "missing"))).isEmpty();
  }

  @Test
  void aFullKeyStillUsesTheExactLookup() {
    final String query = "WITH 1 AS one MATCH (n:T {a: $x, b: $y}) RETURN n.b AS b";
    assertThat(values(query, Map.of("x", "k7", "y", 57))).containsExactly(57);
    assertThat(profile(query, Map.of("x", "k7", "y", 57))).contains("[index: T[a, b]]");
  }

  private List<Integer> values(final String query, final Map<String, Object> params) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Number b = rs.next().getProperty("b");
        if (b != null)
          out.add(b.intValue());
      }
    }
    return out;
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
