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
import com.arcadedb.schema.Schema;
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

  private void createUnbalancedTypes() {
    // a hash composite index: it answers a whole key only
    database.command("sql", "CREATE VERTEX TYPE H");
    database.command("sql", "CREATE PROPERTY H.a STRING");
    database.command("sql", "CREATE PROPERTY H.b INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, false, "H", "a", "b");
    // an index declared on a supertype, queried through a subtype that has a sibling
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.a STRING");
    database.command("sql", "CREATE PROPERTY P.b INTEGER");
    database.command("sql", "CREATE INDEX ON P (a, b) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE C1 EXTENDS P");
    database.command("sql", "CREATE VERTEX TYPE C2 EXTENDS P");
    // a unique single-column index next to a composite one
    database.command("sql", "CREATE VERTEX TYPE U");
    database.command("sql", "CREATE PROPERTY U.id INTEGER");
    database.command("sql", "CREATE PROPERTY U.a STRING");
    database.command("sql", "CREATE PROPERTY U.b INTEGER");
    database.command("sql", "CREATE PROPERTY U.c INTEGER");
    database.command("sql", "CREATE INDEX ON U (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON U (a, b, c) NOTUNIQUE");
    // a prefix holding more entries than one eager read
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.a STRING");
    database.command("sql", "CREATE PROPERTY W.b INTEGER");
    database.command("sql", "CREATE INDEX ON W (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newVertex("H").set("a", "k" + (i % 4)).set("b", i).save();
        database.newVertex("C1").set("a", "k" + (i % 4)).set("b", i).save();
        database.newVertex("C2").set("a", "k" + (i % 4)).set("b", 1000 + i).save();
        database.newVertex("U").set("id", i).set("a", "k" + (i % 4)).set("b", i % 2).set("c", i).save();
      }
      for (int i = 0; i < 600; i++)
        database.newVertex("W").set("a", "same").set("b", i).save();
    });
  }

  @Test
  void aHashIndexDeclinesAPartialKey() {
    createUnbalancedTypes();
    final String query = "WITH 1 AS one MATCH (n:H {a: $x}) RETURN n.b AS b ORDER BY b";
    assertThat(values(query, Map.of("x", "k1"))).containsExactly(1, 5, 9, 13, 17);
    assertThat(profile(query, Map.of("x", "k1"))).doesNotContain("prefix:");
  }

  @Test
  void anInheritedIndexDoesNotReturnSiblingRecords() {
    createUnbalancedTypes();
    final String query = "WITH 1 AS one MATCH (n:C1 {a: $x}) RETURN n.b AS b ORDER BY b";
    assertThat(values(query, Map.of("x", "k1"))).containsExactly(1, 5, 9, 13, 17);
    assertThat(profile(query, Map.of("x", "k1"))).contains("prefix: a");
  }

  @Test
  void aWholeKeyBeatsAPrefixOfAnotherIndex() {
    createUnbalancedTypes();
    final Map<String, Object> params = Map.of("x", "k3", "y", 1, "id", 7);
    final String inline = "WITH 1 AS one MATCH (n:U {a: $x, b: $y}) WHERE n.id = $id RETURN n.c AS b";
    assertThat(values(inline, params)).containsExactly(7);
    assertThat(profile(inline, params)).contains("U[id]").doesNotContain("prefix:");
    final String mixed = "WITH 1 AS one MATCH (n:U {id: $id, a: $x}) RETURN n.c AS b";
    assertThat(values(mixed, params)).containsExactly(7);
    assertThat(profile(mixed, params)).contains("U[id]").doesNotContain("prefix:");
  }

  @Test
  void aPrefixHoldingManyEntriesIsStreamed() {
    createUnbalancedTypes();
    final String query = "WITH 1 AS one MATCH (n:W {a: $x}) RETURN n.b AS b ORDER BY b";
    final List<Integer> all = new ArrayList<>();
    for (int i = 0; i < 600; i++)
      all.add(i);
    assertThat(values(query, Map.of("x", "same"))).containsExactlyElementsOf(all);
    assertThat(profile(query, Map.of("x", "same"))).contains("prefix: a");
    assertThat(values("WITH 1 AS one MATCH (n:W {a: $x}) RETURN n.b AS b LIMIT 3", Map.of("x", "same"))).hasSize(3);
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
