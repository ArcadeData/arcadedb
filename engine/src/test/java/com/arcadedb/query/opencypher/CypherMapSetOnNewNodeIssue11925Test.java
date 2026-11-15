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
import com.arcadedb.database.Database;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #11925: {@code CREATE (n:N) SET n = r}, {@code SET n += r} and
 * {@code MERGE (n:N {k: r.k}) ON CREATE SET n += r} saved every new node empty and gave it its properties in a second
 * write at commit. By then each page held hundreds of empty records, so nearly every growth spilled the record into a
 * chunk chain after walking the page's slot table: 3-4x the time of the inline form. A map SET on a node the clause
 * creates is now written with the node, as {@code SET n.prop = value} already was (issue #8735).
 *
 * @author Shaun Patterson (shaunpatterson@gmail.com)
 */
class CypherMapSetOnNewNodeIssue11925Test extends TestHelper {
  private static long updates(final Database db) {
    return ((Number) db.getStats().get("updateRecord")).longValue();
  }

  private static List<Map<String, Object>> rows(final int count) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < count; i++)
      rows.add(Map.of("k", (long) i, "name", "n" + i, "score", i * 0.5, "tags", List.of("a", "b")));
    return rows;
  }

  private void assertWrittenOnce(final String type, final String cypher) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".k LONG");
    database.command("sql", "CREATE INDEX ON " + type + " (k) UNIQUE");
    database.transaction(() -> {
      final long before = updates(database);
      database.command("opencypher", cypher.replace("$type", type), Map.of("rows", rows(2_000))).close();
      assertThat(updates(database) - before).isZero();
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type
        + ") WHERE n.name = 'n' + toString(n.k) AND n.score = n.k * 0.5 AND n.tags = ['a', 'b'] RETURN count(n) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2_000L);
    }
  }

  @Test
  void replaceMapAfterCreateWritesEachNewNodeOnce() {
    assertWrittenOnce("V11925a", "UNWIND $rows AS r CREATE (n:$type) SET n = r");
  }

  @Test
  void mergeMapAfterCreateWritesEachNewNodeOnce() {
    assertWrittenOnce("V11925b", "UNWIND $rows AS r CREATE (n:$type) SET n += r");
  }

  @Test
  void mergeMapOnCreateWritesEachNewNodeOnce() {
    assertWrittenOnce("V11925c", "UNWIND $rows AS r MERGE (n:$type {k: r.k}) ON CREATE SET n += r");
  }

  @Test
  void foldedAndUnfoldedMapSetsStoreTheSameValuesAndCountTheSame() {
    // "+ 0 * size(keys(n))" reads the new node, so the second statement of each pair takes the SetClauseApplier path
    final String[][] pairs = {
        { "CREATE (n:$type {a: 1, b: 2}) SET n = {b: 3, c: null, d: 4}",
            "CREATE (n:$type {a: 1, b: 2}) SET n = {b: 3, c: null, d: 4 + 0 * size(keys(n))}" },
        { "CREATE (n:$type {a: 1, b: 2}) SET n += {b: 3, a: null, c: null, d: 4}",
            "CREATE (n:$type {a: 1, b: 2}) SET n += {b: 3, a: null, c: null, d: 4 + 0 * size(keys(n))}" },
        { "CREATE (n:$type {a: 1}) SET n.x = 5, n = {b: 2}, n += {c: 3}",
            "CREATE (n:$type {a: 1}) SET n.x = 5, n = {b: 2}, n += {c: 3 + 0 * size(keys(n))}" },
        { "MERGE (n:$type {a: 1}) ON CREATE SET n = {b: 2}",
            "MERGE (n:$type {a: 1}) ON CREATE SET n = {b: 2 + 0 * size(keys(n))}" } };

    int seq = 0;
    for (final String[] pair : pairs) {
      final String foldedType = "V11925f" + seq;
      final String unfoldedType = "V11925u" + seq++;
      final int folded = propertiesSet(pair[0].replace("$type", foldedType));
      final int unfolded = propertiesSet(pair[1].replace("$type", unfoldedType));
      assertThat(folded).as(pair[0]).isEqualTo(unfolded);
      assertThat(properties(foldedType)).as(pair[0]).isEqualTo(properties(unfoldedType));
    }
    assertThat(properties("V11925f0")).isEqualTo(Map.of("b", 3L, "d", 4L));
    assertThat(properties("V11925f1")).isEqualTo(Map.of("b", 3L, "d", 4L));
    assertThat(properties("V11925f2")).isEqualTo(Map.of("b", 2L, "c", 3L));
    assertThat(properties("V11925f3")).isEqualTo(Map.of("b", 2L));
  }

  @Test
  void aNullMapLeavesTheNodeAsTheCreateWroteIt() {
    final Map<String, Object> params = new HashMap<>();
    params.put("p", null);
    database.command("opencypher", "CREATE (n:V11925n {a: 1}) SET n = $p, n += $p", params).close();
    // a pattern literal is stored as the Integer it parses to, a SET literal as a Long
    assertThat(properties("V11925n")).isEqualTo(Map.of("a", 1));
  }

  @Test
  void anEntityRightHandSideCopiesItsProperties() {
    database.command("opencypher", "CREATE (:V11925src {a: 1, b: 'x'})").close();
    database.command("opencypher", "MATCH (m:V11925src) CREATE (n:V11925dst {c: 2}) SET n = m").close();
    assertThat(properties("V11925dst")).isEqualTo(Map.of("a", 1, "b", "x"));
  }

  @Test
  void aMapReadingTheNewNodeIsNotFolded() {
    database.command("opencypher", "CREATE (n:V11925r {a: 1}) SET n += {b: n.a + 1}").close();
    assertThat(properties("V11925r")).isEqualTo(Map.of("a", 1, "b", 2L));
  }

  @Test
  void aRightHandSideThatIsNotAMapStillFailsAndCreatesNothing() {
    assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:V11925t) SET n = 5").close())
        .hasStackTraceContaining("must be a map, a node or a relationship");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V11925t) RETURN count(n) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
  }

  @Test
  void aFoldedMapHoldingAnInvalidEntryValueFailsAndCreatesNothing() {
    // a map nested in a property value is not a valid property value; the folded path must refuse it like the applier
    for (final String clause : new String[] { "SET n = {a: 1, b: {c: 2}}", "SET n += {a: 1, b: {c: 2}}" }) {
      assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:V11925i) " + clause).close())
          .as(clause).isInstanceOf(RuntimeException.class);
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:V11925i) RETURN count(n) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).as(clause).isZero();
      }
    }
  }

  @Test
  void onCreateMapSetStillMatchesADuplicateKeyInsideOneBatch() {
    final List<Map<String, Object>> rows = new ArrayList<>(rows(5));
    rows.addAll(rows(5));
    database.command("opencypher", "UNWIND $rows AS r MERGE (n:V11925d {k: r.k}) ON CREATE SET n += r", Map.of("rows", rows))
        .close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V11925d) RETURN count(n) AS c, count(n.name) AS named")) {
      final Result r = rs.next();
      assertThat(r.<Long>getProperty("c")).isEqualTo(5L);
      assertThat(r.<Long>getProperty("named")).isEqualTo(5L);
    }
  }

  private Map<String, Object> properties(final String type) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") RETURN n")) {
      final Vertex v = rs.next().getVertex().orElseThrow();
      assertThat(rs.hasNext()).isFalse();
      return v.toMap(false);
    }
  }

  private int propertiesSet(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getStatistics().orElseThrow().getPropertiesSet();
    }
  }
}
