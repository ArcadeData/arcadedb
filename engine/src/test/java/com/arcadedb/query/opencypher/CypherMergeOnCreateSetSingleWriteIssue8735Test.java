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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #8735: {@code MERGE (v:V {id: r.id}) ON CREATE SET v.name = r.name} saved the new
 * vertex with {@code id} only and then applied the {@code ON CREATE SET} as a second write, so every new node was
 * written twice and the second write grew the record inside its page. A new node is now written once, with the
 * {@code ON CREATE SET} properties already on it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeOnCreateSetSingleWriteIssue8735Test extends TestHelper {
  private static long updates(final com.arcadedb.database.Database db) {
    return ((Number) db.getStats().get("updateRecord")).longValue();
  }

  private static List<Map<String, Object>> rows(final int from, final int to) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = from; i < to; i++)
      rows.add(Map.of("id", (long) i, "name", "n" + i));
    return rows;
  }

  @Test
  void onCreateSetWritesEachNewNodeOnce() {
    database.transaction(() -> {
      final long before = updates(database);
      database.command("opencypher", "UNWIND $rows AS r MERGE (v:V8735 {id: r.id}) ON CREATE SET v.name = r.name",
          Map.of("rows", rows(0, 50))).close();
      assertThat(updates(database) - before).isZero();
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735) WHERE v.name = 'n' + toString(v.id) RETURN count(v) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(50L);
    }
  }

  @Test
  void onCreateSetValuesAreStoredAndOnMatchIsUntouched() {
    database.command("opencypher", "UNWIND $rows AS r MERGE (v:V8735b {id: r.id}) ON CREATE SET v.name = r.name, v.created = true",
        Map.of("rows", rows(0, 3))).close();
    database.command("opencypher",
        "UNWIND $rows AS r MERGE (v:V8735b {id: r.id}) ON CREATE SET v.name = 'again', v.created = true ON MATCH SET v.seen = 1",
        Map.of("rows", rows(2, 5))).close();

    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735b) RETURN v ORDER BY v.id")) {
      final List<Vertex> vs = new ArrayList<>();
      while (rs.hasNext())
        vs.add(rs.next().getVertex().get());
      assertThat(vs).hasSize(5);
      assertThat(vs.get(0).get("name")).isEqualTo("n0");
      assertThat(vs.get(0).get("seen")).isNull();
      assertThat(vs.get(2).get("name")).isEqualTo("n2");
      assertThat(vs.get(2).get("seen")).isEqualTo(1L);
      assertThat(vs.get(3).get("name")).isEqualTo("again");
      assertThat(vs.get(3).get("created")).isEqualTo(true);
      assertThat(vs.get(3).get("seen")).isNull();
    }
  }

  @Test
  void onCreateSetOverridesPatternPropertyAndCountsStatistics() {
    try (final ResultSet rs = database.command("opencypher",
        "MERGE (v:V8735c {id: 1, name: 'pattern'}) ON CREATE SET v.name = 'set', v.other = 2 RETURN v")) {
      final Vertex v = rs.next().getVertex().get();
      assertThat(v.get("name")).isEqualTo("set");
      assertThat(v.get("other")).isEqualTo(2L);
    }
  }

  @Test
  void onCreateSetReadingTheNewNodeStillSeesItsPreClauseValues() {
    try (final ResultSet rs = database.command("opencypher",
        "MERGE (v:V8735d {id: 1}) ON CREATE SET v.a = v.id + 10, v.b = v.a RETURN v")) {
      final Vertex v = rs.next().getVertex().get();
      assertThat(v.get("a")).isEqualTo(11L);
      assertThat(v.has("b")).isFalse();
    }
  }

  @Test
  void onCreateSetToNullStoresNothing() {
    try (final ResultSet rs = database.command("opencypher",
        "MERGE (v:V8735e {id: 1}) ON CREATE SET v.name = null RETURN v")) {
      assertThat(rs.next().getVertex().get().has("name")).isFalse();
    }
  }

  @Test
  void onCreateSetLabelStillWorks() {
    try (final ResultSet rs = database.command("opencypher",
        "MERGE (v:V8735f {id: 1}) ON CREATE SET v:Extra8735, v.name = 'x' RETURN labels(v) AS l, v.name AS n")) {
      final Result r = rs.next();
      assertThat(r.<List<String>>getProperty("l")).contains("V8735f", "Extra8735");
      assertThat((String) r.getProperty("n")).isEqualTo("x");
    }
  }

  @Test
  void duplicateKeyInsideOneBatchStillMatches() {
    final List<Map<String, Object>> rows = new ArrayList<>(rows(0, 5));
    rows.addAll(rows(0, 5));
    database.command("opencypher", "UNWIND $rows AS r MERGE (v:V8735g {id: r.id}) ON CREATE SET v.name = r.name",
        Map.of("rows", rows)).close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735g) RETURN count(v) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(5L);
    }
  }

  @Test
  void statisticsCountEveryAssignedProperty() {
    assertThat(propertiesSet("MERGE (v:V8735h {id: 1}) ON CREATE SET v.name = 'x', v.other = 2")).isEqualTo(3);
    // an assignment over a pattern property still counts, as it did when the SET was a separate write
    assertThat(propertiesSet("MERGE (v:V8735i {id: 1, name: 'p'}) ON CREATE SET v.name = 's'")).isEqualTo(3);
  }

  @Test
  void setRightAfterMergeWritesEachNewNodeOnceAndStillUpdatesMatchedOnes() {
    database.command("opencypher", "UNWIND $rows AS r MERGE (v:V8735j {id: r.id}) SET v.name = r.name", Map.of("rows", rows(0, 20))).close();
    database.transaction(() -> {
      final long before = updates(database);
      database.command("opencypher", "UNWIND $rows AS r MERGE (v:V8735j {id: r.id}) SET v.name = r.name",
          Map.of("rows", rows(100, 150))).close();
      assertThat(updates(database) - before).isZero();
    });

    // the same keys again: every node matches, and the SET still rewrites them
    database.command("opencypher", "UNWIND $rows AS r MERGE (v:V8735j {id: r.id}) SET v.name = 'again'", Map.of("rows", rows(0, 20))).close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735j) WHERE v.name = 'again' RETURN count(v) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(20L);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735j) RETURN count(v) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(70L);
    }
  }

  @Test
  void setAfterMergeRunsAfterOnCreateAndOnMatchSet() {
    database.command("opencypher", "MERGE (v:V8735k {id: 1}) ON CREATE SET v.name = 'created', v.a = 1 SET v.name = 'final'").close();
    database.command("opencypher", "MERGE (v:V8735k {id: 1}) ON MATCH SET v.a = 2 SET v.b = 3").close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735k) RETURN v")) {
      final Vertex v = rs.next().getVertex().get();
      assertThat(v.get("name")).isEqualTo("final");
      assertThat(v.get("a")).isEqualTo(2L);
      assertThat(v.get("b")).isEqualTo(3L);
    }
  }

  @Test
  void setAfterMergeThatReadsTheNodeIsNotFolded() {
    database.command("opencypher", "UNWIND [1, 2, 3] AS i MERGE (v:V8735l {id: 1}) SET v.n = coalesce(v.n, 0) + i").close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735l) RETURN v.n AS n")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(6L);
    }
  }

  @Test
  void setAfterCreateWritesEachNewNodeOnce() {
    database.transaction(() -> {
      final long before = updates(database);
      database.command("opencypher", "UNWIND $rows AS r CREATE (v:V8735m {id: r.id}) SET v.name = r.name", Map.of("rows", rows(0, 50))).close();
      assertThat(updates(database) - before).isZero();
    });
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735m) WHERE v.name = 'n' + toString(v.id) RETURN count(v) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(50L);
    }
  }

  @Test
  void setAfterCreateKeepsEveryOtherShape() {
    // reads another node the same CREATE makes, targets a matched node, and mixes a property with a label
    database.command("opencypher", "CREATE (a:V8735n {id: 1}), (b:V8735n {id: 2}) SET a.peer = b.id, b.peer = a.id").close();
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V8735n) RETURN v.id AS id, v.peer AS peer ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("peer").longValue()).isEqualTo(2L);
      assertThat(rs.next().<Number>getProperty("peer").longValue()).isEqualTo(1L);
    }

    database.command("opencypher", "CREATE (a:V8735o {id: 1})").close();
    database.command("opencypher", "MATCH (m:V8735o) CREATE (a:V8735p {id: 2}) SET m.touched = true, a.name = 'x'").close();
    database.command("opencypher", "CREATE (c:V8735q {id: 3}) SET c:Extra8735q, c.name = 'y'").close();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (m:V8735o), (a:V8735p), (c:Extra8735q) RETURN m.touched AS t, a.name AS an, c.name AS cn")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("t")).isTrue();
      assertThat(r.<String>getProperty("an")).isEqualTo("x");
      assertThat(r.<String>getProperty("cn")).isEqualTo("y");
    }
  }

  @Test
  void setAfterCreateCountsStatistics() {
    assertThat(propertiesSet("CREATE (v:V8735r {id: 1}) SET v.name = 'x', v.other = 2")).isEqualTo(3);
  }

  private int propertiesSet(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getStatistics().orElseThrow().getPropertiesSet();
    }
  }
}
