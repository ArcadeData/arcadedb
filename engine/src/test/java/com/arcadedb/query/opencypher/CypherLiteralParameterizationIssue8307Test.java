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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser.ParsedQuery;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8307: OpenCypher queries that differ only in their literal values share one parsed statement and one plan, and every
 * position where a parameter would not mean what the literal meant keeps its literal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLiteralParameterizationIssue8307Test extends TestHelper {

  private static final List<String> CORPUS = List.of(//
      "MATCH (p:Person) WHERE p.id = 5 RETURN p.name AS name",//
      "MATCH (p:Person {id: 6}) RETURN p.name AS name, p.id AS id",//
      "MATCH (p:Person) WHERE p.id = 7 RETURN p",//
      "MATCH (p:Person {id: 5}) RETURN 1, p.id + 10, 'a' + p.name",//
      "MATCH (p:Person) WHERE p.id < 4 RETURN p.id + 10 AS k ORDER BY p.id + 10 DESC",//
      "MATCH (p:Person) WHERE p.id < 4 RETURN p.id + 10 AS k ORDER BY k DESC",//
      "MATCH (p:Person) RETURN p.id % 3 + 1 AS k, count(*) AS c ORDER BY k",//
      "MATCH (p:Person) WITH p.id % 4 AS k, count(*) AS c WHERE c > 20 RETURN k, c ORDER BY k",//
      "MATCH (p:Person) WHERE p.id > -5 AND p.id < 2 RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.name STARTS WITH 'p1' RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.name STARTS WITH '' AND p.id < 3 RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.name ENDS WITH '7' RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.name CONTAINS '3' AND p.id < 40 RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.name =~ 'p1.' RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.id IN [1, 2, 3] RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.id IN [1, 1, 2] RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.score = 15.0 RETURN p.name AS n",//
      "MATCH (p:Person) WHERE p.score > 1.4e2 RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) RETURN p.name AS n ORDER BY p.id SKIP 3 LIMIT 2",//
      "MATCH (p:Person) RETURN p.name AS n ORDER BY p.id SKIP 5 LIMIT 3",//
      "RETURN 5 AS a, 2.5 AS b, 'x' AS c, -5 AS d, -9223372036854775808 AS e, 0x1F AS f, -2.5 AS g, 3000000000 AS h, 1e3 AS i",//
      "RETURN 'it\\'s' AS s, \"dq\" AS t, 'a\\nb' AS u, '' AS v",//
      "RETURN toUpper('abc') AS u, size('hello') AS s, substring('hello', 1, 3) AS sub",//
      "RETURN [1, 2, 3] AS l, {a: 1, b: 'x'} AS m, [x IN [1, 2, 3] WHERE x > 1 | x * 10] AS c",//
      "RETURN range(1, 5) AS r, range(0, 10, 3) AS s",//
      "UNWIND [1, 2, 3] AS x RETURN x * 2 AS y ORDER BY y",//
      "UNWIND range(1, 4) AS x WITH x WHERE x > 2 RETURN collect(x) AS xs",//
      "RETURN CASE WHEN 1 < 2 THEN 'yes' ELSE 'no' END AS c",//
      "MATCH (p:Person) WHERE p.id < 3 RETURN CASE p.id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS c ORDER BY c",//
      "RETURN toInteger('42') AS i, toFloat('2.5') AS f, toString(7) AS s, toBoolean('true') AS b",//
      "RETURN date('2024-01-15') AS d, date('2024-01-15') + duration('P1D') AS e, duration('PT2H') AS du",//
      "RETURN round(2.567, 2) AS r, abs(-3) AS a, sqrt(16) AS s, 7 / 2 AS i, 7.0 / 2 AS f, 7 % 3 AS m, 2 ^ 3 AS p",//
      "MATCH (p:Person) WHERE p.id = 3 OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN p.name AS a, q.name AS b",//
      "MATCH (p:Person {id: 3})-[:KNOWS]->(q) RETURN q.name AS n",//
      "MATCH (p:Person {id: 3})-[:KNOWS*1..3]->(q) RETURN q.name AS n ORDER BY n",//
      "MATCH (p:Person {id: 3})-[:KNOWS*2]->(q) RETURN q.name AS n",//
      "MATCH path = shortestPath((a:Person {id: 1})-[:KNOWS*]->(b:Person {id: 5})) RETURN length(path) AS l",//
      "MATCH (p:Person) WHERE p.id < 5 AND EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.id = 3 } RETURN p.name AS n",//
      "MATCH (p:Person) WHERE p.id < 5 RETURN p.name AS n, COUNT { MATCH (p)-[:KNOWS]->(q) WHERE q.id > 2 } AS c ORDER BY n",//
      "MATCH (p:Person) WHERE p.id < 3 RETURN p.name AS n, [(p)-[:KNOWS]->(q) WHERE q.id > 0 | q.id * 100] AS c ORDER BY n",//
      "MATCH (p:Person) WHERE p.id < 3 CALL { WITH p MATCH (p)-[:KNOWS]->(q) RETURN q.id + 1000 AS z } RETURN p.id AS id, z ORDER BY id",//
      "MATCH (p:Person) WHERE p.id = 1 RETURN p.name AS n UNION MATCH (p:Person) WHERE p.id = 2 RETURN p.name AS n",//
      "MATCH (p:Person) WHERE p.id = 1 OR p.id = 1 RETURN p.name AS n",//
      "MATCH (p:Person) WHERE p.id = 3 AND p.name <> 'p4' AND p.name = \"p3\" RETURN p.id AS id",//
      "MATCH (a:Person {id: 1}), (b:Person {id: 2}) RETURN a.name AS a, b.name AS b",//
      "MATCH (a:Person {id: 1})-[:KNOWS]->(b), (c:Person {id: 2})-[:KNOWS]->(d) RETURN b.name AS b, d.name AS d",//
      "MATCH (p:Person) WHERE p.id < 10 WITH p ORDER BY p.id DESC LIMIT 3 RETURN collect(p.name) AS names",//
      "MATCH (p:Person) RETURN count(p) AS c, sum(p.id) AS s, avg(p.score) AS a, min(p.id) AS mi, max(p.name) AS ma",//
      "MATCH (p:Person) WHERE p.id >= 50 RETURN count(*) AS c",//
      "MATCH (p:Person) WHERE p.id < 3 RETURN p {.name, extra: p.id * 2, fixed: 'k'} AS m ORDER BY m.name",//
      "MATCH (p:Person) WHERE p.id < 3 RETURN coalesce(p.missing, 'none') AS c, p.id + 0.5 AS f ORDER BY f",//
      "MATCH (p:Person) WHERE p.id IN range(10, 12) RETURN p.name AS n ORDER BY n",//
      "WITH 3 AS x MATCH (p:Person) WHERE p.id = x RETURN p.name AS n",//
      "WITH [1, 2] AS xs UNWIND xs AS x MATCH (p:Person {id: x}) RETURN p.name AS n ORDER BY n",//
      "MATCH (p:Person) WHERE p.id = 2 SET p.tmp = 5 RETURN p.tmp AS t",//
      "MATCH (p:Person) WHERE p.id = 2 RETURN p.tmp AS t",//
      "MATCH (n:Nothing) RETURN size('ab') AS s",//
      "MATCH (p:Person) WHERE p.id IN [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 1, 2, 3] RETURN p.name AS n ORDER BY n",//
      "RETURN 1 + 2",//
      "RETURN 1 + 2 AS s, 3 * 4 AS p",//
      "MATCH (p:Person) WHERE p.id < 3 RETURN p.id + 1 + 2 AS k ORDER BY p.id + 1 + 2 DESC",//
      "MATCH (p:Person) WHERE p.id < 6 RETURN p.id % 2 + 1 AS k, count(*) AS c ORDER BY p.id % 2 + 1"//
  );

  /**
   * Texts of one shape, each sequence opened by the edge case: the first text of a shape is the one the policy is decided on,
   * so it must not matter which value it happens to carry.
   */
  private static final List<List<String>> SHAPE_SEQUENCES = List.of(//
      List.of("MATCH (p:Person) WHERE p.id > -1 AND p.id < 3 RETURN p.name AS n ORDER BY n",
          "MATCH (p:Person) WHERE p.id > -50 AND p.id < 4 RETURN p.name AS n ORDER BY n"),//
      List.of("MATCH (p:Person {id: 0}) RETURN p.name AS n", "MATCH (p:Person {id: 42}) RETURN p.name AS n"),//
      List.of("MATCH (p:Person) WHERE p.name = 'it\\'s' RETURN p.id AS id", "MATCH (p:Person) WHERE p.name = 'p5' RETURN p.id AS id"),//
      List.of("MATCH (p:Person) WHERE p.name = '' RETURN p.id AS id", "MATCH (p:Person) WHERE p.name = 'p6' RETURN p.id AS id"),//
      List.of("RETURN -9223372036854775808 AS a, 'a\\nb' AS b", "RETURN 7 AS a, 'plain' AS b"),//
      List.of("MATCH (p:Person) WHERE p.score = 0.0 RETURN p.name AS n", "MATCH (p:Person) WHERE p.score = 4.5 RETURN p.name AS n"),//
      List.of("MATCH (p:Person {id: 1}) RETURN p.name AS n, 1 + 2 AS s", "MATCH (p:Person {id: 2}) RETURN p.name AS n, 5 + 6 AS s")//
  );

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < 100; i++) {
        final MutableVertex vertex = database.newVertex("Person").set("id", i).set("name", "p" + i).set("score", i * 1.5D).save();
        if (previous != null)
          previous.newEdge("KNOWS", vertex).save();
        previous = vertex;
      }
    });
  }

  private List<Map<String, Object>> rows(final String cypher) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        // names in projection order, values with their Java type; records by identity, so two runs compare
        final Map<String, Object> map = new LinkedHashMap<>();
        for (final String name : row.getPropertyNames()) {
          final Object value = row.getProperty(name);
          map.put(name, value instanceof Identifiable identifiable ? identifiable.getIdentity() : value);
        }
        rows.add(map);
      }
    }
    return rows;
  }

  private void setParameterization(final boolean enabled) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, enabled);
    db().getCypherStatementCache().clear();
    db().getCypherPlanCache().clear();
  }

  @Test
  void everyQueryAnswersWhatItAnswersAsWritten() {
    final List<List<Map<String, Object>>> asWritten = new ArrayList<>();
    setParameterization(false);
    try {
      database.transaction(() -> {
        for (final String cypher : CORPUS)
          asWritten.add(rows(cypher));
      });
    } finally {
      setParameterization(true);
    }

    // twice: the first pass decides each shape and builds its statement, the second is served from the cache
    for (int pass = 0; pass < 2; pass++) {
      final int p = pass;
      database.transaction(() -> {
        for (int i = 0; i < CORPUS.size(); i++)
          assertThat(rows(CORPUS.get(i))).as("pass %d: %s", p, CORPUS.get(i)).isEqualTo(asWritten.get(i));
      });
    }
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }

  private List<Result> query(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  /**
   * The lookup of a text whose shape has already been decided. The first text of a shape runs as written (see
   * {@link #aShapeSeenOnceRunsAsWritten}), so a lookup made to inspect the extraction looks the text up twice.
   */
  private Lookup<ParsedQuery> lookup(final String cypher) {
    db().getCypherStatementCache().getParameterized(cypher);
    return db().getCypherStatementCache().getParameterized(cypher);
  }

  @Test
  void theFirstTextOfAShapeDecidesForEveryValueWhicheverValueItCarries() {
    for (final List<String> sequence : SHAPE_SEQUENCES) {
      final List<List<Map<String, Object>>> asWritten = new ArrayList<>();
      setParameterization(false);
      try {
        for (final String cypher : sequence)
          asWritten.add(rows(cypher));
      } finally {
        setParameterization(true);
      }
      for (int i = 0; i < sequence.size(); i++)
        assertThat(rows(sequence.get(i))).as(sequence.get(i)).isEqualTo(asWritten.get(i));
    }
  }

  @Test
  void concurrentExecutionsOfOneShapeEachBindTheirOwnValues() throws Exception {
    final List<String> failures = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < 8; t++) {
      final int offset = t;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < 200; i++) {
            final int id = (offset * 13 + i) % 100;
            final List<Result> found = query(
                "MATCH (p:Person {id: " + id + "}) WHERE p.name = 'p" + id + "' RETURN p.name AS n, p.id + 1000 AS k");
            if (found.size() != 1 || !("p" + id).equals(found.getFirst().getProperty("n"))
                || ((Number) found.getFirst().getProperty("k")).longValue() != id + 1000)
              failures.add("id " + id + " answered " + found);
          }
        } catch (final Exception e) {
          failures.add(e.toString());
        }
      });
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();
    assertThat(failures).isEmpty();
  }

  @Test
  void queriesDifferingOnlyInLiteralsShareOneStatementAndOnePlan() {
    db().getCypherStatementCache().clear();
    db().getCypherPlanCache().clear();

    for (int id = 0; id < 50; id++) {
      final List<Result> rows = query("MATCH (p:Person) WHERE p.id = " + id + " RETURN p.name AS name");
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("p" + id);
    }
    for (int id = 0; id < 50; id++) {
      final List<Result> rows = query("MATCH (p:Person {id: " + id + "}) RETURN p.name AS name");
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("p" + id);
    }

    // one template per query shape, not one entry per value; the first text of each shape ran as written
    assertThat(db().getCypherStatementCache().size()).isEqualTo(2);
    assertThat(db().getCypherPlanCache().size()).isLessThanOrEqualTo(4);

    final Lookup<ParsedQuery> first = lookup("MATCH (p:Person) WHERE p.id = 3 RETURN p.name AS name");
    final Lookup<ParsedQuery> second = lookup("MATCH (p:Person) WHERE p.id = 77 RETURN p.name AS name");
    assertThat(second.statement()).isSameAs(first.statement());
    assertThat(second.cacheKey()).isEqualTo(first.cacheKey());
    assertThat(first.cacheKey()).doesNotContain("3");
    assertThat(first.parameters()).containsValue(3L);
    assertThat(second.parameters()).containsValue(77L);
  }

  @Test
  void aShapeSeenOnceRunsAsWritten() {
    // a workload that never repeats a shape pays one parse per text, never a second one for a statement nobody reuses
    final Lookup<ParsedQuery> first = db().getCypherStatementCache().getParameterized("MATCH (p:Person {id: 11}) RETURN p.name AS once");
    assertThat(first.parameters()).isNull();
    assertThat(first.cacheKey()).isEqualTo("MATCH (p:Person {id: 11}) RETURN p.name AS once");
    assertThat(db().getCypherStatementCache().contains(first.cacheKey())).isFalse();

    final Lookup<ParsedQuery> second = db().getCypherStatementCache().getParameterized("MATCH (p:Person {id: 12}) RETURN p.name AS once");
    assertThat(second.parameters()).containsValue(12);
    assertThat(db().getCypherStatementCache().contains(second.cacheKey())).isTrue();
  }

  @Test
  void stringsDoublesNegativesAndEscapesBindTheValueTheParserWouldHaveBuilt() {
    assertThat(query("MATCH (p:Person) WHERE p.name = 'p42' RETURN p.id AS id").getFirst().<Integer>getProperty("id")).isEqualTo(42);
    assertThat(query("MATCH (p:Person) WHERE p.name = \"p43\" RETURN p.id AS id").getFirst().<Integer>getProperty("id")).isEqualTo(43);
    assertThat(query("MATCH (p:Person) WHERE p.score = 15.0 RETURN p.id AS id").getFirst().<Integer>getProperty("id")).isEqualTo(10);
    assertThat(query("MATCH (p:Person) WHERE p.id > -5 AND p.id < 2 RETURN count(*) AS c").getFirst().<Long>getProperty("c"))
        .isEqualTo(2L);
    assertThat(query("MATCH (p:Person) WHERE p.id = 0x1F RETURN p.name AS n").getFirst().<String>getProperty("n")).isEqualTo("p31");

    assertThat(query("RETURN 'it\\'s' AS s, -9223372036854775808 AS m, 1.5e3 AS d, -2.5 AS nd").getFirst().toMap())
        .containsEntry("s", "it's").containsEntry("m", Long.MIN_VALUE).containsEntry("d", 1500.0D).containsEntry("nd", -2.5D);

    // the extracted values keep the types the parser gives the literals
    final Lookup<ParsedQuery> lookup = lookup("RETURN 7 AS a, 2.5 AS b, 'x' AS c");
    assertThat(lookup.parameters().values()).containsExactlyInAnyOrder(7L, 2.5D, "x");
  }

  @Test
  void anUnaliasedProjectionKeepsItsLiteralSoTheColumnKeepsItsName() {
    final Result row = query("MATCH (p:Person {id: 5}) RETURN 1, p.id + 10, 'a' + p.name").getFirst();
    assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("1", "p.id + 10", "'a' + p.name");
    assertThat(row.<Long>getProperty("p.id + 10")).isEqualTo(15L);

    final Result other = query("MATCH (p:Person {id: 6}) RETURN 2, p.id + 20").getFirst();
    assertThat(other.getPropertyNames()).containsExactlyInAnyOrder("2", "p.id + 20");
    assertThat(other.<Long>getProperty("p.id + 20")).isEqualTo(26L);

    // WITH follows the same rule
    final Result with = query("MATCH (p:Person {id: 7}) WITH p.id * 3 RETURN *").getFirst();
    assertThat(with.getPropertyNames()).containsExactly("p.id * 3");
  }

  @Test
  void orderBySkipAndLimitKeepTheirLiterals() {
    final List<Result> limited = query("MATCH (p:Person) WHERE p.id < 20 RETURN p.id AS id ORDER BY p.id + 1 DESC SKIP 2 LIMIT 3");
    assertThat(limited.stream().map(r -> r.<Integer>getProperty("id")).toList()).containsExactly(17, 16, 15);

    final List<Result> limitedMore = query("MATCH (p:Person) WHERE p.id < 30 RETURN p.id AS id ORDER BY p.id + 1 DESC SKIP 1 LIMIT 5");
    assertThat(limitedMore.stream().map(r -> r.<Integer>getProperty("id")).toList()).containsExactly(28, 27, 26, 25, 24);

    assertThat(lookup("MATCH (p:Person) RETURN p.id AS id SKIP 2 LIMIT 3").cacheKey()).contains("SKIP 2 LIMIT 3");

    // an ORDER BY item matched to the projection it repeats, after an aggregation
    final List<Result> grouped = query(
        "MATCH (p:Person) WHERE p.id < 6 RETURN p.id % 2 + 1 AS k, count(*) AS c ORDER BY p.id % 2 + 1");
    assertThat(grouped.stream().map(r -> r.<Long>getProperty("k")).toList()).containsExactly(1L, 2L);
  }

  @Test
  void aLiteralWrittenTwiceIsOneParameter() {
    final Lookup<ParsedQuery> lookup = lookup("MATCH (p:Person) WHERE p.id = 4 OR p.id + 4 = 8 RETURN p.id AS id, p.id + 4 AS k");
    assertThat(lookup.parameters()).hasSize(2);
    assertThat(lookup.parameters().values()).containsExactlyInAnyOrder(4L, 8L);
  }

  @Test
  void inlinePatternPropertiesAreStoredWithTheTypeTheLiteralWouldHaveStored() {
    database.command("sql", "CREATE VERTEX TYPE Typed");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Typed {n: 5, big: 9999999999, list: [1, 2], name: 'a'})");
      database.command("opencypher", "CREATE (:Typed {n: 6, big: 8888888888, list: [3, 4], name: 'b'})");
    });

    final Result first = query("MATCH (t:Typed {n: 5}) RETURN t.n AS n, t.big AS big, t.list AS list, t.name AS name").getFirst();
    assertThat(first.<Object>getProperty("n")).isInstanceOf(Integer.class).isEqualTo(5);
    assertThat(first.<Object>getProperty("big")).isInstanceOf(Long.class).isEqualTo(9999999999L);
    assertThat(first.<List<Object>>getProperty("list")).containsExactly(1L, 2L);
    assertThat(first.<String>getProperty("name")).isEqualTo("a");

    final Result second = query("MATCH (t:Typed {n: 6}) RETURN t.n AS n, t.list AS list").getFirst();
    assertThat(second.<Object>getProperty("n")).isInstanceOf(Integer.class).isEqualTo(6);
    assertThat(second.<List<Object>>getProperty("list")).containsExactly(3L, 4L);

    // the map of an INSERT pattern is read the same way
    database.transaction(() -> {
      database.command("opencypher", "INSERT (:Typed {n: 7, big: 7777777777})");
      database.command("opencypher", "INSERT (:Typed {n: 8, big: 6666666666})");
    });
    final Result inserted = query("MATCH (t:Typed {n: 8}) RETURN t.n AS n, t.big AS big").getFirst();
    assertThat(inserted.<Object>getProperty("n")).isInstanceOf(Integer.class).isEqualTo(8);
    assertThat(inserted.<Object>getProperty("big")).isInstanceOf(Long.class).isEqualTo(6666666666L);
  }

  @Test
  void callerParametersAndExtractedLiteralsBindTogether() {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id >= $from AND p.id < 12 RETURN p.id AS id",
        Map.of("from", 10))) {
      rs.forEachRemaining(rows::add);
    }
    assertThat(rows).hasSize(2);
  }

  @Test
  void schemaStatementsAreNeverParameterized() {
    database.command("opencypher", "CREATE CONSTRAINT person_name IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE");
    final Lookup<ParsedQuery> lookup = lookup("CREATE CONSTRAINT person_name IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE");
    assertThat(lookup.parameters()).isNull();
  }

  @Test
  void aTextThatAlreadyUsesTheGeneratedNamespaceIsLeftAlone() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id = $__lit_i0 RETURN p.name AS n",
        Map.of("__lit_i0", 9))) {
      assertThat(rs.next().<String>getProperty("n")).isEqualTo("p9");
    }
    assertThat(lookup("MATCH (p:Person) WHERE p.id = 3 AND p.name <> '__lit_x' RETURN p").parameters()).isNull();
  }

  @Test
  void theSettingTurnsItOff() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, false);
    try {
      final Lookup<ParsedQuery> lookup = lookup("MATCH (p:Person) WHERE p.id = 3 RETURN p.name AS name");
      assertThat(lookup.parameters()).isNull();
      assertThat(lookup.cacheKey()).isEqualTo("MATCH (p:Person) WHERE p.id = 3 RETURN p.name AS name");
      assertThat(query("MATCH (p:Person) WHERE p.id = 3 RETURN p.name AS name").getFirst().<String>getProperty("name"))
          .isEqualTo("p3");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, true);
    }
  }

  @Test
  void explainAndProfilePlanTheTextAsWritten() {
    // diagnostics show the values the caller wrote, as SQL's EXPLAIN does; their plans are never cached
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (p:Person) WHERE p.id = 3 RETURN p.name AS name")) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("3").doesNotContain("__lit_");
    }
    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:Person {id: 4}) RETURN p.name AS name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p4");
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("id=4").doesNotContain("__lit_");
    }
    // a parameter the caller wrote is shown by its name, not by the identity of the expression object
    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:Person {id: $id}) RETURN p.name AS name",
        Map.of("id", 5))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p5");
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("id=$id").doesNotContain("ParameterExpression@");
    }
  }

  @Test
  void anInvalidLiteralStillRaisesTheParserError() {
    query("RETURN 5 AS n");
    assertThatThrownBy(() -> query("RETURN 99999999999999999999 AS n")).hasMessageContaining("too large");
    assertThatThrownBy(() -> query("MATCH (n:Nothing) RETURN size(42) AS s")).hasMessageContaining("size");
  }

  @Test
  void roundKeepsItsRoundingMode() {
    assertThat(query("RETURN round(2.5, 0, 'HALF_UP') AS r").getFirst().<Double>getProperty("r")).isEqualTo(3.0D);
    assertThatThrownBy(() -> query("RETURN round(2.5, 0, 'NOT_A_MODE') AS r")).isNotNull();
  }

  @Test
  void writesWithLiteralsShareTheirStatement() {
    database.command("sql", "CREATE VERTEX TYPE Event");
    db().getCypherStatementCache().clear();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("opencypher", "CREATE (e:Event {seq: " + i + ", label: 'e" + i + "'}) SET e.weight = " + (i * 0.5D));
    });
    assertThat(db().getCypherStatementCache().size()).isEqualTo(1);
    final Result row = query("MATCH (e:Event {seq: 7}) RETURN e.label AS l, e.weight AS w, e.seq AS s").getFirst();
    assertThat(row.<String>getProperty("l")).isEqualTo("e7");
    assertThat(row.<Double>getProperty("w")).isEqualTo(3.5D);
    assertThat(row.<Object>getProperty("s")).isInstanceOf(Integer.class);
  }

  @Test
  void aSharedRegexStatementMatchesEachExecutionsOwnPattern() throws Exception {
    final List<String> failures = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < 4; t++) {
      final int target = t + 1;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < 300; i++)
            try (final ResultSet rs = database.query("opencypher",
                "MATCH (p:Person) WHERE p.id < 10 AND p.name =~ 'p" + target + "' RETURN p.id AS id")) {
              final List<Integer> ids = new ArrayList<>();
              rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
              if (!ids.equals(List.of(target)))
                failures.add("pattern p" + target + " matched " + ids);
            }
        } catch (final Exception e) {
          failures.add(e.toString());
        }
      });
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();
    assertThat(failures).isEmpty();
  }
}
