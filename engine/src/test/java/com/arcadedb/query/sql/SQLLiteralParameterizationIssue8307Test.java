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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.StatementCache;
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
 * Issue #8307: SQL statements that differ only in their literal values share one parsed statement and one plan, and answer
 * exactly what the statement as written answers: the same rows, the same column names and the same Java types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLLiteralParameterizationIssue8307Test extends TestHelper {

  private static final List<String> CORPUS = List.of(//
      "SELECT name FROM Person WHERE id = 5",//
      "SELECT name FROM Person WHERE id = 6",//
      "SELECT FROM Person WHERE id = 7",//
      "SELECT 1, id + 10, 'a' + name FROM Person WHERE id = 3",//
      "SELECT 2, id + 20, 'b' + name FROM Person WHERE id = 4",//
      "SELECT id + 10 AS k FROM Person WHERE id < 4 ORDER BY id + 10 DESC",//
      "SELECT id % 3 + 1 AS k, count(*) AS c FROM Person GROUP BY id % 3 + 1 ORDER BY k",//
      "SELECT id % 4 + 2 AS k, count(*) AS c FROM Person GROUP BY id % 4 + 2 ORDER BY k",//
      "SELECT id % 3 + 1 AS k, count(*) AS c FROM Person GROUP BY k ORDER BY k",//
      "SELECT name FROM Person WHERE id > -5 AND id < 2 ORDER BY name",//
      "SELECT name FROM Person WHERE name LIKE 'p1%' ORDER BY name",//
      "SELECT name FROM Person WHERE name LIKE 'p2%' ORDER BY name",//
      "SELECT name FROM Person WHERE name ILIKE 'P3%' ORDER BY name",//
      "SELECT name FROM Person WHERE id IN [1, 2, 3] ORDER BY name",//
      "SELECT name FROM Person WHERE id IN (4, 5) ORDER BY name",//
      "SELECT name FROM Person WHERE id IN [1, 1, 2] ORDER BY name",//
      "SELECT name FROM Person WHERE id BETWEEN 10 AND 12 ORDER BY name",//
      "SELECT name FROM Person WHERE score = 15.0",//
      "SELECT name FROM Person WHERE score > 1.4e2 ORDER BY name",//
      "SELECT name FROM Person WHERE 1 = 1 AND id = 3",//
      "SELECT name FROM Person WHERE 1 = 0",//
      "SELECT name FROM Person WHERE id = 5 LIMIT 1",//
      "SELECT name FROM Person ORDER BY id SKIP 3 LIMIT 2",//
      "SELECT name FROM Person ORDER BY id SKIP 5 LIMIT 3",//
      "SELECT 5 AS a, 5L AS b, 2.5 AS c, 2.5f AS d, 1.23456789012345678901 AS e, 'x' AS f, -5 AS g, -2147483648 AS h,"
          + " -9223372036854775808 AS i, 0x1F AS j, -2.5 AS k, 3000000000 AS l, -1.5f AS m",//
      "SELECT 'it\\'s' AS s, \"dq\" AS t, 'a\\nb' AS u, '' AS v",//
      "SELECT name.toUpperCase() AS u FROM Person WHERE name = 'p9'",//
      "SELECT 'abc'.toUpperCase() AS u",//
      "SELECT name FROM Person WHERE name = 'p1' OR name = 'p1'",//
      "SELECT count(*) AS c FROM Person WHERE id >= 50",//
      "SELECT max(id) AS m FROM Person WHERE id < 30",//
      "MATCH {type: Person, as: p, where: (id = 4)} RETURN p.name AS n",//
      "MATCH {type: Person, as: p, where: (id = 4)} RETURN p.name",//
      "MATCH {type: Person, as: p, where: (id < 3)} RETURN p.name AS n ORDER BY n LIMIT 2",//
      "SELECT FROM (SELECT FROM Person WHERE id = 8)",//
      "SELECT name, id * 2 AS d FROM Person WHERE id IN (SELECT id FROM Person WHERE id < 3) ORDER BY name",//
      "SELECT ifnull(name, 'none') AS n FROM Person WHERE id = 1",//
      "SELECT name FROM Person WHERE id = 3 AND name = 'p3' AND score = 4.5",//
      "SELECT name FROM Person LET $x = 3 WHERE id = $x",//
      "SELECT name, (id = 3) AS three FROM Person WHERE id < 5 ORDER BY name",//
      "SELECT name FROM Person WHERE id = 3 AND name <> 'p4' AND name = \"p3\"",//
      "SELECT FROM Dbl WHERE d = 9007199254740993",//
      "SELECT FROM Dbl WHERE d = 9007199254740992",//
      "SELECT FROM Dbl WHERE d > 1.00000000000000000001",//
      "SELECT FROM Dbl WHERE d IN [9007199254740993, 1]",//
      "SELECT FROM Flt WHERE f = 16777217",//
      "SELECT FROM Flt WHERE f = 16777216",//
      "SELECT FROM Flt WHERE f = 3",//
      "SELECT name FROM Person WHERE id IN [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 1, 2, 3] ORDER BY name",//
      "SELECT l FROM Lng WHERE l = 5",//
      "SELECT l FROM Lng WHERE l = 5000000000",//
      "SELECT l FROM Lng WHERE l = 5.0",//
      "SELECT l FROM Lng WHERE l > 2.5 ORDER BY l",//
      "SELECT l FROM Lng WHERE l BETWEEN 1 AND 5000000000 ORDER BY l",//
      "SELECT l FROM Lng WHERE l = 1.00000000000000000001",//
      "SELECT l FROM Lng WHERE a = 1 ORDER BY l",//
      "SELECT l FROM Lng WHERE a = 1 AND b = 'x' ORDER BY l",//
      "SELECT l FROM Lng WHERE a = 1.5 AND b = 'x'",//
      "SELECT l FROM Lng WHERE a >= 1 AND a < 3 ORDER BY l",//
      "SELECT 1 + 2",//
      "SELECT 1 + 2 AS s, 3 * 4 AS p",//
      "SELECT id + 1 + 2 AS k FROM Person WHERE id < 3 ORDER BY id + 1 + 2 DESC"//
  );

  /**
   * Texts of one shape, each sequence opened by the edge case: the first text of a shape is the one the policy is decided on,
   * so it must not matter which value it happens to carry.
   */
  private static final List<List<String>> SHAPE_SEQUENCES = List.of(//
      List.of("SELECT name FROM Person WHERE id > -1 AND id < 3 ORDER BY name", "SELECT name FROM Person WHERE id > -50 AND id < 4 ORDER BY name"),//
      List.of("SELECT name FROM Person WHERE id = 0", "SELECT name FROM Person WHERE id = 42"),//
      List.of("SELECT name FROM Person WHERE name = 'it\\'s'", "SELECT name FROM Person WHERE name = 'p5'"),//
      List.of("SELECT name FROM Person WHERE name = ''", "SELECT name FROM Person WHERE name = 'p6'"),//
      List.of("SELECT -2147483648 AS a, 'a\\nb' AS b", "SELECT 7 AS a, 'plain' AS b"),//
      List.of("SELECT name FROM Person WHERE score = 0.0", "SELECT name FROM Person WHERE score = 4.5"),//
      List.of("SELECT name, 1 + 2 AS s FROM Person WHERE id = 1", "SELECT name, 5 + 6 AS s FROM Person WHERE id = 2")//
  );

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Dbl");
    database.command("sql", "CREATE PROPERTY Dbl.d DOUBLE");
    database.command("sql", "CREATE INDEX ON Dbl (d) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Flt");
    database.command("sql", "CREATE PROPERTY Flt.f FLOAT");
    database.command("sql", "CREATE INDEX ON Flt (f) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Lng");
    database.command("sql", "CREATE PROPERTY Lng.l LONG");
    database.command("sql", "CREATE PROPERTY Lng.a INTEGER");
    database.command("sql", "CREATE PROPERTY Lng.b STRING");
    database.command("sql", "CREATE INDEX ON Lng (l) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Lng (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Person").set("id", i).set("name", "p" + i).set("score", i * 1.5D).save();
      database.newDocument("Dbl").set("d", 9007199254740992D).save();
      database.newDocument("Dbl").set("d", 1D).save();
      database.newDocument("Flt").set("f", 16777216F).save();
      database.newDocument("Flt").set("f", 3F).save();
      database.newDocument("Lng").set("l", 1L, "a", 1, "b", "x").save();
      database.newDocument("Lng").set("l", 5L, "a", 1, "b", "y").save();
      database.newDocument("Lng").set("l", 5000000000L, "a", 2, "b", "x").save();
    });
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }

  private List<Map<String, Object>> rows(final String sql, final Object... args) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        // names in projection order, values with their Java type
        final Map<String, Object> map = new LinkedHashMap<>();
        for (final String name : row.getPropertyNames())
          map.put(name, row.getProperty(name));
        rows.add(map);
      }
    }
    return rows;
  }

  /**
   * The lookup of a text whose shape has already been decided. The first text of a shape runs as written (see
   * {@link #aShapeSeenOnceRunsAsWritten}), so a lookup made to inspect the extraction looks the text up twice.
   */
  private Lookup<Statement> parameterized(final String sql) {
    db().getStatementCache().getParameterized(sql);
    return db().getStatementCache().getParameterized(sql);
  }

  private void setParameterization(final boolean enabled) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, enabled);
    db().getStatementCache().clear();
    db().getExecutionPlanCache().invalidate();
  }

  @Test
  void everyStatementAnswersWhatItAnswersAsWritten() {
    final List<List<Map<String, Object>>> asWritten = new ArrayList<>();
    setParameterization(false);
    try {
      for (final String sql : CORPUS)
        asWritten.add(rows(sql));
    } finally {
      setParameterization(true);
    }

    // twice: the first pass decides each shape and builds its statement, the second is served from the cache
    for (int pass = 0; pass < 2; pass++)
      for (int i = 0; i < CORPUS.size(); i++)
        assertThat(rows(CORPUS.get(i))).as("pass %d: %s", pass, CORPUS.get(i)).isEqualTo(asWritten.get(i));
  }

  @Test
  void theFirstTextOfAShapeDecidesForEveryValueWhicheverValueItCarries() {
    for (final List<String> sequence : SHAPE_SEQUENCES) {
      final List<List<Map<String, Object>>> asWritten = new ArrayList<>();
      setParameterization(false);
      try {
        for (final String sql : sequence)
          asWritten.add(rows(sql));
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
            final List<Map<String, Object>> found = rows("SELECT name, id + 1000 AS k FROM Person WHERE id = " + id + " AND name = 'p" + id + "'");
            if (!found.equals(List.of(Map.of("name", "p" + id, "k", id + 1000))))
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
  void statementsDifferingOnlyInLiteralsShareOneStatementAndOnePlan() {
    db().getStatementCache().clear();
    db().getExecutionPlanCache().invalidate();

    for (int id = 0; id < 50; id++)
      assertThat(rows("SELECT name FROM Person WHERE id = " + id)).containsExactly(Map.of("name", "p" + id));

    final Lookup<Statement> first = parameterized("SELECT name FROM Person WHERE id = 3");
    final Lookup<Statement> second = parameterized("SELECT name FROM Person WHERE id = 77");
    assertThat(second.statement()).isSameAs(first.statement());
    assertThat(first.cacheKey()).isEqualTo("SELECT name FROM Person WHERE id = :__lit_i0");
    assertThat(first.parameters()).isEqualTo(Map.of("__lit_i0", 3));
    assertThat(second.parameters()).isEqualTo(Map.of("__lit_i0", 77));
    // the one plan is cached under the parameterized text
    assertThat(db().getExecutionPlanCache().contains(first.cacheKey())).isTrue();
    assertThat(db().getStatementCache().contains("SELECT name FROM Person WHERE id = 3")).isFalse();
  }

  @Test
  void aShapeSeenOnceRunsAsWritten() {
    // a workload that never repeats a shape pays one parse per text, never a second one for a statement nobody reuses
    final Lookup<Statement> first = db().getStatementCache().getParameterized("SELECT name AS once FROM Person WHERE id = 11");
    assertThat(first.parameters()).isNull();
    assertThat(first.cacheKey()).isEqualTo("SELECT name AS once FROM Person WHERE id = 11");
    assertThat(db().getStatementCache().contains(first.cacheKey())).isFalse();

    final Lookup<Statement> second = db().getStatementCache().getParameterized("SELECT name AS once FROM Person WHERE id = 12");
    assertThat(second.parameters()).isEqualTo(Map.of("__lit_i0", 12));
    assertThat(db().getStatementCache().contains(second.cacheKey())).isTrue();
  }

  @Test
  void aShapeWhoseStatementWasEvictedIsBuiltAgainFromItsPolicy() {
    // a two-entry cache: the shape's policy and its statement are evicted independently, and either way the next text of the
    // shape answers what it answers as written
    final StatementCache small = new StatementCache(database, 2);
    final List<String> shapes = List.of("SELECT name FROM Person WHERE id = %d", "SELECT name AS n FROM Person WHERE id = %d",
        "SELECT name AS m FROM Person WHERE id = %d");
    for (int round = 0; round < 3; round++)
      for (final String shape : shapes) {
        final int id = round * 10 + shapes.indexOf(shape);
        final Lookup<Statement> lookup = small.getParameterized(String.format(shape, id));
        final List<Object> values = new ArrayList<>();
        try (final ResultSet rs = lookup.statement().execute(database, lookup.mergeParameters(null))) {
          rs.forEachRemaining(r -> values.addAll(r.toMap().values()));
        }
        assertThat(values).as("%s with id %d", shape, id).containsExactly("p" + id);
      }
  }

  @Test
  void callerPositionalAndNamedParametersKeepTheirNumbers() {
    assertThat(rows("SELECT name FROM Person WHERE name = 'p7' AND id = ?", 7)).containsExactly(Map.of("name", "p7"));
    assertThat(rows("SELECT name FROM Person WHERE id = ? AND name = 'p8' AND score > ?", 8, 1.0D))
        .containsExactly(Map.of("name", "p8"));
    assertThat(rows("SELECT name FROM Person WHERE name = 'p9' AND id = ? LIMIT 5", 9)).containsExactly(Map.of("name", "p9"));

    try (final ResultSet rs = database.query("sql", "SELECT name FROM Person WHERE id = :id AND name = 'p11'", Map.of("id", 11))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p11");
    }
    try (final ResultSet rs = database.command("sql", "SELECT name FROM Person WHERE id = ? AND score = 18.0", 12)) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p12");
    }
  }

  @Test
  void writesStoreTheTypesTheLiteralsWouldHaveStored() {
    database.command("sql", "CREATE DOCUMENT TYPE W");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.command("sql",
            "INSERT INTO W SET seq = " + i + ", a = 5, b = 5L, c = 2.5, d = 'x', e = -3, f = 1.5f, g = 1.23456789012345678901");
      database.command("sql", "UPDATE W SET a = 6, d = 'y' WHERE seq = 1");
      database.command("sql", "DELETE FROM W WHERE seq = 2");
    });

    final List<Map<String, Object>> stored = rows("SELECT seq, a, b, c, d, e, f, g FROM W ORDER BY seq");
    assertThat(stored).hasSize(2);
    final Map<String, Object> first = stored.getFirst();
    assertThat(first.get("a")).isInstanceOf(Integer.class).isEqualTo(5);
    assertThat(first.get("b")).isInstanceOf(Long.class).isEqualTo(5L);
    assertThat(first.get("c")).isInstanceOf(Double.class).isEqualTo(2.5D);
    assertThat(first.get("d")).isEqualTo("x");
    assertThat(first.get("e")).isInstanceOf(Integer.class).isEqualTo(-3);
    assertThat(first.get("f")).isInstanceOf(Float.class).isEqualTo(1.5F);
    assertThat(first.get("g")).isInstanceOf(java.math.BigDecimal.class);
    assertThat(stored.get(1).get("a")).isEqualTo(6);
    assertThat(stored.get(1).get("d")).isEqualTo("y");
  }

  @Test
  void schemaStatementsAreNeverParameterized() {
    final Lookup<Statement> lookup = db().getStatementCache()
        .getParameterized("CREATE PROPERTY Person.extra STRING (default 'abc')");
    assertThat(lookup.parameters()).isNull();
    database.command("sql", "CREATE PROPERTY Person.extra STRING (default 'abc')");
    assertThat(database.getSchema().getType("Person").getProperty("extra").getDefaultValue()).isEqualTo("abc");
  }

  @Test
  void keptPositionsStayInTheKey() {
    assertThat(parameterized("SELECT name FROM Person WHERE id = 3 SKIP 2 LIMIT 7 TIMEOUT 5000")
        .cacheKey()).isEqualTo("SELECT name FROM Person WHERE id = :__lit_i0 SKIP 2 LIMIT 7 TIMEOUT 5000");
    // the same literal in a kept position and in a filter: both kept, so the expressions stay equal
    assertThat(parameterized("SELECT id + 1 AS k FROM Person GROUP BY id + 1").parameters()).isNull();
    // a literal compared to a literal is folded by the planner
    assertThat(parameterized("SELECT FROM Person WHERE 1 = 1").parameters()).isNull();
  }

  @Test
  void aCallerTextIdenticalToAGeneratedKeyBindsItsOwnValue() {
    // the cache holds the statement of the generated key; a caller sending that very text gets it, and binds the name itself
    final Lookup<Statement> generated = parameterized("SELECT name FROM Person WHERE id = 3");
    assertThat(generated.cacheKey()).isEqualTo("SELECT name FROM Person WHERE id = :__lit_i0");
    try (final ResultSet rs = database.query("sql", generated.cacheKey(), Map.of("__lit_i0", 8))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p8");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aTextThatAlreadyUsesTheGeneratedNamespaceIsLeftAlone() {
    try (final ResultSet rs = database.query("sql", "SELECT name FROM Person WHERE id = :__lit_i0", Map.of("__lit_i0", 9))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("p9");
    }
    assertThat(parameterized("SELECT FROM Person WHERE id = 3 AND name <> '__LIT_x'").parameters())
        .isNull();
  }

  @Test
  void theSettingTurnsItOff() {
    setParameterization(false);
    try {
      final Lookup<Statement> lookup = parameterized("SELECT name FROM Person WHERE id = 3");
      assertThat(lookup.parameters()).isNull();
      assertThat(lookup.cacheKey()).isEqualTo("SELECT name FROM Person WHERE id = 3");
    } finally {
      setParameterization(true);
    }
  }

  @Test
  void anInvalidLiteralStillRaisesTheParserError() {
    rows("SELECT 5 AS n");
    assertThatThrownBy(() -> rows("SELECT 99999999999999999999 AS n")).hasMessageContaining("99999999999999999999");
  }
}
