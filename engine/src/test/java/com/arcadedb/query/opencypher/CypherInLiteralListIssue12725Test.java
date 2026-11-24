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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12725: {@code p.name IN ['name1', ..., 'name100']} was ten times slower than {@code p.name IN $names}.
 * The literal extraction of the plan cache turns every element of the list into a generated parameter, so the right side was no
 * longer a list of literals: every row built the list again, one parameter lookup per element, and walked it, because a list
 * built per row is never met twice and never gets the hash an {@code IN $names} gets (issue #11682). A right side that holds only
 * literals and parameters is the same on every row of an execution: it is now evaluated once per execution and answered from that
 * hash, so the semantics below are pinned on a short list (always walked) and a long one (answered from the hash), and the
 * parameters of one execution must never answer another.
 */
class CypherInLiteralListIssue12725Test extends TestHelper {
  private static final int ROWS = 3;

  /** A parameter map that counts the reads of each key, to see how often the right side is evaluated. */
  private static final class CountingMap extends HashMap<String, Object> {
    private final Map<Object, Integer> reads = new HashMap<>();

    @Override
    public Object get(final Object key) {
      reads.merge(key, 1, Integer::sum);
      return super.get(key);
    }

    int reads(final String key) {
      return reads.getOrDefault(key, 0);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void nullsKeepThreeValuedLogic(final int size) {
    assertRows("'a' IN " + literal(size, i -> "'s" + i + "'", "'a'", "null"), true);
    assertRows("'b' IN " + literal(size, i -> "'s" + i + "'", "'a'", "null"), null);
    assertRows("null IN " + literal(size, i -> "'s" + i + "'", "'a'", "'b'"), null);
    assertRows("7 IN " + literal(size, i -> String.valueOf(i + 1000), "1", "null"), null);
    assertRows("1 IN " + literal(size, i -> String.valueOf(i + 1000), "1", "null"), true);
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void numbersAreEqualAcrossTheirTypes(final int size) {
    assertRows("1 IN " + literal(size, i -> (i + 1000) + ".0", "1.0", "2.0"), true);
    assertRows("2.0 IN " + literal(size, i -> String.valueOf(i + 1000), "2", "3"), true);
    assertRows("2.5 IN " + literal(size, i -> String.valueOf(i + 1000), "2", "3"), false);
    // A number is never equal to its text, a boolean to a number
    assertRows("1 IN " + literal(size, i -> "'s" + i + "'", "'1'", "'2'"), false);
    assertRows("'1' IN " + literal(size, i -> String.valueOf(i + 1000), "1", "2"), false);
    assertRows("true IN " + literal(size, i -> String.valueOf(i + 1000), "1", "0"), false);
    assertRows("true IN " + literal(size, i -> String.valueOf(i + 1000), "false", "true"), true);
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void duplicatesAreMembersOnce(final int size) {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.transaction(() -> database.command("opencypher", "UNWIND range(0, 99) AS i CREATE (:P {name: 'n' + toString(i % 10)})").close());

    final String names = literal(size, i -> "'x" + i + "'", "'n1'", "'n1'", "'n2'", "'n2'", "'n1'");
    assertThat(count("MATCH (p:P) WHERE p.name IN " + names + " RETURN count(p) AS c", Map.of())).isEqualTo(20L);
    assertThat(count("MATCH (p:P) WHERE NOT p.name IN " + names + " RETURN count(p) AS c", Map.of())).isEqualTo(80L);
  }

  /**
   * A declared property compares its stored value with what each element would be stored as (issues #9695, #9794): a text
   * against an INTEGER and a number against a STRING are still answered so when the list comes from the literals of the text.
   */
  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void aDeclaredPropertyStillConvertsTheElements(final int size) {
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.n INTEGER");
    database.command("sql", "CREATE PROPERTY D.s STRING");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:D {n: 5, s: '1'})").close();
      for (int i = 0; i < 4; i++)
        database.command("opencypher", "CREATE (:D {n: $n, s: $s})", Map.of("n", 100 + i, "s", "t" + i)).close();
    });

    assertThat(count("MATCH (d:D) WHERE d.n IN " + literal(size, i -> "'x" + i + "'", "'5'", "'6'") + " RETURN count(d) AS c",
        Map.of())).isEqualTo(1L);
    assertThat(count("MATCH (d:D) WHERE d.s IN " + literal(size, i -> String.valueOf(i + 1000), "1", "2") + " RETURN count(d) AS c",
        Map.of())).isEqualTo(1L);
    assertThat(count("MATCH (d:D) WHERE d.n IN " + literal(size, i -> String.valueOf(i + 1000), "5", "6") + " RETURN count(d) AS c",
        Map.of())).isEqualTo(1L);
    assertThat(count("MATCH (d:D) WHERE d.s IN " + literal(size, i -> "'x" + i + "'", "'t1'", "'t2'") + " RETURN count(d) AS c",
        Map.of())).isEqualTo(2L);
  }

  /**
   * Texts differing only in their literal values share one cached plan, whose list is built from the parameters the literals
   * were extracted into: each execution answers from its own values, never from those of an execution before it.
   */
  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void theCachedPlanAnswersEachExecutionFromItsOwnLiterals(final int size) {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.transaction(() -> database.command("opencypher", "UNWIND range(0, 99) AS i CREATE (:P {name: 'n' + toString(i % 10)})").close());

    final String first = "MATCH (p:P) WHERE p.name IN " + literal(size, i -> "'x" + i + "'", "'n1'", "'n2'") + " RETURN count(p) AS c";
    final String second = "MATCH (p:P) WHERE p.name IN " + literal(size, i -> "'y" + i + "'", "'n3'", "'z'") + " RETURN count(p) AS c";
    final String none = "MATCH (p:P) WHERE p.name IN " + literal(size, i -> "'w" + i + "'", "'q1'", "'q2'") + " RETURN count(p) AS c";
    assertThat(count(first, Map.of())).isEqualTo(20L);
    assertThat(count(second, Map.of())).isEqualTo(10L);
    assertThat(count(none, Map.of())).isEqualTo(0L);
    assertThat(count(first, Map.of())).isEqualTo(20L);
    assertThat(count(second, Map.of())).isEqualTo(10L);
  }

  /** The same parameter map, changed between two executions, is answered from what it holds now. */
  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void aParameterMapChangedBetweenExecutionsIsAnsweredFromItsNewContent(final int size) {
    final Map<String, Object> params = new HashMap<>();
    params.put("rows", List.of(1, 2, 3));
    params.put("v", "a");
    final String query = "UNWIND $rows AS i RETURN $v IN " + parameters(size, params, i -> "s" + i) + " AS r";

    params.put("a0", "a");
    assertThat(answers(query, params)).containsExactly(true, true, true);
    params.put("a0", "z");
    assertThat(answers(query, params)).containsExactly(false, false, false);
    params.put("a0", null);
    assertThat(answers(query, params)).containsExactly(null, null, null);
    params.put("a0", "a");
    assertThat(answers(query, params)).containsExactly(true, true, true);
  }

  /**
   * The cost: the right side reads the same parameters on every row, so it is evaluated once per execution, not once per row.
   * Counted on the reads of one of its parameters, whatever the number of rows.
   */
  @ParameterizedTest
  @ValueSource(ints = { 3, 40 })
  void aListOfParametersIsEvaluatedOncePerExecution(final int size) {
    final int rows = 1000;
    final CountingMap params = new CountingMap();
    final List<Object> rowValues = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++)
      rowValues.add("s" + i);
    params.put("rows", rowValues);
    final String query = "UNWIND $rows AS r RETURN count(CASE WHEN r IN " + parameters(size, params, i -> "s" + i * 2) + " THEN 1 END) AS c";

    assertThat(count(query, params)).isEqualTo(size);
    // At least one read: a copy of the map would make the count vacuous
    assertThat(params.reads("a0")).as("reads of an element of the right side over %d rows", rows).isBetween(1, 4);
  }

  /**
   * A right side that reads no variable of the row, only literals, parameters and functions of them, is the same on every row too:
   * a comprehension over {@code range()} is evaluated once per execution. One that reads a variable of the row is evaluated on
   * every row, and so is one whose own iteration variable shadows nothing.
   */
  @Test
  void aComprehensionReadingNoRowVariableIsEvaluatedOncePerExecution() {
    final int rows = 1000;
    final CountingMap params = new CountingMap();
    final List<Object> rowValues = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++)
      rowValues.add("name" + i);
    params.put("rows", rowValues);
    params.put("prefix", "name");
    params.put("last", 100);
    final String query = "UNWIND $rows AS r RETURN count(CASE WHEN r IN [x IN range(1, $last) | $prefix + toString(x)] THEN 1 END) AS c";

    assertThat(count(query, params)).isEqualTo(100L);
    assertThat(params.reads("prefix")).as("reads of a parameter of the comprehension over %d rows", rows).isBetween(100, 4 * 100);
    params.put("prefix", "nam");
    assertThat(count(query, params)).isEqualTo(0L);
  }

  /**
   * Two executions of one cached plan with different lists, read in turns: each keeps the value it evaluated, and neither makes
   * the other evaluate, and hash, its right side again. A single memo on the shared plan node took the slot from the other
   * execution at every batch, which then paid the evaluation and the hash again (more than the walk it replaced).
   */
  @Test
  void interleavedExecutionsOfOneCachedPlanEachEvaluateTheirRightSideOnce() {
    final int rows = 1000;
    final int size = 40;
    final List<Object> rowValues = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++)
      rowValues.add("s" + i);

    final CountingMap evens = new CountingMap();
    evens.put("rows", rowValues);
    final CountingMap odds = new CountingMap();
    odds.put("rows", rowValues);
    final String query = "UNWIND $rows AS r RETURN r IN " + parameters(size, evens, i -> "s" + i * 2) + " AS r";
    parameters(size, odds, i -> "s" + (i * 2 + 1));

    int evenHits = 0;
    int oddHits = 0;
    int read = 0;
    try (final ResultSet a = database.query("opencypher", query, evens);
        final ResultSet b = database.query("opencypher", query, odds)) {
      while (a.hasNext() && b.hasNext()) {
        if (Boolean.TRUE.equals(a.next().getProperty("r")))
          ++evenHits;
        if (Boolean.TRUE.equals(b.next().getProperty("r")))
          ++oddHits;
        ++read;
      }
      assertThat(a.hasNext()).isFalse();
      assertThat(b.hasNext()).isFalse();
    }

    assertThat(read).isEqualTo(rows);
    assertThat(evenHits).isEqualTo(size);
    assertThat(oddHits).isEqualTo(size);
    assertThat(evens.reads("a0")).as("reads of the first execution's right side over %d interleaved rows", rows).isEqualTo(1);
    assertThat(odds.reads("a0")).as("reads of the second execution's right side over %d interleaved rows", rows).isEqualTo(1);
  }

  /**
   * The allow-list of what is evaluated once per execution holds only what answers from its operands alone. A function that does
   * not ({@code rand()}) is evaluated on every row, and so is one reading the content of a parameter ({@code keys()}), and an
   * index into a parameter, which can read a property of a record.
   */
  @Test
  void aRightSideOutsideTheAllowListIsEvaluatedOnEveryRow() {
    final List<Object> answers = answers("UNWIND range(1, 200) AS i RETURN 1 IN [toInteger(rand() * 2)] AS r", Map.of());
    assertThat(answers).hasSize(200).contains(true, false);

    final int rows = 100;
    final List<Object> rowValues = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++)
      rowValues.add("k" + i);

    final CountingMap keys = new CountingMap();
    keys.put("rows", rowValues);
    keys.put("m", Map.of("k1", 1, "k2", 2));
    assertThat(count("UNWIND $rows AS r RETURN count(CASE WHEN r IN [k IN keys($m) | k] THEN 1 END) AS c", keys)).isEqualTo(2L);
    assertThat(keys.reads("m")).as("reads of the map whose keys are the right side").isGreaterThanOrEqualTo(rows);

    final CountingMap indexed = new CountingMap();
    indexed.put("rows", rowValues);
    indexed.put("m", Map.of("names", List.of("k1", "k2")));
    assertThat(count("UNWIND $rows AS r RETURN count(CASE WHEN r IN [$m['names'][0], 'k3'] THEN 1 END) AS c", indexed)).isEqualTo(2L);
    assertThat(indexed.reads("m")).as("reads of the parameter indexed by the right side").isGreaterThanOrEqualTo(rows);
  }

  @Test
  void aRightSideReadingTheRowIsEvaluatedOnEveryRow() {
    assertThat(answers("UNWIND [1, 2, 3] AS i RETURN 2 IN [x IN range(1, i) | x] AS r", Map.of())).containsExactly(false, true, true);
    assertThat(answers("UNWIND [1, 2, 3] AS x RETURN 4 IN [y IN range(1, 3) | y * x] AS r", Map.of())).containsExactly(false, true, false);
    // The comprehension binds the name of the row's variable for itself: it reads no variable of the row
    assertThat(answers("UNWIND [1, 2] AS x RETURN 4 IN [x IN range(1, 3) | x * x] AS r", Map.of())).containsExactly(true, true);
    assertThat(answers("UNWIND [1, 2, 3] AS i RETURN i IN [1, i + 1, 3] AS r", Map.of())).containsExactly(true, false, true);
    assertThat(answers("UNWIND [1, 2, 3] AS i RETURN 3 IN range(i, 3) AS r", Map.of())).containsExactly(true, true, true);
    assertThat(answers("UNWIND [1, 2, 3] AS i RETURN 1 IN range(i, 3) AS r", Map.of())).containsExactly(true, false, false);
  }

  /** A list literal written with the parameters and literals the caller chose, the first {@code head} then filled up to size. */
  private static String literal(final int size, final IntFunction<String> filler, final String... head) {
    final StringBuilder text = new StringBuilder("[");
    for (int i = 0; i < Math.max(size, head.length); i++) {
      if (i > 0)
        text.append(", ");
      text.append(i < head.length ? head[i] : filler.apply(i));
    }
    return text.append(']').toString();
  }

  /** A list literal of {@code size} parameters {@code $a0..}, bound in {@code params} to the filler's values. */
  private static String parameters(final int size, final Map<String, Object> params, final IntFunction<Object> filler) {
    final StringBuilder text = new StringBuilder("[");
    for (int i = 0; i < size; i++) {
      if (i > 0)
        text.append(", ");
      text.append("$a").append(i);
      params.put("a" + i, filler.apply(i));
    }
    return text.append(']').toString();
  }

  private void assertRows(final String predicate, final Boolean expected) {
    final String query = "UNWIND range(1, " + ROWS + ") AS i RETURN " + predicate + " AS r";
    final List<Object> expectedRows = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      expectedRows.add(expected);
    assertThat(answers(query, Map.of())).as(query).isEqualTo(expectedRows);
  }

  private List<Object> answers(final String query, final Map<String, Object> params) {
    final List<Object> answers = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        answers.add(rs.next().getProperty("r"));
    }
    return answers;
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      final Result row = rs.next();
      return ((Number) row.getProperty("c")).longValue();
    }
  }
}
