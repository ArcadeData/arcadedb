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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11682: {@code x IN $list} walked the whole list on every row, even for a list that is the same
 * on every row, so an index seek on {@code c.key IN $ids}, which re-checks each id it found against the list, was quadratic
 * in the list (16,000 ids took 2.1 s). A list met again on a later row is now answered from a hash built once.
 * <p>
 * The hash has to answer exactly as the walk does, so the semantics below run over three rows: the first is answered by the
 * walk, the later ones by the lookup once the list is long enough to have one. Each case runs on a short list, which is
 * always walked, and on a long one.
 */
class CypherInListLookupIssue11682Test extends TestHelper {
  private static final int ROWS = 3;

  /** A list that counts the elements read from it, by index or through its iterator. */
  private static final class CountingList extends AbstractList<Object> {
    private final List<Object> elements;
    private long               reads;

    private CountingList(final List<Object> elements) {
      this.elements = elements;
    }

    @Override
    public Object get(final int index) {
      reads++;
      return elements.get(index);
    }

    @Override
    public int size() {
      return elements.size();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void numbersAreEqualAcrossTheirTypes(final int size) {
    assertAnswer(1L, padded(size, i -> (double) i, 1.0, 2.0), true);
    assertAnswer(2.0, padded(size, i -> (long) i, 2L, 3L), true);
    assertAnswer(2.5, padded(size, i -> (long) i, 2L, 3L), false);
    assertAnswer(5, padded(size, i -> (long) i, 5L, 6L), true);
    assertAnswer(7L, padded(size, i -> (long) i + 1000, 5, 6), false);
    // Past 2^53 a double no longer tells adjacent longs apart: = compares them exactly
    assertAnswer(9007199254740993L, padded(size, i -> (double) i, 9007199254740992.0), false);
    assertAnswer(9007199254740993L, padded(size, i -> (long) i, 9007199254740993L), true);
    assertAnswer(Double.NaN, padded(size, i -> (double) i, Double.NaN, 1.0), false);
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void textsBooleansAndOtherKinds(final int size) {
    assertAnswer("a", padded(size, i -> "s" + i, "a", "c"), true);
    assertAnswer("b", padded(size, i -> "s" + i, "a", "c"), false);
    // A number is never equal to its text
    assertAnswer(1L, padded(size, i -> "s" + i, "1", "2"), false);
    assertAnswer("1", padded(size, i -> (long) i + 1000, 1L, 2L), false);
    // A boolean is equal to a boolean only
    assertAnswer(true, padded(size, i -> (long) i, 1L, 0L), false);
    assertAnswer(true, padded(size, i -> (long) i, false, true), true);
    assertAnswer(1L, padded(size, i -> (long) i + 1000, false, true), false);
    assertAnswer(List.of(1L), padded(size, i -> List.of((long) i), List.of(1L), List.of(2L)), true);
    assertAnswer(List.of(5L), padded(size, i -> List.of((long) i + 1000), List.of(1L), List.of(2L)), false);
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void nullsKeepThreeValuedLogic(final int size) {
    assertAnswer("a", padded(size, i -> "s" + i, "a", null), true);
    assertAnswer("b", padded(size, i -> "s" + i, "a", null), null);
    assertAnswer(null, padded(size, i -> "s" + i, "a", "b"), null);
    assertAnswer(7L, padded(size, i -> (long) i + 1000, 1L, null), null);
  }

  /** A list literal of literals is one list on every row, and is answered as a parameter is. */
  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void aLiteralListAnswersAsTheParameter(final int size) {
    final StringBuilder doubles = new StringBuilder("1.0, 2.0");
    final StringBuilder texts = new StringBuilder("'a', 'c'");
    for (int i = 2; i < size; i++) {
      doubles.append(", ").append(i + 1000).append(".0");
      texts.append(", 's").append(i).append("'");
    }
    assertRows("UNWIND range(1, " + ROWS + ") AS i RETURN 1 IN [" + doubles + "] AS r", Map.of(), true);
    assertRows("UNWIND range(1, " + ROWS + ") AS i RETURN 2.5 IN [" + doubles + "] AS r", Map.of(), false);
    assertRows("UNWIND range(1, " + ROWS + ") AS i RETURN 'a' IN [" + texts + "] AS r", Map.of(), true);
    assertRows("UNWIND range(1, " + ROWS + ") AS i RETURN 'b' IN [" + texts + ", null] AS r", Map.of(), null);
  }

  /**
   * A declared property compares its stored value with what each element would be stored as (issues #9695, #9794): a text
   * against a number and a number against a text are still answered so on a long list.
   */
  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void aDeclaredPropertyStillConvertsTheElements(final int size) {
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.n INTEGER");
    database.command("sql", "CREATE PROPERTY D.s STRING");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:D {n: 5, s: '1'})").close();
      for (int i = 0; i < 4; i++)
        database.command("opencypher", "CREATE (:D {n: $n, s: $s})", Map.of("n", 100 + i, "s", "t" + i)).close();
    });

    final Map<String, Object> texts = Map.of("list", padded(size, i -> "x" + i, "5", "6"));
    assertThat(count("MATCH (d:D) WHERE d.n IN $list RETURN count(d) AS c", texts)).isEqualTo(1L);
    final Map<String, Object> numbers = Map.of("list", padded(size, i -> (long) i + 1000, 1L, 2L));
    assertThat(count("MATCH (d:D) WHERE d.s IN $list RETURN count(d) AS c", numbers)).isEqualTo(1L);
  }

  /** The same list object, changed between two executions of the cached plan, is answered from what it holds now. */
  @Test
  void aListChangedBetweenExecutionsIsAnsweredFromItsNewContent() {
    final List<Object> list = padded(100, i -> "s" + i, "a", "c");
    assertAnswer("a", list, true);
    list.set(0, "z");
    assertAnswer("a", list, false);
    list.set(0, "a");
    assertAnswer("a", list, true);
  }

  /**
   * The cost: an index seek on {@code key IN $ids} re-checks every key it found against the list, and a scan checks every
   * record. The list counts the elements read from it: walked per row it was read about rows times half its size, answered
   * from a hash it is read a few times over, whatever the number of rows.
   */
  @Test
  void aListMetOnEveryRowIsReadOnce() {
    final int records = 2000;
    final int ids = 1000;
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE PROPERTY Chunk.key STRING");
    database.command("sql", "CREATE INDEX ON Chunk (key) UNIQUE");
    database.transaction(() -> database.command("opencypher",
        "UNWIND range(0, $last) AS i CREATE (:Chunk {key: 'c' + toString(i), grp: i % 2})", Map.of("last", records - 1)).close());

    final List<Object> keys = new ArrayList<>(ids);
    for (int i = 0; i < ids; i++)
      keys.add("c" + i * 2);

    assertReads("MATCH (c:Chunk) WHERE c.key IN $ids RETURN count(c) AS c", keys, ids);
    assertReads("MATCH (c:Chunk) WHERE c.grp = 0 AND NOT c.key IN $ids RETURN count(c) AS c", keys, 0);
    assertReads("MATCH (c:Chunk) RETURN sum(CASE WHEN c.key IN $ids THEN 1 ELSE 0 END) AS c", keys, ids);
  }

  private void assertReads(final String query, final List<Object> keys, final long expected) {
    final CountingList list = new CountingList(keys);
    assertThat(count(query, Map.of("ids", list))).as(query).isEqualTo(expected);
    assertThat(list.reads).as("elements of a %d-element list read by %s", keys.size(), query).isLessThanOrEqualTo(4L * keys.size());
  }

  private static List<Object> padded(final int size, final IntFunction<Object> filler, final Object... head) {
    final List<Object> list = new ArrayList<>(Arrays.asList(head));
    for (int i = list.size(); i < size; i++)
      list.add(filler.apply(i));
    return list;
  }

  private void assertAnswer(final Object value, final List<Object> list, final Boolean expected) {
    final Map<String, Object> params = new HashMap<>();
    params.put("v", value);
    params.put("list", list);
    assertRows("UNWIND range(1, " + ROWS + ") AS i RETURN $v IN $list AS r", params, expected);
  }

  private void assertRows(final String query, final Map<String, Object> params, final Boolean expected) {
    final List<Object> answers = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        answers.add(rs.next().getProperty("r"));
    }
    final List<Object> expectedRows = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      expectedRows.add(expected);
    assertThat(answers).as("%s with %s", query, params).isEqualTo(expectedRows);
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      final Result row = rs.next();
      return ((Number) row.getProperty("c")).longValue();
    }
  }
}
