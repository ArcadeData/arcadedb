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
package com.arcadedb.query.sql.parser.operators;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.WhereClause;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CONTAINSANY / CONTAINSALL against an operand that is the same on every row hash that operand once instead of walking it for
 * every row (issue #11703). The semantics tests hold before and after the change, at a size that is walked (3) and one that is
 * hashed (100); the cost tests count how often the operand's elements are read.
 */
class ContainsAnyAllLookupTest extends TestHelper {
  private static final long TWO_53 = 1L << 53;

  /** The operand of the given size: {@code items} preceded by fillers of the same kind that match nothing. */
  private static List<Object> padded(final int size, final boolean numbers, final Object... items) {
    final List<Object> list = new ArrayList<>();
    for (int i = 0; list.size() + items.length < size; i++)
      list.add(numbers ? (Object) (100_000L + i) : "filler" + i);
    list.addAll(Arrays.asList(items));
    return list;
  }

  /** Evaluates the condition over three rows holding {@code row}, against the same {@code $p} object, and checks each answer. */
  private void check(final String condition, final Object row, final Object p, final boolean expected) {
    final WhereClause where = new SQLAntlrParser(database).parseCondition(condition);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setVariable("p", p);
    for (int i = 0; i < 3; i++) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("v", row);
      assertThat(where.matchesFilters(result, context)).as("%s, row %s, row %d", condition, row, i).isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void containsAnyAnswersAsTheWalkDoes(final int size) {
    check("v CONTAINSANY $p", List.of("a", "b"), padded(size, false, "b"), true);
    check("v CONTAINSANY $p", List.of("a", "c"), padded(size, false, "b"), false);
    check("$p CONTAINSANY v", List.of("a", "b"), padded(size, false, "b"), true);
    check("$p CONTAINSANY v", List.of("a", "c"), padded(size, false, "b"), false);

    // 1, 1L and 1.0 are one value; 1.5 is none of them
    check("v CONTAINSANY $p", List.of(1, 7), padded(size, true, 7L), true);
    check("v CONTAINSANY $p", List.of(1, 7), padded(size, true, 7.0d), true);
    check("v CONTAINSANY $p", List.of(1.0d, 9), padded(size, true, 1), true);
    check("v CONTAINSANY $p", List.of(1.5d), padded(size, true, 1L, 2L), false);
    check("$p CONTAINSANY v", List.of(7.0f), padded(size, true, 7), true);

    // a text equals the number it converts to, which the hash of numbers cannot see
    check("v CONTAINSANY $p", List.of("7"), padded(size, true, 7), true);
    check("v CONTAINSANY $p", List.of(7), padded(size, false, "7"), true);
    check("v CONTAINSANY $p", List.of(7L), padded(size, false, "8"), false);
    check("v CONTAINSANY $p", List.of(true), padded(size, false, "true"), true);

    // a long beyond 2^53 is not the nearest double; such an operand is walked
    check("v CONTAINSANY $p", List.of((double) TWO_53), padded(size, true, TWO_53 + 1), false);
    check("v CONTAINSANY $p", List.of(TWO_53 + 1), padded(size, true, TWO_53 + 1), true);

    // null matches null only
    final List<Object> rowWithNull = Arrays.asList(null, "x");
    check("v CONTAINSANY $p", rowWithNull, padded(size, false, (Object) null), true);
    check("v CONTAINSANY $p", rowWithNull, padded(size, false, "y"), false);
    check("v CONTAINSANY $p", rowWithNull, padded(size, false, "x"), true);
    check("$p CONTAINSANY v", rowWithNull, padded(size, false, "y"), false);

    // nested lists, a RID against its text, a scalar left operand (issue #8436), an array right operand (issue #7084)
    check("v CONTAINSANY $p", List.of(List.of("a"), "z"), padded(size, false, List.of("a")), true);
    check("v CONTAINSANY $p", List.of(List.of("a")), padded(size, false, "a"), true);
    check("v CONTAINSANY $p", List.of(List.of("a", "b")), padded(size, false, "a"), false);
    check("v CONTAINSANY $p", List.of(new RID(3, 7)), padded(size, false, "#3:7"), true);
    check("v CONTAINSANY $p", List.of(new RID(3, 7)), padded(size, false, "#3:8"), false);
    check("v CONTAINSANY $p", "b", padded(size, false, "b"), true);
    check("v CONTAINSANY $p", "c", padded(size, false, "b"), false);
    check("v CONTAINSANY $p", List.of("a", "b"), padded(size, false, "b").toArray(new Object[0]), true);
    check("v CONTAINSANY $p", List.of("a", "c"), padded(size, false, "b").toArray(new String[0]), false);

    // a byte[] is one BINARY value, not a sequence of bytes
    final byte[] bytes = { 1, 2 };
    check("v CONTAINSANY $p", List.of(bytes, "x"), bytes, true);
    check("v CONTAINSANY $p", List.of((byte) 1, (byte) 2), bytes, false);

    check("NOT (v CONTAINSANY $p)", List.of("a", "b"), padded(size, false, "b"), false);
    check("NOT (v CONTAINSANY $p)", List.of("a", "c"), padded(size, false, "b"), true);
  }

  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void containsAllAnswersAsTheWalkDoes(final int size) {
    final List<Object> p = padded(size, false, "b");
    final List<Object> all = new ArrayList<>(p);
    all.add("extra");
    final List<Object> allButOne = new ArrayList<>(all.subList(1, all.size()));

    check("v CONTAINSALL $p", all, p, true);
    check("v CONTAINSALL $p", allButOne, p, false);
    check("$p CONTAINSALL v", List.of("b", "b", p.get(0)), p, true);
    check("$p CONTAINSALL v", List.of("b", "missing"), p, false);

    // duplicates count once on either side
    final List<Object> doubled = new ArrayList<>(p);
    doubled.addAll(p);
    check("v CONTAINSALL $p", all, doubled, true);
    check("v CONTAINSALL $p", allButOne, doubled, false);

    // empty lists: nothing is contained in an empty row, and an empty operand is contained in anything
    check("v CONTAINSALL $p", List.of(), p, false);
    check("$p CONTAINSALL v", List.of(), p, true);
    check("v CONTAINSALL $p", List.of("a"), List.of(), true);

    // numbers of mixed kinds and a text of a number
    final List<Object> numbers = padded(size, true, 7L);
    final List<Object> numberRow = new ArrayList<>();
    for (final Object n : numbers)
      numberRow.add(((Long) n).doubleValue());
    check("v CONTAINSALL $p", numberRow, numbers, true);
    final List<Object> textRow = new ArrayList<>(numberRow);
    textRow.set(textRow.size() - 1, "7");
    check("v CONTAINSALL $p", textRow, numbers, true);
    textRow.set(textRow.size() - 1, 7.5d);
    check("v CONTAINSALL $p", textRow, numbers, false);

    // a null item is contained only where a null is
    final List<Object> withNull = new ArrayList<>(p);
    withNull.add(null);
    check("v CONTAINSALL $p", all, withNull, false);
    final List<Object> allAndNull = new ArrayList<>(all);
    allAndNull.add(null);
    check("v CONTAINSALL $p", allAndNull, withNull, true);
    check("$p CONTAINSALL v", Arrays.asList("b", null), p, false);

    // a scalar left operand contains the operand only when every item equals it
    check("v CONTAINSALL $p", "b", p, false);
    check("v CONTAINSALL $p", "b", List.of("b", "b"), true);
  }

  /**
   * Pairs of numbers "=" does not compare by value, which no key of a value can answer: the operand that holds one is walked
   * (issue #11703 review). A {@code Short} on the left narrows to a {@code Byte} on the right, so {@code (short) 256} equals
   * {@code (byte) 0}; a {@code Double} narrows to a {@code Float} it meets, so {@code 16777217d} equals {@code 16777216f}; and a
   * {@code Float} at or above 2^24 meets an integer through its shortest decimal, so {@code 33554448f} reads as 33554450.
   */
  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void numberPairsCompareAsTheWalkDoes(final int size) {
    // SHORT ON THE LEFT, BYTE ON THE RIGHT: THE SHORT IS NARROWED
    check("v CONTAINSANY $p", List.of((short) 256), padded(size, true, (byte) 0), true);
    check("$p CONTAINSANY v", List.of((byte) 0), padded(size, true, (short) 256), true);
    final List<Object> bytes = padded(size, true, (byte) 0);
    final List<Object> shortRow = new ArrayList<>(bytes);
    shortRow.set(shortRow.size() - 1, (short) 256);
    check("v CONTAINSALL $p", shortRow, bytes, true);
    check("$p CONTAINSALL v", List.of((byte) 0), padded(size, true, (short) 256), true);
    // a byte on the left is widened, and a short in the range of a byte is that byte
    check("v CONTAINSANY $p", List.of((byte) 0), padded(size, true, (short) 256), false);
    check("v CONTAINSANY $p", List.of((short) 5), padded(size, true, (byte) 5), true);

    // A FLOAT MEETS A DOUBLE IT IS THE NARROWING OF
    check("v CONTAINSANY $p", List.of(16777216f), padded(size, true, 16777217d), true);
    check("v CONTAINSANY $p", List.of(16777217d), padded(size, true, 16777216f), true);
    check("$p CONTAINSANY v", List.of(16777217d), padded(size, true, 16777216f), true);
    final List<Object> doubles = padded(size, true, 16777217d);
    final List<Object> floatRow = new ArrayList<>(doubles);
    floatRow.set(floatRow.size() - 1, 16777216f);
    check("v CONTAINSALL $p", floatRow, doubles, true);

    // A FLOAT ABOVE 2^24 MEETS AN INTEGER THROUGH ITS SHORTEST DECIMAL
    check("v CONTAINSANY $p", List.of(33554448f), padded(size, true, 33554448), false);
    check("$p CONTAINSANY v", List.of(33554448), padded(size, true, 33554448f), false);

    // below 2^24 a float holds every integer exactly and is one value with it
    check("v CONTAINSANY $p", List.of(16777215f), padded(size, true, 16777215d), true);
    check("v CONTAINSANY $p", List.of(16777215f), padded(size, true, 16777214L), false);
    check("$p CONTAINSANY v", List.of(-3), padded(size, true, -3f), true);
  }

  /** A list parameter is hashed once per execution: the answers stay those of the walk. */
  @ParameterizedTest
  @ValueSource(ints = { 3, 100 })
  void parameterOperand(final int size) {
    database.getSchema().createDocumentType("Doc11703_" + size);
    final List<Object> p = padded(size, false, "b");
    database.transaction(() -> {
      database.newDocument("Doc11703_" + size).set("name", "hit").set("v", List.of("a", "b")).save();
      database.newDocument("Doc11703_" + size).set("name", "miss").set("v", List.of("a", "c")).save();
      database.newDocument("Doc11703_" + size).set("name", "all").set("v", p).save();
    });
    final Map<String, Object> params = new HashMap<>();
    params.put("p", p);
    assertThat(names("SELECT name FROM Doc11703_" + size + " WHERE v CONTAINSANY :p ORDER BY name", params)).containsExactly(
        "all", "hit");
    assertThat(names("SELECT name FROM Doc11703_" + size + " WHERE v CONTAINSALL :p", params)).containsExactly("all");
    assertThat(names("SELECT name FROM Doc11703_" + size + " WHERE :p CONTAINSALL v", params)).containsExactly("all");
    assertThat(names("SELECT name FROM Doc11703_" + size + " WHERE NOT (v CONTAINSANY :p)", params)).containsExactly("miss");
  }

  private List<String> names(final String query, final Map<String, Object> params) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      rs.stream().forEach(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  /** A list that counts how often its elements are read. */
  private static final class CountingList extends AbstractList<Object> {
    private final List<Object> items;
    private long reads;

    CountingList(final List<Object> items) {
      this.items = items;
    }

    @Override
    public Object get(final int index) {
      reads++;
      return items.get(index);
    }

    @Override
    public int size() {
      return items.size();
    }
  }

  /**
   * The cost of an operand that is the same object on every row is its size once, plus the rows' own items: it used to be read
   * once per row (CONTAINSANY) or once per row item (CONTAINSALL), 200 rows x 20 items x 1,000 elements.
   */
  @Test
  void sameOperandIsReadOnce() {
    final int rows = 200;
    final int rowSize = 20;
    final int size = 1_000;
    final Random random = new Random(11703);
    final List<Object> principals = new ArrayList<>();
    for (int i = 0; i < size; i++)
      principals.add("t" + random.nextInt(100_000));
    final List<List<Object>> rowValues = new ArrayList<>();
    for (int r = 0; r < rows; r++) {
      final List<Object> row = new ArrayList<>();
      for (int i = 0; i < rowSize; i++)
        row.add("t" + random.nextInt(100_000));
      rowValues.add(row);
    }

    for (final String condition : List.of("v CONTAINSANY $p", "$p CONTAINSANY v", "v CONTAINSALL $p", "$p CONTAINSALL v")) {
      final WhereClause where = new SQLAntlrParser(database).parseCondition(condition);
      final CountingList p = new CountingList(principals);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      context.setVariable("p", p);

      final WhereClause oracle = new SQLAntlrParser(database).parseCondition(condition);
      final BasicCommandContext oracleContext = new BasicCommandContext();
      oracleContext.setDatabase(database);
      for (final List<Object> row : rowValues) {
        final ResultInternal result = new ResultInternal();
        result.setProperty("v", row);
        // a fresh copy per row is never recognized as the same operand: the walk, as the reference answer
        oracleContext.setVariable("p", new ArrayList<>(principals));
        assertThat(where.matchesFilters(result, context)).as(condition).isEqualTo(oracle.matchesFilters(result, oracleContext));
      }
      // the first row walks the operand (once per row item at most), as an operand is hashed the second time it is met; the
      // hash then reads it once, and later rows read only their own items
      assertThat(p.reads).as(condition).isLessThanOrEqualTo((long) rowSize * size + 2L * size + (long) rows * rowSize);
    }
  }
}
