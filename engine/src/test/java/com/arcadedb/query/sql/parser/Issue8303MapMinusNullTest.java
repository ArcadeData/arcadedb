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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8303: {@code MINUS.apply(Object, Object)} dereferenced the right operand in its Map arm, so
 * {@code <map> - <null or missing field>} aborted the query with a raw {@link NullPointerException}, while
 * {@code <map> + <missing>}, {@code <date> - <missing>}, {@code <number> - <missing>} and {@code <list> - <missing>} all
 * answered the left operand. The map-minus-map arm also NPE'd on a null value in the right map, and the collection and
 * array arms on a null key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8303MapMinusNullTest {

  @Test
  void mapMinusNullReturnsTheMapUnchanged() {
    final Map<String, Object> map = new HashMap<>(Map.of("a", 1));
    assertThat(MathExpression.Operator.MINUS.apply(map, null)).isEqualTo(Map.of("a", 1));
    assertThat(MathExpression.Operator.MINUS.apply(null, map)).isNull();
  }

  @Test
  void mapMinusMapWithNullValueDoesNotThrow() {
    final Map<String, Object> left = new HashMap<>();
    left.put("a", 1);
    left.put("b", null);
    left.put("c", 3);

    final Map<String, Object> right = new HashMap<>();
    right.put("a", null); // left has a=1: values differ, kept
    right.put("b", null); // left has b=null: values match, removed
    right.put("z", null); // left has no z: nothing to remove

    final Map<String, Object> result = (Map<String, Object>) MathExpression.Operator.MINUS.apply(left, right);
    assertThat(result).containsOnlyKeys("a", "c");
  }

  @Test
  void mapMinusKeysWithNullKeyDoesNotThrow() {
    final List<Object> keys = new ArrayList<>(Arrays.asList("a", null));
    assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(new HashMap<>(Map.of("a", 1, "b", 2)), keys))//
        .containsOnlyKeys("b");
    assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(new HashMap<>(Map.of("a", 1, "b", 2)),
        new Object[] { null, "b" })).containsOnlyKeys("a");
  }

  @Test
  void queryMapMinusMissingFieldDoesNotThrow() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue8303MapMinusNull", db -> {
      db.getSchema().createDocumentType("T").createProperty("m", Type.MAP);
      db.transaction(() -> db.newDocument("T").set("m", Map.of("a", 1)).set("n", 5).set("l", List.of(1, 2, 3)).save());

      try (final ResultSet rs = db.query("sql", "SELECT m - missing AS r FROM T")) {
        assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
      }
      try (final ResultSet rs = db.query("sql", "SELECT m - null AS r FROM T")) {
        assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
      }
      try (final ResultSet rs = db.query("sql", "SELECT m - {'a': null} AS r FROM T")) {
        assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
      }
      try (final ResultSet rs = db.query("sql", "SELECT missing - m AS r FROM T")) {
        assertThat(rs.next().<Object>getProperty("r")).isNull();
      }

      // control: MINUS and PLUS now agree on the same pair of operands
      try (final ResultSet rs = db.query("sql", "SELECT m + missing AS r FROM T")) {
        assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
      }
      try (final ResultSet rs = db.query("sql", "SELECT n - missing AS r, l - missing AS l2 FROM T")) {
        final var row = rs.next();
        assertThat(row.<Object>getProperty("r")).isEqualTo(5);
        assertThat(row.<List<Object>>getProperty("l2")).containsExactly(1, 2, 3);
      }
    });
  }

  /**
   * Correlated defect found while fixing #8303: the collection and map arms of {@code +} and {@code -} edited their left
   * operand in place. That operand is the record's own property, so a read-only {@code SELECT l - 1} rewrote the
   * record's cached list, and {@code UPDATE ... SET l = l + 9} skipped the write (the "new" value was the very instance
   * the record held), so the change never reached disk.
   */
  @Test
  void operatorsNeverMutateTheRecordTheyRead() {
    final String path = "./target/databases/testIssue8303NoMutation";
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      if (factory.exists())
        factory.open().drop();

      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType("T");
        db.transaction(() -> db.command("sql", "INSERT INTO T SET l = [1, 2, 3], m = {'a': 1, 'b': 2}"));

        for (final String q : new String[] { "SELECT l - 1 AS r FROM T", "SELECT l + 9 AS r FROM T", "SELECT m - ['a'] AS r FROM T",
            "SELECT m + {'z': 1} AS r FROM T", "SELECT m - {'b': 2} AS r FROM T" })
          try (final ResultSet rs = db.query("sql", q)) {
            rs.next();
          }

        try (final ResultSet rs = db.query("sql", "SELECT l, m FROM T")) {
          final var row = rs.next();
          assertThat(row.<List<Object>>getProperty("l")).containsExactly(1, 2, 3);
          assertThat(row.<Map<String, Object>>getProperty("m")).isEqualTo(Map.of("a", 1, "b", 2));
        }

        db.transaction(() -> db.command("sql", "UPDATE T SET l = l + 9, m = m - ['a']"));
      }

      try (final Database db = factory.open()) {
        try (final ResultSet rs = db.query("sql", "SELECT l, m FROM T")) {
          final var row = rs.next();
          assertThat(row.<List<Object>>getProperty("l")).containsExactly(1, 2, 3, 9);
          assertThat(row.<Map<String, Object>>getProperty("m")).isEqualTo(Map.of("b", 2));
        }
        db.drop();
      }
    }
  }

  @Test
  void operatorsAcceptImmutableOperands() {
    assertThat((List<Object>) MathExpression.Operator.MINUS.apply(List.of(1, 2, 3), 2)).containsExactly(1, 3);
    assertThat((List<Object>) MathExpression.Operator.PLUS.apply(List.of(1, 2), 3)).containsExactly(1, 2, 3);
    assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(Map.of("a", 1), null)).isEqualTo(Map.of("a", 1));
    assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(Map.of("a", 1), List.of("a"))).isEmpty();
  }

  @Test
  void sortedOperandsStaySortedByTheirComparator() {
    final TreeSet<Integer> set = new TreeSet<>(Comparator.reverseOrder());
    set.addAll(List.of(1, 2, 3));
    final Object plus = MathExpression.Operator.PLUS.apply(set, 4);
    assertThat(plus).isInstanceOf(TreeSet.class);
    assertThat((TreeSet<Integer>) plus).containsExactly(4, 3, 2, 1);
    assertThat(set).containsExactly(3, 2, 1);

    final TreeMap<String, Object> map = new TreeMap<>(Comparator.reverseOrder());
    map.put("a", 1);
    map.put("b", 2);
    final Object minus = MathExpression.Operator.MINUS.apply(map, List.of("a"));
    assertThat(minus).isInstanceOf(TreeMap.class);
    assertThat(((TreeMap<String, Object>) minus).comparator()).isEqualTo(Comparator.reverseOrder());
    assertThat(map).containsOnlyKeys("a", "b");
  }
}
