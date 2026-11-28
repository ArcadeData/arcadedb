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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12881: {@code .sort()} on a subquery result, a list of rows, failed with a bare ClassCastException (HTTP 500), while the
 * same rows written as map literals sorted. Rows and records now sort by their properties, as the maps do, and the method still
 * answers the rows themselves. Values that still have no order (an embedded document, a nested row, a row beside a date) are
 * refused with an IllegalArgumentException, a client error, rather than a ClassCastException.
 */
class SQLMethodSortRowsIssue12881Test extends TestHelper {
  private static final String ROWS_REFUSED =
      "sort() cannot order these rows by their properties: some of the values they hold, or stand beside in the list, have no order between them";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO P SET name = 'b', n = 2");
      database.command("sql", "INSERT INTO P SET name = 'c', n = 3");
      database.command("sql", "INSERT INTO P SET name = 'a', n = 1");
    });
  }

  @Test
  void projectedRowsSortByTheirValuesLikeTheSameMaps() {
    assertThat(names("SELECT (SELECT name FROM P).sort() AS v")).containsExactly("a", "b", "c");
    assertThat(names("SELECT [{'name': 'b'}, {'name': 'c'}, {'name': 'a'}].sort() AS v")).containsExactly("a", "b", "c");
  }

  @Test
  void descendingSortOfRows() {
    assertThat(names("SELECT (SELECT name FROM P).sort(false) AS v")).containsExactly("c", "b", "a");
  }

  @Test
  void wholeRecordRowsSortByTheirProperties() {
    assertThat(names("SELECT (SELECT name, n FROM P).sort() AS v")).containsExactly("a", "b", "c");
    assertThat(names("SELECT (SELECT FROM P).sort() AS v")).containsExactly("a", "b", "c");
  }

  @Test
  void aRowWithANullValueSortsFirstAsANullMapValueDoes() {
    database.transaction(() -> database.command("sql", "INSERT INTO P SET n = 0"));
    final List<Object> names = names("SELECT (SELECT name FROM P).sort() AS v");
    assertThat(names).hasSize(4);
    assertThat(names.get(0)).isNull();
    assertThat(names.subList(1, 4)).containsExactly("a", "b", "c");
  }

  @Test
  void aSortedSubqueryCanBeComparedInWhere() {
    // the statement of the issue: it answered HTTP 500
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM P WHERE (SELECT name FROM P).sort(name) = []")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    // the order the sort gives is the one the WHERE sees
    try (final ResultSet rs = database.query("sql", "SELECT name FROM P WHERE name = (SELECT name FROM P).sort(false)[0].name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("c");
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT name FROM P WHERE name = (SELECT name FROM P).sort()[0].name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("a");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void rowsHoldingAValueWithNoOrderAreRefusedNotAnInternalError() {
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Q SET name = 'b', e = {'@type': 'E', 'x': 2}");
      database.command("sql", "INSERT INTO Q SET name = 'a', e = {'@type': 'E', 'x': 1}");
    });

    // an embedded document has no order, so neither has a row ordered by it
    assertThatThrownBy(() -> names("SELECT (SELECT FROM Q).sort() AS v")).hasStackTraceContaining(
        ROWS_REFUSED);
    // nor a row holding rows
    assertThatThrownBy(() -> names("SELECT (SELECT (SELECT name FROM P) AS inner FROM P).sort() AS v")).hasStackTraceContaining(
        ROWS_REFUSED);
  }

  @Test
  void aListMixingARowWithAValueItHasNoOrderWithIsRefused() {
    final SQLMethodSort method = new SQLMethodSort();
    final ResultInternal row = new ResultInternal(Map.<String, Object>of("name", "a"));

    // a row against a date: the comparator refuses the pair
    assertThatThrownBy(() -> method.execute(List.of(row, LocalDate.of(2026, 1, 1)), null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(ROWS_REFUSED);

    // a row against a string orders as the same map does against it, by kind
    final List<Object> sorted = (List<Object>) method.execute(List.of("z", row), null, null, null);
    assertThat(sorted).containsExactlyInAnyOrder(row, "z");
  }

  @Test
  void plainValuesWithNoOrderAreRefusedNotAnInternalError() {
    final SQLMethodSort method = new SQLMethodSort();
    assertThatThrownBy(() -> method.execute(List.of(new Object(), new Object()), null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("sort() cannot order values of type Object: the values have no order");
    assertThatThrownBy(() -> method.execute(List.of(LocalDate.of(2026, 1, 1), new HashMap<>(Map.of("a", 1))), null, null,
        new Object[] { false }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("sort() cannot order values of types LocalDate, HashMap: the values have no order between them");
  }

  private List<Object> names(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<?> sorted = rs.next().getProperty("v");
      final List<Object> names = new ArrayList<>(sorted.size());
      for (final Object row : sorted)
        names.add(row instanceof Result result ? result.getProperty("name") : ((Map<?, ?>) row).get("name"));
      return names;
    }
  }
}
