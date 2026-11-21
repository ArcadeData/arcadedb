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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12881: {@code .sort()} on a subquery result, a list of rows, failed with a bare ClassCastException (HTTP 500), while the
 * same rows written as map literals sorted. Rows and records now sort by their properties, as the maps do, and the method still
 * answers the rows themselves.
 */
class SQLMethodSortRowsIssue12881Test extends TestHelper {

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
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM P WHERE (SELECT name FROM P).sort(name) = []")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
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
