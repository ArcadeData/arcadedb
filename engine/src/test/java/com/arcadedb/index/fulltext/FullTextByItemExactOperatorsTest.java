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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8438: a FULL_TEXT BY ITEM index answers by analyzer token, so it must not answer an exact operator on the list
 * ({@code =}, {@code IN}, {@code CONTAINS}, {@code CONTAINSANY}, {@code CONTAINSALL}): every one of them must return what
 * the same query returns with no index at all. CONTAINSTEXT keeps using the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextByItemExactOperatorsTest extends TestHelper {
  private static final String[] CONDITIONS = { "txt = 'two'", "txt = ['two']", "txt IN ['two', 'x']", "'two' IN txt",
      "txt CONTAINS 'two'", "txt CONTAINSANY ['two']", "txt CONTAINSALL ['two']", "txt CONTAINS '--'", "txt CONTAINS '-two'",
      "txt CONTAINSANY ['a:b']" };

  @Test
  void exactOperatorsAnswerAsWithoutIndex() {
    createType("Plain", null);
    createType("Indexed", "FULL_TEXT");

    for (final String condition : CONDITIONS) {
      final List<Integer> expected = ids("SELECT n FROM Plain WHERE " + condition + " ORDER BY n");
      assertThat(ids("SELECT n FROM Indexed WHERE " + condition + " ORDER BY n")).as(condition).isEqualTo(expected);
      assertThat(explain("SELECT n FROM Indexed WHERE " + condition)).as(condition).doesNotContain("FETCH FROM INDEX");
    }

    // the issue's own expectations
    assertThat(ids("SELECT n FROM Indexed WHERE txt = 'two' ORDER BY n")).containsExactly(2);
    assertThat(ids("SELECT n FROM Indexed WHERE txt CONTAINS 'two' ORDER BY n")).containsExactly(2, 3);
    assertThat(ids("SELECT n FROM Indexed WHERE txt CONTAINS '--' ORDER BY n")).containsExactly(5);
  }

  @Test
  void containsTextStillUsesTheIndex() {
    createType("Indexed", "FULL_TEXT");
    assertThat(explain("SELECT n FROM Indexed WHERE txt CONTAINSTEXT 'two'")).contains("FETCH FROM INDEX");
    assertThat(ids("SELECT n FROM Indexed WHERE txt CONTAINSTEXT 'two' ORDER BY n")).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  void keyByItemIndexIsStillUsed() {
    createType("Keyed", "NOTUNIQUE");
    assertThat(explain("SELECT n FROM Keyed WHERE txt CONTAINS 'two'")).contains("FETCH FROM INDEX");
    assertThat(ids("SELECT n FROM Keyed WHERE txt CONTAINS 'two' ORDER BY n")).containsExactly(2, 3);
  }

  private void createType(final String name, final String indexType) {
    database.command("sql", "CREATE DOCUMENT TYPE " + name);
    database.command("sql", "CREATE PROPERTY " + name + ".txt LIST OF STRING");
    if (indexType != null)
      database.command("sql", "CREATE INDEX ON " + name + " (txt BY ITEM) " + indexType);
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + name + " SET n = 1, txt = ['two words', 'x']");
      database.command("sql", "INSERT INTO " + name + " SET n = 2, txt = ['two']");
      database.command("sql", "INSERT INTO " + name + " SET n = 3, txt = ['one', 'two', 'three']");
      database.command("sql", "INSERT INTO " + name + " SET n = 4, txt = ['Two']");
      database.command("sql", "INSERT INTO " + name + " SET n = 5, txt = ['--', '-two', 'a:b']");
    });
  }

  private List<Integer> ids(final String query) {
    final List<Integer> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> result.add(r.<Number>getProperty("n").intValue()));
    }
    return result;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
