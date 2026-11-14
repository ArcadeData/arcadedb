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
package com.arcadedb.function.sql.coll;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11581: {@code set()}, {@code intersect()}, {@code difference()} and {@code symmetricDifference()}
 * kept their items in a {@code HashSet}, so {@code 1} as a {@code Long} (a list written by openCypher) and {@code 1} as an
 * {@code Integer} (a list written by SQL or a literal) were different items, while every SQL comparison calls them equal.
 */
class SQLSetFunctionsNumericEqualityIssue11581Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V SET src = 'sql', l = [1, 2]");
      database.command("opencypher", "CREATE (:V {src: 'cypher', l: [1, 2]})");
    });
  }

  @Test
  void inlineFunctionsOnAListWrittenByEitherLanguage() {
    for (final String src : new String[] { "sql", "cypher" }) {
      assertThat(longs("SELECT intersect(l, [1, 9]) AS v FROM V WHERE src = '" + src + "'")).as(src).containsExactly(1L);
      assertThat(longs("SELECT difference(l, [1]) AS v FROM V WHERE src = '" + src + "'")).as(src).containsExactly(2L);
      assertThat(longs("SELECT difference([1, 9], l) AS v FROM V WHERE src = '" + src + "'")).as(src).containsExactly(9L);
      assertThat(longs("SELECT symmetricDifference(l, [1, 9]) AS v FROM V WHERE src = '" + src + "'")).as(src)
          .containsExactlyInAnyOrder(2L, 9L);
    }
  }

  @Test
  void inlineSetOnAListWrittenByEitherLanguage() {
    for (final String src : new String[] { "sql", "cypher" }) {
      assertThat(longs("SELECT set(l, [1, 3]) AS v FROM V WHERE src = '" + src + "'")).as(src)
          .containsExactlyInAnyOrder(1L, 2L, 3L);
    }
  }

  @Test
  void aggregatesOverListsWrittenByBothLanguages() {
    assertThat(longs("SELECT set(l) AS v FROM V")).containsExactlyInAnyOrder(1L, 2L);
    assertThat(longs("SELECT intersect(l) AS v FROM V")).containsExactlyInAnyOrder(1L, 2L);
    assertThat(longs("SELECT symmetricDifference(l) AS v FROM V")).isEmpty();
  }

  @Test
  void theFirstValueOfEachKeyIsKept() {
    final Object intersection = new SQLFunctionIntersect().execute(null, null, null,
        new Object[] { List.of(1L, 2L, 1), List.of(1, 2.0) }, new BasicCommandContext());
    assertThat(items(intersection)).containsExactlyInAnyOrder(1L, 2L);

    final Object difference = new SQLFunctionDifference().execute(null, null, null,
        new Object[] { List.of(1, 2, 3), List.of(1L), 3.0 }, new BasicCommandContext());
    assertThat(items(difference)).containsExactly(2);

    final SQLFunctionSet set = new SQLFunctionSet();
    set.execute(null, null, null, new Object[] { List.of(1, 2) }, new BasicCommandContext());
    set.execute(null, null, null, new Object[] { List.of(2L, 3L) }, new BasicCommandContext());
    assertThat(items(set.getResult())).containsExactlyInAnyOrder(1, 2, 3L);

    // A single row aggregates to its own items, read in their order: the Integer 1 comes first and is the one kept
    final SQLFunctionIntersect single = new SQLFunctionIntersect();
    single.execute(null, null, null, new Object[] { List.of(1, 1.0) }, new BasicCommandContext());
    assertThat(items(single.getResult())).containsExactly(1);
  }

  @Test
  void nullsAndTextsStayDistinctItems() {
    final Object difference = new SQLFunctionDifference().execute(null, null, null,
        new Object[] { new ArrayList<>(Arrays.asList(null, "1", 1)), List.of(1L) }, new BasicCommandContext());
    assertThat(items(difference)).containsExactlyInAnyOrder(null, "1");
  }

  private static List<Object> items(final Object value) {
    return new ArrayList<>((Collection<?>) value);
  }

  private List<Long> longs(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      final List<Long> out = new ArrayList<>();
      for (final Object item : rs.next().<Collection<?>>getProperty("v"))
        out.add(((Number) item).longValue());
      return out;
    }
  }
}
