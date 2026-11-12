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
package com.arcadedb.function;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests for #11589: functions given an argument of the wrong kind, or a null where they need a value, leaked a raw
 * JDK exception ({@code NullPointerException}, {@code ClassCastException}, {@code DateTimeParseException}), which the HTTP
 * API answers with 500. A wrong argument is the caller's mistake: it is now refused with an
 * {@link IllegalArgumentException}, which the API answers with 400, or answered the way the function answers a null.
 */
class FunctionArgumentErrorsIssue11589Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE F");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:V {id: 1})-[:E]->(b:V {id: 2}), (a)-[:F]->(:V {id: 3})"));
  }

  @Test
  void aNullAmongTheEdgeTypesIsIgnored() {
    assertThat(ids("SELECT expand(out('E', null)) FROM V WHERE id = 1")).containsExactly(2);
    assertThat(ids("SELECT expand(in('E', null)) FROM V WHERE id = 2")).containsExactly(1);
    assertThat(ids("SELECT expand(both('E', null)) FROM V WHERE id = 1")).containsExactly(2);
    assertThat(count("SELECT expand(outE('E', null)) FROM V WHERE id = 1")).isEqualTo(1L);
  }

  @Test
  void wrongArgumentsAreRefusedAsIllegalArguments() {
    final String[][] statements = {
        { "sql", "SELECT geo.lineString(1) AS v FROM V WHERE id = 1" },
        { "sql", "SELECT geo.polygon(@rid) AS v FROM V WHERE id = 1" },
        { "sql", "SELECT ts.rate(1, 'abc') AS v FROM V WHERE id = 1" },
        { "sql", "SELECT ts.last(1, 'x') AS v FROM V WHERE id = 1" },
        { "opencypher", "MATCH (n:V {id: 1}) RETURN date.fields('abc') AS v" } };
    for (final String[] statement : statements) {
      final Throwable thrown = catchThrowable(() -> {
        try (final ResultSet rs = database.query(statement[0], statement[1])) {
          while (rs.hasNext())
            rs.next();
        }
      });
      assertThat(thrown).as(statement[1]).isNotNull();
      assertThat(causeChainHas(thrown, IllegalArgumentException.class)).as("%s: %s", statement[1], thrown).isTrue();
    }
  }

  @Test
  void timeTruncateOfANullIsNull() {
    try (final ResultSet rs = database.query("opencypher", "RETURN time.truncate(null, time('12:30')) AS a, "
        + "time.truncate('hour', null) AS b")) {
      final var row = rs.next();
      assertThat(row.<Object>getProperty("a")).isNull();
      assertThat(row.<Object>getProperty("b")).isNull();
    }
  }

  private static boolean causeChainHas(final Throwable thrown, final Class<? extends Throwable> type) {
    for (Throwable t = thrown; t != null; t = t.getCause())
      if (type.isInstance(t))
        return true;
    return false;
  }

  private List<Integer> ids(final String query) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("id").intValue());
    }
    return out;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.stream().count();
    }
  }
}
