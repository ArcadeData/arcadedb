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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12084: a TRAVERSE target that is a context variable ({@code FROM $x}, {@code FROM $parent.$current}
 * in a LET subquery) was resolved as a type name and failed with "Type with name '$x' was not found", while SELECT FROM the
 * same variable works.
 */
class Issue12084TraverseFromVariableTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE EDGE TYPE L");
      database.command("sql", "CREATE PROPERTY N.children LIST OF N");
      database.command("sql", "CREATE VERTEX N SET name = 'A'");
      database.command("sql", "CREATE VERTEX N SET name = 'B'");
      database.command("sql", "CREATE VERTEX N SET name = 'C'");
      database.command("sql", "CREATE VERTEX N SET name = 'D'");
      database.command("sql", "CREATE EDGE L FROM (SELECT FROM N WHERE name = 'A') TO (SELECT FROM N WHERE name = 'B')");
      database.command("sql", "CREATE EDGE L FROM (SELECT FROM N WHERE name = 'C') TO (SELECT FROM N WHERE name = 'D')");
      database.command("sql", "UPDATE N SET children = (SELECT FROM N WHERE name = 'C') WHERE name = 'A'");
    });
  }

  @Test
  void parentCurrentInLetSubquery() {
    assertThat(letSize("SELECT expand(out()) FROM $parent.$current")).isEqualTo(1);
    assertThat(letSize("TRAVERSE out() FROM $parent.$current")).isEqualTo(2);
  }

  @Test
  void parentCurrentLinkListInLetSubquery() {
    assertThat(letSize("SELECT FROM $parent.$current.children")).isEqualTo(1);
    assertThat(letSize("TRAVERSE out() FROM $parent.$current.children")).isEqualTo(2);
  }

  @Test
  void scriptVariableBoundToResultSet() {
    assertThat(script("LET $x = SELECT FROM N WHERE name = 'A'; TRAVERSE out() FROM $x;")).containsExactly("A", "B");
    assertThat(script("LET $x = SELECT FROM N WHERE name = 'A' OR name = 'C'; TRAVERSE out() FROM $x;"))
        .containsExactlyInAnyOrder("A", "B", "C", "D");
  }

  @Test
  void scriptVariableBoundToSingleRecord() {
    assertThat(script("LET $r = SELECT FROM N WHERE name = 'A'; LET $x = $r[0]; TRAVERSE out() FROM $x;"))
        .containsExactly("A", "B");
  }

  @Test
  void scriptVariableBoundToRidAndRidList() {
    assertThat(script("LET $r = SELECT FROM N WHERE name = 'A'; LET $x = $r[0].@rid; TRAVERSE out() FROM $x;"))
        .containsExactly("A", "B");
    assertThat(script("LET $r = SELECT FROM N WHERE name = 'A' OR name = 'C'; LET $x = $r.@rid; TRAVERSE out() FROM $x;"))
        .containsExactlyInAnyOrder("A", "B", "C", "D");
  }

  @Test
  void scriptVariableWithModifier() {
    assertThat(script("LET $x = SELECT FROM N WHERE name = 'A' OR name = 'C' ORDER BY name; TRAVERSE out() FROM $x[1];"))
        .containsExactly("C", "D");
  }

  @Test
  void nullOrUnknownVariableTraversesNothingAsSelectDoes() {
    assertThat(script("LET $x = null; SELECT FROM $x;")).isEmpty();
    assertThat(script("LET $x = null; TRAVERSE out() FROM $x;")).isEmpty();
    assertThat(script("SELECT FROM $unknown;")).isEmpty();
    assertThat(script("TRAVERSE out() FROM $unknown;")).isEmpty();
    assertThat(names(database.query("sql", "SELECT FROM $unknown"))).isEmpty();
    assertThat(names(database.query("sql", "TRAVERSE out() FROM $unknown"))).isEmpty();
  }

  @Test
  void typeTargetStillWorks() {
    assertThat(names(database.query("sql", "TRAVERSE out() FROM N"))).containsExactlyInAnyOrder("A", "B", "C", "D");
  }

  /**
   * A back-tick quoted {@code `$Q`} is a name, not a context variable (see {@code Identifier.isQuoted()}): TRAVERSE read it as a
   * type name before this fix and still does, while the unquoted {@code $Q} is now the variable.
   */
  @Test
  void quotedDollarIdentifierStaysATypeName() {
    database.getSchema().createVertexType("$Q");
    database.transaction(() -> database.newVertex("$Q").set("name", "Q").save());
    assertThat(script("LET $Q = SELECT FROM N WHERE name = 'A'; TRAVERSE out() FROM `$Q`;")).containsExactly("Q");
    assertThat(script("LET $Q = SELECT FROM N WHERE name = 'A'; TRAVERSE out() FROM $Q;")).containsExactly("A", "B");
    assertThat(names(database.query("sql", "TRAVERSE out() FROM `$Q`"))).containsExactly("Q");
  }

  private int letSize(final String subquery) {
    try (final ResultSet rs = database.query("sql",
        "SELECT name, $c.size() AS n FROM N LET $c = (" + subquery + ") WHERE name = 'A'")) {
      final Result row = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return row.<Number>getProperty("n").intValue();
    }
  }

  private List<String> script(final String script) {
    return names(database.command("sqlscript", script));
  }

  private static List<String> names(final ResultSet rs) {
    final List<String> names = new ArrayList<>();
    try (rs) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
