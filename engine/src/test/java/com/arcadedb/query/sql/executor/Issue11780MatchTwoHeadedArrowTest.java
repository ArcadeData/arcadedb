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
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.StatementCache;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11780: the grammar accepts the typed arrow {@code <-E->} both at the top level of a MATCH path and inside
 * {@code .( )}, and the two AST visitors decided its direction differently: {@code both('E')} at the top level, {@code in('E')}
 * nested. Both now take one decision, a hop in either direction (as openCypher reads {@code <-[:E]->}). Also covers the EXPLAIN
 * rendering of a nested path step, which printed {@code {x}null{y}}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11780MatchTwoHeadedArrowTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:N {name: 'a'})-[:E]->(b:N {name: 'b'}), (c:N {name: 'c'})-[:E]->(b), (d:N {name: 'd'})-[:E]->(a)"));
  }

  @Test
  void twoHeadedArrowIsAHopInEitherDirectionAtTheTopLevelAndNested() {
    final String from = "MATCH {type: N, as: x, where: (name = 'a')}";
    assertThat(names(from + "<-E->{as: y} RETURN y.name AS n")).containsExactly("b", "d");
    assertThat(names(from + ".(<-E->){as: y} RETURN y.name AS n")).containsExactly("b", "d");

    // Every other typed spelling answers the same rows in both positions
    assertThat(names(from + "-E-{as: y} RETURN y.name AS n")).containsExactly("b", "d");
    assertThat(names(from + ".(-E-){as: y} RETURN y.name AS n")).containsExactly("b", "d");
    assertThat(names(from + "-E->{as: y} RETURN y.name AS n")).containsExactly("b");
    assertThat(names(from + ".(-E->){as: y} RETURN y.name AS n")).containsExactly("b");
    assertThat(names(from + "<-E-{as: y} RETURN y.name AS n")).containsExactly("d");
    assertThat(names(from + ".(<-E-){as: y} RETURN y.name AS n")).containsExactly("d");
  }

  @Test
  void bothPositionsParseToTheSameStep() {
    assertThat(parse("MATCH {type: N, as: x}<-E->{as: y} RETURN y")).isEqualTo("MATCH {type: N, as: x}.both('E'){as: y} RETURN y");
    assertThat(parse("MATCH {type: N, as: x}.(<-E->){as: y} RETURN y")).isEqualTo("MATCH {type: N, as: x}.(both('E')){as: y} RETURN y");
  }

  @Test
  void explainRendersTheNestedPathStep() {
    try (final ResultSet rs = database.query("sql", "EXPLAIN MATCH {type: N, as: x, where: (name = 'a')}.(<-E->){as: y} RETURN y")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).doesNotContain("null{y}");
      assertThat(plan).contains("{x}.(both('E')){y}");
    }
  }

  private String parse(final String sql) {
    final Statement statement = new StatementCache(database, 10).get(sql);
    return statement.toString();
  }

  private List<String> names(final String match) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT n FROM (" + match + ") ORDER BY n")) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("n"));
    }
    return out;
  }
}
