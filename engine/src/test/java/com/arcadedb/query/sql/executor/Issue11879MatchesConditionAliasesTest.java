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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.MatchesCondition;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11879: a MATCH whose {@code where:} has a MATCHES with a side that names no pattern alias threw a NullPointerException
 * while planning. {@code getMatchPatternInvolvedAliases()} answers null for an expression that involves no alias, and
 * {@code MatchesCondition} copied both sides' answers without a null check.
 */
class Issue11879MatchesConditionAliasesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      final Vertex abc = database.newVertex("V").set("name", "abc", "l", List.of(1, 2)).save();
      final Vertex abcd = database.newVertex("V").set("name", "abcd", "l", List.of(3)).save();
      final Vertex xyz = database.newVertex("V").set("name", "xyz", "l", List.of(3)).save();
      abc.newEdge("E", abcd);
      abc.newEdge("E", xyz);
    });
  }

  @Test
  void aMatchesWhoseRightSideNamesNoAliasPlansAndFilters() {
    assertThat(names("MATCH {type: V, as: a, where: (name MATCHES 'a' + '.*')} RETURN a.name AS name"))
        .containsExactlyInAnyOrder("abc", "abcd");
    // the expression of the issue: an operand that is not a string, so no row matches, as in the SELECT
    assertThat(names("MATCH {type: V, as: a, where: (l[-1] MATCHES -1 | 'x')} RETURN a.name AS name")).isEmpty();
    assertThat(names("SELECT name FROM V WHERE l[-1] MATCHES -1 | 'x'")).isEmpty();
  }

  @Test
  void aMatchesWhoseLeftSideNamesNoAliasPlansAndFilters() {
    assertThat(names("MATCH {type: V, as: a, where: ('ab' + 'c' MATCHES 'a.*' AND name = 'abc')} RETURN a.name AS name"))
        .containsExactly("abc");
  }

  @Test
  void aMatchesThatReadsAnAliasThroughMatchedStillOrdersThePlan() {
    // b is filtered by the name of a, so the planner must still see the dependency when only the right side names the alias
    assertThat(names("MATCH {type: V, as: a, where: (name = 'abc')}.out('E'){as: b, where: (name MATCHES $matched.a.name + '.*')} "
        + "RETURN b.name AS name")).containsExactly("abcd");
  }

  /** The contract of {@code Expression.getMatchPatternInvolvedAliases()}: null when nothing is named, as every other condition. */
  @Test
  void aMatchesNamingNoAliasAnswersNull() {
    assertThat(matchesCondition("name MATCHES 'a' + '.*'").getMatchPatternInvolvedAliases()).isNull();
    assertThat(matchesCondition("'ab' + 'c' MATCHES 'a.*'").getMatchPatternInvolvedAliases()).isNull();
    assertThat(matchesCondition("name MATCHES 'a.*'").getMatchPatternInvolvedAliases()).isNull();
    assertThat(matchesCondition("name MATCHES :regex").getMatchPatternInvolvedAliases()).isNull();
  }

  @Test
  void aMatchesNamingAnAliasOnEitherSideAnswersIt() {
    assertThat(matchesCondition("name MATCHES $matched.a.name + '.*'").getMatchPatternInvolvedAliases()).containsExactly("a");
    assertThat(matchesCondition("$matched.a.name + 'c' MATCHES 'a.*'").getMatchPatternInvolvedAliases()).containsExactly("a");
    assertThat(matchesCondition("$matched.a.name + 'c' MATCHES $matched.b.name + '.*'").getMatchPatternInvolvedAliases())
        .containsExactlyInAnyOrder("a", "b");
  }

  private MatchesCondition matchesCondition(final String condition) {
    final SelectStatement statement = (SelectStatement) new SQLAntlrParser(null).parse("SELECT FROM V WHERE " + condition);
    final BooleanExpression term = statement.getWhereClause().flatten().getFirst().getSubBlocks().getFirst();
    assertThat(term).isInstanceOf(MatchesCondition.class);
    return (MatchesCondition) term;
  }

  private Set<String> names(final String query) {
    final Set<String> names = new HashSet<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
